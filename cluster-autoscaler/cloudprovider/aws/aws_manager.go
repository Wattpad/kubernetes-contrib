/*
Copyright 2016 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package aws

import (
	"fmt"
	"io"
	"sync"
	"time"

	"gopkg.in/gcfg.v1"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/golang/glog"
	provider_aws "k8s.io/kubernetes/pkg/cloudprovider/providers/aws"
	"k8s.io/kubernetes/pkg/util/wait"
)

const (
	operationWaitTimeout  = 5 * time.Second
	operationPollInterval = 100 * time.Millisecond
)

type asgInformation struct {
	config   *Asg
	basename string
}

// AwsManager is handles aws communication and data caching.
type AwsManager struct {
	asgs     []*asgInformation
	asgCache map[AwsRef]*Asg

	service    *autoscaling.AutoScaling
	cacheMutex sync.Mutex
}

// CreateAwsManager constructs awsManager object.
func CreateAwsManager(configReader io.Reader) (*AwsManager, error) {
	if configReader != nil {
		var cfg provider_aws.AWSCloudConfig
		if err := gcfg.ReadInto(&cfg, configReader); err != nil {
			glog.Errorf("Couldn't read config: %v", err)
			return nil, err
		}
	}

	service := autoscaling.New(session.New())
	manager := &AwsManager{
		asgs:     make([]*asgInformation, 0),
		service:  service,
		asgCache: make(map[AwsRef]*Asg),
	}

	go wait.Forever(func() { manager.regenerateCacheIgnoreError() }, time.Hour)

	return manager, nil
}

// RegisterAsg registers asg in Aws Manager.
func (m *AwsManager) RegisterAsg(asg *Asg) {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()

	m.asgs = append(m.asgs, &asgInformation{
		config: asg,
	})
}

// GetAsgSize gets ASG size.
func (m *AwsManager) GetAsgSize(asgConfig *Asg) (int64, error) {
	params := &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []*string{aws.String(asgConfig.Name)},
		MaxRecords:            aws.Int64(1),
	}
	resp, err := m.service.DescribeAutoScalingGroups(params)

	if err != nil {
		return -1, err
	}

	// TODO: check for nil pointers
	asg := *resp.AutoScalingGroups[0]
	return *asg.DesiredCapacity, nil
}

// SetAsgSize sets ASG size.
func (m *AwsManager) SetAsgSize(asg *Asg, size int64) error {
	params := &autoscaling.SetDesiredCapacityInput{
		AutoScalingGroupName: aws.String(asg.Name),
		DesiredCapacity:      aws.Int64(size),
		HonorCooldown:        aws.Bool(false),
	}
	// TODO implement waitForOp as it is on GCE
	// TODO do something with the response
	_, err := m.service.SetDesiredCapacity(params)

	if err != nil {
		return err
	}
	return nil
}

// DeleteInstances deletes the given instances. All instances must be controlled by the same ASG.
func (m *AwsManager) DeleteInstances(instances []*AwsRef) error {
	if len(instances) == 0 {
		return nil
	}
	commonAsg, err := m.GetAsgForInstance(instances[0])
	if err != nil {
		return err
	}
	for _, instance := range instances {
		asg, err := m.GetAsgForInstance(instance)
		if err != nil {
			return err
		}
		if asg != commonAsg {
			return fmt.Errorf("Cannot delete instances which don't belong to the same ASG.")
		}
	}

	for _, instance := range instances {
		params := &autoscaling.TerminateInstanceInAutoScalingGroupInput{
			InstanceId:                     aws.String(instance.Name),
			ShouldDecrementDesiredCapacity: aws.Bool(true),
		}
		// TODO: do something with this response
		_, err := m.service.TerminateInstanceInAutoScalingGroup(params)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetAsgForInstance returns AsgConfig of the given Instance
func (m *AwsManager) GetAsgForInstance(instance *AwsRef) (*Asg, error) {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()
	if config, found := m.asgCache[*instance]; found {
		return config, nil
	}

	for _, asg := range m.asgs {
		// Instances in an ASG don't actually need to be in the same AZ, but
		// for early development of the cluster autoscaler, instances attached to an ASG but have the
		// the same AZ as the ASG.
		if asg.config.Zone == instance.Zone {
			if err := m.regenerateCache(); err != nil {
				return nil, fmt.Errorf("Error while looking for ASG for instance %+v, error: %v", *instance, err)
			}

			if config, found := m.asgCache[*instance]; found {
				return config, nil
			}

			return nil, fmt.Errorf("Instance %+v does not belong to any configured ASG", *instance)
		}
	}

	// Instance doesn't belong to any configured asg.
	return nil, nil
}

func (m *AwsManager) regenerateCacheIgnoreError() {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()
	if err := m.regenerateCache(); err != nil {
		glog.Errorf("Error while regenerating Asg cache: %v", err)
	}
}

func (m *AwsManager) regenerateCache() error {
	// newCache := map[config.InstanceConfig]*config.ScalingConfig{}
	newCache := make(map[AwsRef]*Asg)

	for _, asg := range m.asgs {
		glog.V(4).Infof("Regenerating ASG information for %s", asg.basename)
		params := &autoscaling.DescribeAutoScalingGroupsInput{
			AutoScalingGroupNames: []*string{aws.String(asg.basename)},
			MaxRecords:            aws.Int64(1),
		}
		groups, err := m.service.DescribeAutoScalingGroups(params)
		if err != nil {
			glog.V(4).Infof("Failed ASG info request for %s: %v", asg.basename, err)
			return err
		}
		// TODO: check for nil pointers
		group := *groups.AutoScalingGroups[0]

		for _, instance := range group.Instances {
			// TODO fewer queries
			params := &autoscaling.DescribeAutoScalingInstancesInput{
				InstanceIds: []*string{
					aws.String(*instance.InstanceId),
				},
				MaxRecords: aws.Int64(1),
			}
			resp, err := m.service.DescribeAutoScalingInstances(params)

			if err != nil {
				return err
			}
			details := *resp.AutoScalingInstances[0]
			newCache[AwsRef{Zone: *details.AvailabilityZone, Name: *instance.InstanceId}] = asg.config
		}
	}

	m.asgCache = newCache
	return nil
}
