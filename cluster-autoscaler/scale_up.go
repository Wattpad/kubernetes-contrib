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

package main

import (
	"fmt"

	"k8s.io/contrib/cluster-autoscaler/estimator"
	"k8s.io/contrib/cluster-autoscaler/provider"
	"k8s.io/contrib/cluster-autoscaler/simulator"

	kube_api "k8s.io/kubernetes/pkg/api"
	kube_record "k8s.io/kubernetes/pkg/client/record"
	kube_client "k8s.io/kubernetes/pkg/client/unversioned"

	"github.com/golang/glog"
)

// ExpansionOption describes an option to expand the cluster.
type ExpansionOption struct {
	nodeGroup provider.NodeGroup
	estimator *estimator.BasicNodeEstimator
}

// ScaleUp tries to scale the cluster up. Return true if it found a way to increase the size,
// false if it didn't and error if an error occured.
func ScaleUp(
	unschedulablePods []*kube_api.Pod,
	nodes []*kube_api.Node,
	provider provider.Provider,
	kubeClient *kube_client.Client,
	predicateChecker *simulator.PredicateChecker,
	recorder kube_record.EventRecorder) (bool, error) {

	// From now on we only care about unschedulable pods that were marked after the newest
	// node became available for the scheduler.
	if len(unschedulablePods) == 0 {
		glog.V(1).Info("No unschedulable pods")
		return false, nil
	}

	for _, pod := range unschedulablePods {
		glog.V(1).Infof("Pod %s/%s is unschedulable", pod.Namespace, pod.Name)
	}

	expansionOptions := make([]ExpansionOption, 0)
	podsRemainUnshedulable := make(map[*kube_api.Pod]struct{})

	nodeGroups, err := provider.GetNodeGroups(nodes)
	if err != nil {
		return false, fmt.Errorf("failed to get NodeGroups for migs: %v", err)
	}

	for _, nodeGroup := range nodeGroups {
		ok, err := nodeGroup.IsScaleUpPossible()
		if err != nil {
			glog.Errorf("Could not determine scale-up possibility for nodeGroup %v: %v", nodeGroup, err)
			continue
		}

		if !ok {
			continue
		}

		option := ExpansionOption{
			nodeGroup: nodeGroup,
			estimator: estimator.NewBasicNodeEstimator(),
		}

		migHelpsSomePods := false

		for _, pod := range unschedulablePods {
			nodeInfo, err := simulator.BuildNodeInfoForNode(nodeGroup.GetSampleNode(), kubeClient)
			if err != nil {
				glog.Errorf("Error getting nodeInfo for nodeGroup %v: %v", nodeGroup, err)
				continue
			}

			err = predicateChecker.CheckPredicates(pod, nodeInfo)
			if err == nil {
				migHelpsSomePods = true
				option.estimator.Add(pod)
			} else {
				glog.V(2).Infof("Scale-up predicate failed: %v", err)
				podsRemainUnshedulable[pod] = struct{}{}
			}
		}

		if migHelpsSomePods {
			expansionOptions = append(expansionOptions, option)
		}
	}

	// Pick some expansion option.
	bestOption := BestExpansionOption(expansionOptions)
	if bestOption != nil && bestOption.estimator.GetCount() > 0 {
		glog.V(1).Infof("Best option to resize: %s", bestOption.nodeGroup)

		nodeInfo, err := simulator.BuildNodeInfoForNode(bestOption.nodeGroup.GetSampleNode(), kubeClient)
		if err != nil {
			glog.Errorf("Error getting nodeInfo for nodeGroup %v: %v", bestOption.nodeGroup, err)
			return false, err
		}

		estimate, report := bestOption.estimator.Estimate(nodeInfo.Node())
		glog.V(1).Info(bestOption.estimator.GetDebug())
		glog.V(1).Info(report)
		glog.V(1).Infof("Estimated %d nodes needed in %s", estimate, bestOption.nodeGroup)

		currentSize, err := bestOption.nodeGroup.GetCurrentSize()
		if err != nil {
			return false, fmt.Errorf("Error getting nodeGroup size: %v", err)
		}
		newSize := currentSize + estimate

		if err = bestOption.nodeGroup.SetSize(estimate); err != nil {
			return false, err
		}

		for pod := range bestOption.estimator.FittingPods {
			recorder.Eventf(pod, kube_api.EventTypeNormal, "TriggeredScaleUp",
				"pod triggered scale-up, nodeGroup: %s, sizes (current/new): %d/%d", bestOption.nodeGroup, currentSize, newSize)
		}

		return true, nil
	}
	for pod := range podsRemainUnshedulable {
		recorder.Event(pod, kube_api.EventTypeNormal, "NotTriggerScaleUp",
			"pod didn't trigger scale-up (it wouldn't fit if a new node is added)")
	}

	return false, nil
}
