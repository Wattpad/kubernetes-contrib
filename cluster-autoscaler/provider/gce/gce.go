package gce

import (
	"fmt"

	"github.com/golang/glog"
	"k8s.io/contrib/cluster-autoscaler/config"
	"k8s.io/contrib/cluster-autoscaler/provider"
	"k8s.io/contrib/cluster-autoscaler/utils/gce"
	kube_api "k8s.io/kubernetes/pkg/api"
)

func NewGceProvider(manager *gce.GceManager, migConfigs []*config.MigConfig) *GceProvider {
	return &GceProvider{
		Manager:    manager,
		MigConfigs: migConfigs,
	}
}

type GceProvider struct {
	Manager    *gce.GceManager
	MigConfigs []*config.MigConfig
}

func (g *GceProvider) IsScaleDownPossible(node *kube_api.Node) (bool, error) {
	// Check mig size.
	instance, err := config.InstanceConfigFromProviderId(node.Spec.ProviderID)
	if err != nil {
		glog.Errorf("Error while parsing providerid of %s: %v", node.Name, err)
		return false, err
	}
	migConfig, err := g.Manager.GetMigForInstance(instance)
	if err != nil {
		glog.Errorf("Error while checking mig config for instance %v: %v", instance, err)
		return false, err
	}
	size, err := g.Manager.GetMigSize(migConfig)
	if err != nil {
		glog.Errorf("Error while checking mig size for instance %v: %v", instance, err)
		return false, err
	}

	if size <= int64(migConfig.MinSize) {
		glog.V(1).Infof("Skipping %s - mig min size reached", node.Name)
		return false, err
	}

	return true, nil
}

func (g *GceProvider) DeleteNode(node *kube_api.Node) error {
	instanceConfig, err := config.InstanceConfigFromProviderId(node.Spec.ProviderID)
	if err != nil {
		glog.Errorf("Failed to get instance config for %s: %v", node.Name, err)
		return err
	}

	err = g.Manager.DeleteInstances([]*config.InstanceConfig{instanceConfig})
	if err != nil {
		glog.Errorf("Failed to delete instance %v: %v", instanceConfig, err)
		return err
	}
	return nil
}

func (g *GceProvider) GetNodeGroups(nodes []*kube_api.Node) ([]provider.NodeGroup, error) {
	migConfigs := make(map[*config.MigConfig]bool)
	nodeGroups := []provider.NodeGroup{}

	for _, node := range nodes {
		instanceConfig, err := config.InstanceConfigFromProviderId(node.Spec.ProviderID)
		if err != nil {
			return []provider.NodeGroup{}, err
		}

		migConfig, err := g.Manager.GetMigForInstance(instanceConfig)
		if err != nil {
			return []provider.NodeGroup{}, err
		}

		if !migConfigs[migConfig] {
			migConfigs[migConfig] = true
			nodeGroups = append(nodeGroups, &gceNodeGroup{
				manager:    g.Manager,
				migConfig:  migConfig,
				sampleNode: node,
			})
		}

	}
	return nodeGroups, nil
}

type gceNodeGroup struct {
	manager    *gce.GceManager
	migConfig  *config.MigConfig
	sampleNode *kube_api.Node
}

func (g *gceNodeGroup) IsScaleUpPossible() (bool, error) {
	currentSize, err := g.manager.GetMigSize(g.migConfig)
	if err != nil {
		glog.Errorf("Failed to get MIG size: %v", err)
		return false, err
	}
	if currentSize >= int64(g.migConfig.MaxSize) {
		// skip this mig.
		glog.V(4).Infof("Skipping MIG %s - max size reached", g.migConfig.Url())
		return false, nil
	}

	return true, nil
}

func (g *gceNodeGroup) GetSampleNode() *kube_api.Node {
	return g.sampleNode
}

func (g *gceNodeGroup) SetSize(size int) error {
	if size >= g.migConfig.MaxSize {
		glog.V(1).Infof("Capping size to MAX (%d)", g.migConfig.MaxSize)
		size = g.migConfig.MaxSize
	}
	glog.V(1).Infof("Setting %s size to %d", g.migConfig.Url(), size)

	if err := g.manager.SetMigSize(g.migConfig, int64(size)); err != nil {
		return fmt.Errorf("failed to set MIG size: %v", err)
	}

	return nil
}

func (g *gceNodeGroup) GetCurrentSize() (int, error) {
	currentSize, err := g.manager.GetMigSize(g.migConfig)
	if err != nil {
		return 0, fmt.Errorf("failed to get MIG size: %v", err)
	}
	return int(currentSize), nil
}
