package provider

import (
	kube_api "k8s.io/kubernetes/pkg/api"
)

type Provider interface {
	IsScaleDownPossible(node *kube_api.Node) (bool, error)
	AreAllNodeGroupsReady(existingNodes []*kube_api.Node) (bool, error)
	DeleteNode(node *kube_api.Node) error
	GetNodeGroups(existingNodes []*kube_api.Node) ([]NodeGroup, error)
}

type NodeGroup interface {
	IsScaleUpPossible() (bool, error)
	GetCurrentSize() (int, error)
	GetSampleNode() *kube_api.Node
	SetSize(size int) error
}
