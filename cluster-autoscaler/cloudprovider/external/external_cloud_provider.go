package external

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/golang/glog"

	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config/dynamic"
	apiv1 "k8s.io/kubernetes/pkg/api/v1"
)

type ExternalCloudProvider struct {
	ExtGroup []*nodeGroup
	url      string
}

type nodeGroup struct {
	minSize int
	maxSize int
	id      string
	nodes   []string
	lock    sync.RWMutex
	url     string
}

func (eg *nodeGroup) MinSize() int {
	return eg.minSize
}

func (eg *nodeGroup) MaxSize() int {
	return eg.maxSize
}

// How many VMs we are going to have
func (eg *nodeGroup) TargetSize() (int, error) {
	eg.lock.RLock()
	defer eg.lock.RUnlock()

	return len(eg.nodes), nil
}

// How many "currently building" VMs needs to be removed
func (eg *nodeGroup) DecreaseTargetSize(delta int) error {
	return fmt.Errorf("external: DecreaseTargetSize not implemented")
}

func (eg *nodeGroup) IncreaseSize(delta int) error {
	resp, err := http.Get(fmt.Sprintf("%s/addNodes?size=%d", eg.url, delta))

	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("Status code != 200!")
	}
	return nil
}

func (eg *nodeGroup) DeleteNodes(nodes []*apiv1.Node) error {
	msg := make(chan error, len(nodes))

	for _, node := range nodes {
		go func(node *apiv1.Node, msg chan error) {
			_, err := http.Get(fmt.Sprintf("%s/removeNode?name=%v", eg.url, node.Spec.ProviderID))
			if err != nil {
				msg <- err
				return
			}
			msg <- nil
		}(node, msg)
	}

	for i := 0; i < len(nodes); i++ {
		err := <-msg
		if err != nil {
			return err
		}
	}

	return nil
}

func (eg *nodeGroup) Id() string {
	return eg.id
}

func (eg *nodeGroup) Debug() string {
	return fmt.Sprintf("%s (%d:%d)", eg.id, eg.MinSize(), eg.MaxSize())
}

// []string is a list of ProviderID, not machine id!
func (eg *nodeGroup) Nodes() ([]string, error) {
	eg.lock.RLock()
	defer eg.lock.RUnlock()

	return eg.nodes, nil
}

func (ext *ExternalCloudProvider) fetchCloudNodes() {
	resp, err := http.Get(fmt.Sprintf("%s/nodeGroups", ext.url))
	if err != nil {
		glog.Warningf("[external] failed to fetch nodes: %v", err)
		return
	}

	var res map[string][]string
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		glog.Warningf("[external] failed to decode request: %v", err)
		return
	}

	for _, group := range ext.ExtGroup {
		group.lock.Lock()
		group.nodes = res[group.id] // default to []string{} if not found
		group.lock.Unlock()
	}
}

func loopForever(interval time.Duration, f func()) {
	for {
		f()
		time.Sleep(interval)
	}
}

func BuildExternalCloudProvider(specs []string, url string) (*ExternalCloudProvider, error) {
	ext := &ExternalCloudProvider{
		url: url,
	}

	for _, spec := range specs {
		groupSpec, err := dynamic.SpecFromString(spec)
		if err != nil {
			return nil, err
		}
		ext.ExtGroup = append(ext.ExtGroup, &nodeGroup{
			minSize: groupSpec.MinSize,
			maxSize: groupSpec.MaxSize,
			id:      groupSpec.Name,
			url:     url,
		})
	}

	go loopForever(10*time.Second, ext.fetchCloudNodes)

	return ext, nil
}

func (ext *ExternalCloudProvider) Name() string {
	return "external"
}

func (ext *ExternalCloudProvider) NodeGroups() []cloudprovider.NodeGroup {
	result := make([]cloudprovider.NodeGroup, 0, len(ext.ExtGroup))
	for _, nodeGroup := range ext.ExtGroup {
		result = append(result, nodeGroup)
	}

	return result
}

func (ext *ExternalCloudProvider) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	for _, nodeGroup := range ext.ExtGroup {
		nodeGroup.lock.RLock()
		for _, name := range nodeGroup.nodes {
			if name == node.Spec.ProviderID {
				nodeGroup.lock.RUnlock()
				return nodeGroup, nil
			}
		}
		nodeGroup.lock.RUnlock()
	}
	return nil, nil
}

// func (eg *nodeGroup) TemplateNodeInfo() (*schedulercache.NodeInfo, error) {
// 	return nil, cloudprovider.ErrNotImplemented
// }

// func (ext *ExternalCloudProvider) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
// 	return nil, cloudprovider.ErrNotImplemented
// }
