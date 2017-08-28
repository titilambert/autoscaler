package external

// Config file example: --cloud-config=cloud-config.yml -- Note that {*} will be replaced with the appropriate information.
// ---
// nodesURL: "http://127.0.0.1:8080/api/v1beta/autoscaler/nodes"
// scaleUpURL: "http://127.0.0.1:8080/api/v1beta/autoscaler/scaleUp/{nodeGroupID}/{size}"
// scaleDownURL: "http://127.0.0.1:8080/api/v1beta/autoscaler/scaleDown/{nodeGroupID}/{nodeID}"

// TODO: Comments
// TODO: Better check for http response | INPROGRESS
// TODO: Tests

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"gopkg.in/yaml.v2"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config/dynamic"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	apiv1 "k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"
)

type CloudConfig struct {
	nodeGroups        []*nodeGroup
	NodesTemplate     string `yaml:"nodesTemplate"`
	ScaleUpTemplate   string `yaml:"scaleUpTemplate"`
	ScaleDownTemplate string `yaml:"scaleDownTemplate"`
}

type nodeGroup struct {
	minSize     int
	maxSize     int
	id          string
	nodes       []string
	lock        sync.RWMutex
	cloudConfig *CloudConfig
}

func (n *nodeGroup) MinSize() int {
	return n.minSize
}

func (n *nodeGroup) MaxSize() int {
	return n.maxSize
}

// How many VMs we are going to have
func (n *nodeGroup) TargetSize() (int, error) {
	n.lock.RLock()
	defer n.lock.RUnlock()

	return len(n.nodes), nil
}

// How many "currently building" VMs needs to be removed
func (n *nodeGroup) DecreaseTargetSize(delta int) error {
	return fmt.Errorf("external: DecreaseTargetSize not implemented")
}

func (n *nodeGroup) IncreaseSize(delta int) error {
	scaleUpURL := strings.Replace(n.cloudConfig.ScaleUpTemplate, "{nodeGroupID}", n.id, 1)
	scaleUpURL = strings.Replace(scaleUpURL, "{size}", strconv.Itoa(delta), 1)
	resp, err := http.Get(scaleUpURL)

	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("Status code != 200!")
	}
	return nil
}

func (n *nodeGroup) DeleteNodes(nodes []*apiv1.Node) error {
	msg := make(chan error, len(nodes))

	for _, node := range nodes {
		go func(node *apiv1.Node, msg chan error) {
			scaleDownURL := strings.Replace(n.cloudConfig.ScaleDownTemplate, "{nodeGroupID}", n.id, 1)
			// TODO: send node.Spec.ProviderID instead of node.Name (need to move to a POST though)
			scaleDownURL = strings.Replace(scaleDownURL, "{nodeID}", node.Name, 1)

			_, err := http.Get(scaleDownURL)
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

func (n *nodeGroup) Id() string {
	return n.id
}

func (n *nodeGroup) Debug() string {
	return fmt.Sprintf("%s (%d:%d)", n.id, n.MinSize(), n.MaxSize())
}

// []string is a list of ProviderID, not machine id!
func (n *nodeGroup) Nodes() ([]string, error) {
	n.lock.RLock()
	defer n.lock.RUnlock()

	return n.nodes, nil
}

// TODO: Missing code + reason in case of status != success.
// {
//	"status": "success",
//	"groups": [{
//		"nodes": ["node1", "node2"]
//		"id": "foo"
//	  },
//	  {
//		"nodes": [],
//		"id": "bar"
//	  }]
// }
type nodesResponse struct {
	Status string
	Groups []struct {
		Nodes []string
		Id    string
	}
}

func (c *CloudConfig) fetchCloudNodes() {
	resp, err := http.Get(c.NodesTemplate)
	if err != nil {
		glog.Warningf("[external] failed to fetch nodes: %v", err)
		return
	}

	var response nodesResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		glog.Warningf("[external] failed to decode request: %v", err)
		return
	}

	if response.Status != "success" {
		glog.Warningf("[external] list nodes request failed: %v", response.Status)
	}

	for _, group := range c.nodeGroups {
		for _, apigroup := range response.Groups {
			// TODO: doc - the api should always return all groups.
			// TODO: what if not?
			if group.id == apigroup.Id {
				group.lock.Lock()
				group.nodes = apigroup.Nodes
				group.lock.Unlock()
				break
			}
		}
	}
}

func loopForever(interval time.Duration, f func()) {
	for {
		f()
		time.Sleep(interval)
	}
}

func BuildExternalCloudProvider(specs []string, config io.Reader) (*CloudConfig, error) {
	configBytes, err := ioutil.ReadAll(config)
	if err != nil {
		return nil, err
	}

	// The unmarshalling is the most WTF thing I've seen in Go.
	// It only copy a value if:
	//   - The field is exported
	//   - The name of the field is all lower case except for the first character (Likethis).
	// 1) This means that sometimes, you can write a string how ever you want (LiKeThIs), and sometimes,
	// you must match an exact string (e.g. when using `yaml:"likeThis"`).
	// It must be one of the most confusing thing when a user is seeing this.
	// 2) Now, why only exported fields? It only adds a meaningless restriction. I fail to see any kind of benefits
	// from enforcing such a policy.
	// Maybe I'm missing something. Rant over.
	var cloudConfig CloudConfig
	err = yaml.Unmarshal(configBytes, &cloudConfig)
	if err != nil {
		return nil, err
	}

	for _, spec := range specs {
		groupSpec, err := dynamic.SpecFromString(spec, false)
		if err != nil {
			return nil, err
		}
		cloudConfig.nodeGroups = append(cloudConfig.nodeGroups, &nodeGroup{
			minSize:     groupSpec.MinSize,
			maxSize:     groupSpec.MaxSize,
			id:          groupSpec.Name,
			cloudConfig: &cloudConfig,
		})
	}

	go loopForever(10*time.Second, cloudConfig.fetchCloudNodes)

	return &cloudConfig, nil
}

func (c *CloudConfig) Name() string {
	return "external"
}

func (c *CloudConfig) NodeGroups() []cloudprovider.NodeGroup {
	result := make([]cloudprovider.NodeGroup, 0, len(c.nodeGroups))
	for _, nodeGroup := range c.nodeGroups {
		result = append(result, nodeGroup)
	}

	return result
}

func (c *CloudConfig) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	for _, nodeGroup := range c.nodeGroups {
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

func (n *nodeGroup) TemplateNodeInfo() (*schedulercache.NodeInfo, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (c *CloudConfig) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	return nil, cloudprovider.ErrNotImplemented
}
