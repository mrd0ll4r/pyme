package main

import (
	"errors"
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"

	"github.com/mrd0ll4r/pyme/tasks/nodeserver"
	"github.com/mrd0ll4r/pyme/tasks/rater"
)

// ConfigFile represents a namespaced YAML configuration file for the
// nodeserver command.
type ConfigFile struct {
	MainConfigBlock struct {
		HTTPEndpoint           string                       `yaml:"http_endpoint"`
		HTTPPrintDebugLogs     bool                         `yaml:"http_print_debug_logs"`
		ManualDistributors     []string                     `yaml:"distributors"`
		PYMCClusterRaterConfig rater.PYMEClusterRaterConfig `yaml:"pyme_cluster_rater"`
		NodeServerConfig       nodeserver.Config            `yaml:"logic"`
		UseMDNS                bool                         `yaml:"use_mdns"`
		AnnounceIP             string                       `yaml:"announce_ip"`
	} `yaml:"nodeserver"`
}

// parseConfigFile returns a new ConfigFile given the path to a YAML
// configuration file.
func parseConfigFile(path string) (*ConfigFile, error) {
	if path == "" {
		return nil, errors.New("no config path specified")
	}

	f, err := os.Open(os.ExpandEnv(path))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	contents, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	var cfgFile ConfigFile
	err = yaml.Unmarshal(contents, &cfgFile)
	if err != nil {
		return nil, err
	}

	return &cfgFile, nil
}
