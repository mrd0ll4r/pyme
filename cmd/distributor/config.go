package main

import (
	"errors"
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"

	"github.com/mrd0ll4r/pyme/tasks/distributor"
)

// ConfigFile represents a namespaced YAML configuration file for the
// distributor command.
type ConfigFile struct {
	MainConfigBlock struct {
		HTTPEndpoint       string             `yaml:"http_endpoint"`
		HTTPPrintDebugLogs bool               `yaml:"http_print_debug_logs"`
		DistributorConfig  distributor.Config `yaml:"logic"`
	} `yaml:"distributor"`
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
