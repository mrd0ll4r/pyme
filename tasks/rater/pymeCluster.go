package rater

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/mrd0ll4r/pyme/tasks"
)

// PYMEClusterRaterConfig represents the configuration for a Rater for the
// pyme-cluster scheme.
type PYMEClusterRaterConfig struct {
	DataServerRootDir string `yaml:"data_server_root_dir"`
}

type pymeClusterRater struct {
	dataServerRootDir string
}

func (r *pymeClusterRater) Rate(u *url.URL) (float64, error) {
	if strings.ToLower(u.Scheme) != "pyme-cluster" {
		return 0, fmt.Errorf("invalid scheme %q for pyme-cluster rater", u.Scheme)
	}

	path := filepath.Join(r.dataServerRootDir, u.Path)

	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return 0, nil
		} else if os.IsNotExist(err) {
			return 1, nil
		}
	}
	// if err is nil this returns 0,nil, which is what we want: no error
	// calling stat means the file exists.
	return 0, err
}

func (r *pymeClusterRater) Scheme() string {
	return "pyme-cluster"
}

// NewPYMEClusterRater returns a new Rater for the pyme-cluster scheme from
// the config.
func NewPYMEClusterRater(cfg PYMEClusterRaterConfig) tasks.Rater {
	return &pymeClusterRater{
		dataServerRootDir: cfg.DataServerRootDir,
	}
}
