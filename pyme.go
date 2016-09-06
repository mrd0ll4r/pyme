package pyme

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/mitchellh/go-homedir"
	"github.com/mrd0ll4r/pyme/random"
)

// NodeID is a unique identifier for a node.
type NodeID string

// NewNodeID generates a new NodeID as a random alphanumeric string of length
// 20.
func NewNodeID() NodeID {
	rand.Seed(time.Now().UnixNano())
	s := rand.NewSource(rand.Int63())
	return NodeID(random.AlphaNumericString(s, 20))
}

func (id NodeID) writeTo(w io.Writer) error {
	n, err := w.Write([]byte(id))
	if err != nil {
		return err
	}
	if n != len(id) {
		return fmt.Errorf("wrote %d bytes, expected %d", n, len(id))
	}

	return nil
}

// SaveToFile saves the NodeID to the file identified by the given fileName.
func (id NodeID) SaveToFile(fileName string) error {
	f, err := os.Create(fileName)
	if err != nil {
		return errors.Wrap(err, "unable to create file")
	}
	defer f.Close()

	return id.writeTo(f)
}

// LoadNodeIDFromFile loads a NodeID from the file identified by the given
// fileName
func LoadNodeIDFromFile(fileName string) (NodeID, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return NodeID(""), errors.Wrap(err, "unable to open file")
	}
	defer f.Close()

	s, err := ioutil.ReadAll(f)
	if err != nil {
		return NodeID(""), errors.Wrap(err, "unable to read from file")
	}

	return NodeID(s), nil
}

// GetHomeDir returns the location of the home directory of the current user.
func GetHomeDir() (string, error) {
	return homedir.Dir()
}
