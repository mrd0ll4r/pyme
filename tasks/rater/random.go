package rater

import (
	"math/rand"
	"net/url"

	"github.com/mrd0ll4r/pyme/tasks"
)

type randomRater struct{}

func (r *randomRater) Rate(_ *url.URL) (float64, error) {
	return rand.Float64(), nil
}

func (r *randomRater) Scheme() string {
	return "random"
}

// NewRandomRater returns a new Rater for the random scheme.
func NewRandomRater() tasks.Rater {
	return &randomRater{}
}
