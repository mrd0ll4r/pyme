package calculator

import (
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/mrd0ll4r/pyme/tasks"
)

type localizationCalculator struct {
	raters map[string]tasks.Rater
}

func (c *localizationCalculator) Calculate(t tasks.Task) (float64, error) {
	if t.TaskdefRef != "" {
		// don't bother fetching the definition to calculate the cost
		return 1, nil
	}
	if t.Taskdef == nil {
		return 0, errors.New("invalid task: no TaskdefRef and no Taskdef")
	}

	frameString, ok := t.Taskdef["frameIndex"]
	if !ok {
		return 0, errors.New("invalid localization task: no frame number")
	}

	frame, err := strconv.Atoi(frameString)
	if err != nil {
		return 0, errors.Wrap(err, "invalid localization task: invalid frame number")
	}

	framesURI, ok := t.Inputs["frames"]
	if !ok {
		return 0, errors.New("invalid localization task: no frames URI")
	}

	parsedFramesURI, err := url.Parse(framesURI)
	if err != nil {
		return 0, errors.Wrap(err, "unable to parse frames URI")
	}
	rater, ok := c.raters[parsedFramesURI.Scheme]
	if !ok {
		log.Printf("localization calculator: unknown scheme %q", parsedFramesURI.Scheme)
		return -1, nil
	}

	tmp := ""
	if strings.HasSuffix(framesURI, "/") {
		tmp = fmt.Sprintf("%sframe%d.pzf", framesURI, frame)
	} else {
		tmp = fmt.Sprintf("%s/frame%d.pzf", framesURI, frame)
	}

	frameURI, err := url.Parse(tmp)
	if err != nil {
		return 0, errors.Wrap(err, "unable to construct frame URI")
	}

	cost, err := rater.Rate(frameURI)
	if err != nil {
		return 0, errors.Wrap(err, "unable to rate frame URI")
	}

	return cost, nil
}

func (c *localizationCalculator) TaskType() tasks.TaskType {
	return tasks.Localization
}

// NewLocalizationCalculator returns a new Calculator for localization tasks
// using the given Raters.
func NewLocalizationCalculator(raters []tasks.Rater) tasks.Calculator {
	toReturn := &localizationCalculator{
		raters: make(map[string]tasks.Rater),
	}

	for _, rater := range raters {
		toReturn.raters[rater.Scheme()] = rater
	}

	return toReturn
}
