package static

import (
	"github.com/go-kit/kit/loadbalancer"
	"github.com/go-kit/kit/loadbalancer/fixed"
	"github.com/go-kit/kit/log"
)

// Publisher yields a set of static endpoints as produced by the passed factory.
type Publisher struct{ *fixed.Publisher }

// NewPublisher returns a static endpoint Publisher.
func NewPublisher(instances []string, factory loadbalancer.Factory, logger log.Logger) Publisher {
	logger = log.NewContext(logger).With("component", "Static Publisher")
	p := Publisher{
		fixed.NewPublisher(factory, logger),
	}
	p.Replace(instances)
	return p
}
