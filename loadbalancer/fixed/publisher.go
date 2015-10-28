package fixed

import (
	"io"
	"sync"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/loadbalancer"
	"github.com/go-kit/kit/log"
)

// Publisher holds the endpoints that need to be deallocated when they're no
// longer useful. Clients update the endpoints by providing a current set of
// instance strings. The publisher converts each instance string to an endpoint
// and a closer via the factory function.
//
// Instance strings are assumed to be unique and are used as keys. Endpoints
// that were in the previous set of instances and are not in the current set
// are considered invalid and closed.
//
// Fixed publisher is designed to be used in your publisher implementation.
type Publisher struct {
	mtx    sync.RWMutex
	f      loadbalancer.Factory
	m      []endpointCloser
	logger log.Logger
}

// NewPublisher returns a fixed endpoint Publisher.
func NewPublisher(f loadbalancer.Factory, logger log.Logger) *Publisher {
	return &Publisher{
		f:      f,
		m:      make([]endpointCloser, 0),
		logger: logger,
	}
}

type endpointCloser struct {
	endpoint.Endpoint
	io.Closer
	instance string
}

// Endpoints implements the Publisher interface.
func (p *Publisher) Endpoints() ([]endpoint.Endpoint, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	a := make([]endpoint.Endpoint, 0, len(p.m))
	for _, ec := range p.m {
		a = append(a, ec.Endpoint)
	}
	return a, nil
}

// Replace replaces the current set of endpoints with endpoints manufactured
// by the passed instances. If the same instance exists in both the existing
// and new sets, it's left untouched.
func (p *Publisher) Replace(instances []string) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	// Produce the current set of endpoints.
	final := make([]endpointCloser, 0, len(instances))

	for _, ec := range p.m {
		exists := false

		for _, instance := range instances {
			if ec.instance == instance {
				exists = true
				break
			}
		}

		// If it already exists, just copy it over.
		if exists {
			final = append(final, ec)
		} else {
			if ec.Closer != nil {
				ec.Closer.Close()
			}
		}
	}

	for _, instance := range instances {
		exists := false
		for _, ec := range p.m {
			if ec.instance == instance {
				exists = true
				break
			}
		}

		// If it doesn't exist, create it.
		if !exists {
			endpoint, closer, err := p.f(instance)
			if err != nil {
				p.logger.Log("instance", instance, "err", err)
				continue
			}
			final = append(final, endpointCloser{endpoint, closer, instance})
		}
	}

	ordered := make([]endpointCloser, len(final))

	// sort instances
	for i, instance := range instances {
		for _, endpoint := range final {
			if endpoint.instance == instance {
				ordered[i] = endpoint
			}
		}
	}

	// Swap and GC.
	p.m = ordered
}
