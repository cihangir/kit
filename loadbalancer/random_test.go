package loadbalancer_test

import (
	"io"
	"math"
	"strconv"
	"testing"

	"golang.org/x/net/context"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/loadbalancer"
	"github.com/go-kit/kit/loadbalancer/fixed"
	"github.com/go-kit/kit/log"
)

func TestRandomDistribution(t *testing.T) {
	var (
		n          = 3
		hosts      = make([]string, n)
		counts     = make([]int, n)
		seed       = int64(123)
		ctx        = context.Background()
		iterations = 100000
		want       = iterations / n
		tolerance  = want / 100 // 1%

		f = func(s string) (endpoint.Endpoint, io.Closer, error) {
			return func(context.Context, interface{}) (interface{}, error) {
				i, err := strconv.Atoi(s)
				if err != nil {
					t.Fatalf("Can not convert %+v to integer", s)
				}

				counts[i]++
				return struct{}{}, nil
			}, nil, nil
		}
		p = fixed.NewPublisher(f, log.NewNopLogger())
	)

	for i := 0; i < n; i++ {
		hosts[i] = strconv.Itoa(i)
	}

	p.Replace(hosts)

	lb := loadbalancer.NewRandom(p, seed)

	for i := 0; i < iterations; i++ {
		e, err := lb.Endpoint()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e(ctx, struct{}{}); err != nil {
			t.Error(err)
		}
	}

	for i, have := range counts {
		if math.Abs(float64(want-have)) > float64(tolerance) {
			t.Errorf("%d: want %d, have %d", i, want, have)
		}
	}
}

func TestRandomBadPublisher(t *testing.T) {
	t.Skip("TODO")
}

func TestRandomNoEndpoints(t *testing.T) {
	f := func(s string) (endpoint.Endpoint, io.Closer, error) {
		return nil, nil, nil
	}
	p := fixed.NewPublisher(f, log.NewNopLogger())

	lb := loadbalancer.NewRandom(p, 123)
	_, have := lb.Endpoint()
	if want := loadbalancer.ErrNoEndpoints; want != have {
		t.Errorf("want %q, have %q", want, have)
	}
}
