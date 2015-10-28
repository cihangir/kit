package loadbalancer_test

import (
	"fmt"
	"io"
	"reflect"
	"strconv"
	"testing"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/loadbalancer"
	"github.com/go-kit/kit/loadbalancer/fixed"
	"github.com/go-kit/kit/log"
	"golang.org/x/net/context"
)

func TestRoundRobinDistribution(t *testing.T) {
	var (
		ctx    = context.Background()
		n      = 3
		hosts  = make([]string, n)
		counts = make([]int, n)

		f = func(s string) (endpoint.Endpoint, io.Closer, error) {
			return func(context.Context, interface{}) (interface{}, error) {
				i, err := strconv.Atoi(s)
				if err != nil {
					return nil, fmt.Errorf("Can not convert %+v to integer", s)
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
	lb := loadbalancer.NewRoundRobin(p)

	for i, want := range [][]int{
		{1, 0, 0},
		{1, 1, 0},
		{1, 1, 1},
		{2, 1, 1},
		{2, 2, 1},
		{2, 2, 2},
		{3, 2, 2},
	} {
		e, err := lb.Endpoint()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e(ctx, struct{}{}); err != nil {
			t.Error(err)
		}
		if have := counts; !reflect.DeepEqual(want, have) {
			t.Fatalf("%d: want %v, have %v", i, want, have)
		}

	}
}

func TestRoundRobinBadPublisher(t *testing.T) {
	t.Skip("TODO")
}
