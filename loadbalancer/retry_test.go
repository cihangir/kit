package loadbalancer_test

import (
	"errors"
	"io"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/loadbalancer"
	"github.com/go-kit/kit/loadbalancer/fixed"
	"github.com/go-kit/kit/log"
)

func TestRetryMaxTotalFail(t *testing.T) {
	var (
		f = func(s string) (endpoint.Endpoint, io.Closer, error) {
			return nil, nil, nil
		}
		p     = fixed.NewPublisher(f, log.NewNopLogger())
		lb    = loadbalancer.NewRoundRobin(p)
		retry = loadbalancer.Retry(999, time.Second, lb) // lots of retries
		ctx   = context.Background()
	)
	if _, err := retry(ctx, struct{}{}); err == nil {
		t.Errorf("expected error, got none") // should fail
	}
}

func TestRetryMaxPartialFail(t *testing.T) {
	type returnVal struct {
		data interface{}
		err  error
	}

	returnVals := []returnVal{
		{
			data: nil,
			err:  errors.New("error one"),
		},
		{
			data: nil,
			err:  errors.New("error two"),
		},
		{
			data: struct{}{},
			err:  nil,
		},
	}
	var (
		f = func(s string) (endpoint.Endpoint, io.Closer, error) {
			return func(context.Context, interface{}) (interface{}, error) {
				i, err := strconv.Atoi(s)
				if err != nil {
					return nil, err
				}

				r := returnVals[i]
				return r.data, r.err

			}, nil, nil
		}

		retries = len(returnVals) - 1 // not quite enough retries
		p       = fixed.NewPublisher(f, log.NewNopLogger())
		lb      = loadbalancer.NewRoundRobin(p)
		ctx     = context.Background()
	)
	hosts := make([]string, len(returnVals))
	for i := 0; i < len(returnVals); i++ {
		hosts[i] = strconv.Itoa(i)
	}

	p.Replace(hosts)

	if _, err := loadbalancer.Retry(retries, time.Second, lb)(ctx, struct{}{}); err == nil {
		t.Errorf("expected error, got none")
	}
}

func TestRetryMaxSuccess(t *testing.T) {
	type returnVal struct {
		data interface{}
		err  error
	}

	returnVals := []returnVal{
		{
			data: nil,
			err:  errors.New("error one"),
		},
		{
			data: nil,
			err:  errors.New("error two"),
		},
		{
			data: struct{}{},
			err:  nil,
		},
	}
	var (
		f = func(s string) (endpoint.Endpoint, io.Closer, error) {
			return func(context.Context, interface{}) (interface{}, error) {
				i, err := strconv.Atoi(s)
				if err != nil {
					return nil, err
				}

				r := returnVals[i]
				return r.data, r.err

			}, nil, nil
		}

		retries = len(returnVals) // not quite enough retries
		p       = fixed.NewPublisher(f, log.NewNopLogger())
		lb      = loadbalancer.NewRoundRobin(p)
		ctx     = context.Background()
	)
	hosts := make([]string, len(returnVals))
	for i := 0; i < len(returnVals); i++ {
		hosts[i] = strconv.Itoa(i)
	}

	p.Replace(hosts)
	if _, err := loadbalancer.Retry(retries, time.Second, lb)(ctx, struct{}{}); err != nil {
		t.Error(err)
	}
}

func TestRetryTimeout(t *testing.T) {
	var (
		step = make(chan struct{})
		e    = func(context.Context, interface{}) (interface{}, error) { <-step; return struct{}{}, nil }
		f    = func(s string) (endpoint.Endpoint, io.Closer, error) {
			return e, nil, nil
		}
		timeout = time.Millisecond
		p       = fixed.NewPublisher(f, log.NewNopLogger())
		retry   = loadbalancer.Retry(999, timeout, loadbalancer.NewRoundRobin(p))
		errs    = make(chan error, 1)
		invoke  = func() { _, err := retry(context.Background(), struct{}{}); errs <- err }
	)
	p.Replace([]string{"endpoint"})
	go func() { step <- struct{}{} }() // queue up a flush of the endpoint
	invoke()                           // invoke the endpoint and trigger the flush
	if err := <-errs; err != nil {     // that should succeed
		t.Error(err)
	}

	go func() { time.Sleep(10 * timeout); step <- struct{}{} }() // a delayed flush
	invoke()                                                     // invoke the endpoint
	if err := <-errs; err != context.DeadlineExceeded {          // that should not succeed
		t.Errorf("wanted %v, got none", context.DeadlineExceeded)
	}
}
