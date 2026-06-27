package process

import (
	"context"
	"errors"
	"sync/atomic"
)

// CommandFactory returns the command for a worker index.
type CommandFactory func(index int) (Command, error)

// Pool manages a fixed set of long-lived process clients.
type Pool[Request any, Response any] struct {
	clients []*Client[Request, Response]
	next    atomic.Uint64
}

// NewPool starts size process workers.
func NewPool[Request any, Response any](ctx context.Context, size int, factory CommandFactory) (*Pool[Request, Response], error) {
	if size < 1 {
		size = 1
	}
	if factory == nil {
		return nil, errors.New("process: nil command factory")
	}

	clients := make([]*Client[Request, Response], 0, size)
	for i := 0; i < size; i++ {
		command, err := factory(i)
		if err != nil {
			closeAll(clients)
			return nil, err
		}

		client, err := Start[Request, Response](ctx, i, command)
		if err != nil {
			closeAll(clients)
			return nil, err
		}
		clients = append(clients, client)
	}

	return &Pool[Request, Response]{clients: clients}, nil
}

// Len returns the number of workers.
func (p *Pool[Request, Response]) Len() int {
	return len(p.clients)
}

// Call sends a request to the next worker in round-robin order.
func (p *Pool[Request, Response]) Call(request Request) (Response, error) {
	if len(p.clients) == 0 {
		var zero Response
		return zero, errors.New("process: worker pool is empty")
	}
	index := int(p.next.Add(1)-1) % len(p.clients)
	return p.CallAt(index, request)
}

// CallAt sends a request to a specific worker index.
func (p *Pool[Request, Response]) CallAt(index int, request Request) (Response, error) {
	if len(p.clients) == 0 {
		var zero Response
		return zero, errors.New("process: worker pool is empty")
	}
	index %= len(p.clients)
	if index < 0 {
		index += len(p.clients)
	}
	return p.clients[index].Call(request)
}

// ClientAt returns a specific worker client.
func (p *Pool[Request, Response]) ClientAt(index int) (*Client[Request, Response], error) {
	if len(p.clients) == 0 {
		return nil, errors.New("process: worker pool is empty")
	}
	index %= len(p.clients)
	if index < 0 {
		index += len(p.clients)
	}
	return p.clients[index], nil
}

// Close terminates all process workers.
func (p *Pool[Request, Response]) Close() error {
	var err error
	for _, client := range p.clients {
		if closeErr := client.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

func closeAll[Request any, Response any](clients []*Client[Request, Response]) {
	for _, client := range clients {
		_ = client.Close()
	}
}
