// Package process wraps long-lived JSON Lines worker processes.
package process

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
)

// Command describes one worker process.
type Command struct {
	Name string
	Args []string
	Dir  string
	Env  []string
}

// Client sends JSON-line requests to one long-lived process.
type Client[Request any, Response any] struct {
	id     int
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	mu     sync.Mutex
	seq    atomic.Uint64
}

// Start launches a process worker.
func Start[Request any, Response any](ctx context.Context, id int, command Command) (*Client[Request, Response], error) {
	if command.Name == "" {
		return nil, errors.New("process: command name is required")
	}

	cmd := exec.CommandContext(ctx, command.Name, command.Args...)
	cmd.Dir = command.Dir
	cmd.Env = append(os.Environ(), command.Env...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if cmd.Stderr == nil {
		cmd.Stderr = os.Stderr
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	client := &Client[Request, Response]{
		id:     id,
		cmd:    cmd,
		stdin:  stdin,
		stdout: bufio.NewReader(stdoutPipe),
	}

	go func() {
		_ = cmd.Wait()
	}()

	return client, nil
}

// Call sends one JSON-line request and waits for one JSON-line response.
func (c *Client[Request, Response]) Call(request Request) (Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	payload, err := json.Marshal(request)
	if err != nil {
		var zero Response
		return zero, err
	}

	if _, err := c.stdin.Write(append(payload, '\n')); err != nil {
		var zero Response
		return zero, err
	}

	line, err := c.stdout.ReadBytes('\n')
	if err != nil {
		var zero Response
		return zero, err
	}

	var response Response
	if err := json.Unmarshal(line, &response); err != nil {
		return response, err
	}
	return response, nil
}

// NextID returns a stable request id prefix for the worker.
func (c *Client[Request, Response]) NextID() string {
	return fmt.Sprintf("w%d-%d", c.id, c.seq.Add(1))
}

// Close terminates the process worker.
func (c *Client[Request, Response]) Close() error {
	var err error
	if c.stdin != nil {
		err = c.stdin.Close()
	}
	if c.cmd != nil && c.cmd.Process != nil {
		if killErr := c.cmd.Process.Kill(); err == nil {
			err = killErr
		}
	}
	return err
}
