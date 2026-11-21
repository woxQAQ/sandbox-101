package ephemeral

import (
	"net"
	"sync"
)

type closeWriter interface {
	// CloseWrite sends EOF to the peer while leaving the read side open.
	CloseWrite() error
}

// ConnectionIdentity pins the configuration that authorized a socket. Later
// settings or instances cannot upgrade this socket's original admission.
type ConnectionIdentity struct {
	SandboxID  string
	Instance   string
	Generation string
}

type Connection struct {
	net.Conn

	registry *Registry
	identity ConnectionIdentity
	done     chan struct{}
	once     sync.Once
}

func (c *Connection) Identity() ConnectionIdentity {
	return c.identity
}

func (c *Connection) Done() <-chan struct{} {
	return c.done
}

func (c *Connection) BelongsTo(registry *Registry) bool {
	return c.registry == registry
}

func (c *Connection) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() {
		close(c.done)
		c.registry.remove(c)
	})
	return err
}

func (c *Connection) CloseWrite() error {
	if half, ok := c.Conn.(closeWriter); ok {
		return half.CloseWrite()
	}
	return nil
}
