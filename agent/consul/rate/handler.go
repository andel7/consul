// package rate implements server-side RPC rate limiting.
package rate

import (
	"context"
	"errors"
	"net"
	"sync/atomic"

	"github.com/hashicorp/consul/agent/consul/rate/multilimiter"
)

var (
	// ErrRetryElsewhere indicates that the operation was not allowed because the
	// rate limit was exhausted, but may succeed on a different server.
	//
	// Results in a RESOURCE_EXHAUSTED or "429 Too Many Requests" response.
	ErrRetryElsewhere = errors.New("rate limit exceeded, try a different server")

	// ErrRetryLater indicates that the operation was not allowed because the rate
	// limit was exhausted, and trying a different server won't help (e.g. because
	// the operation can only be performed on the leader).
	//
	// Results in an UNAVAILABLE or "503 Service Unavailable" response.
	ErrRetryLater = errors.New("rate limit exceeded, try again later")
)

// Mode determines the action that will be taken when a rate limit has been
// exhausted (e.g. log and allow, or reject).
type Mode string

const (
	// ModeDisabled causes the handler to not register itself.
	ModeDisabled Mode = "disabled"

	// ModePermissive causes the handler to log the rate-limited operation but
	// still allow it to proceed.
	ModePermissive Mode = "permissive"

	// ModeEnforcing causes the handler to reject the rate-limited operation.
	ModeEnforcing Mode = "enforcing"
)

var modeToName = map[Mode]string{
	ModeDisabled:   "disabled",
	ModeEnforcing:  "enforcing",
	ModePermissive: "permissive",
}
var modeFromName = map[string]Mode{
	"disabled":   ModeDisabled,
	"enforcing":  ModeEnforcing,
	"permissive": ModePermissive,
	// If the value is not found in the persisted config file, then use the
	// disabled default.
	"": ModeDisabled,
}

func (m Mode) String() string {
	return modeToName[m]
}

// RequestLimitsModeFromName will unmarshal the string form of a configMode.
func RequestLimitsModeFromName(name string) (Mode, bool) {
	s, ok := modeFromName[name]
	return s, ok
}

// RequestLimitsModeFromNameWithDefault will unmarshal the string form of a configMode.
func RequestLimitsModeFromNameWithDefault(name string) Mode {
	s, ok := modeFromName[name]
	if !ok {
		return ModeDisabled
	}
	return s
}

// OperationType is the type of operation the client is attempting to perform.
type OperationType int

const (
	// OperationTypeRead represents a read operation.
	OperationTypeRead OperationType = iota

	// OperationTypeWrite represents a write operation.
	OperationTypeWrite
)

// Operation the client is attempting to perform.
type Operation struct {
	// Name of the RPC endpoint (e.g. "Foo.Bar" for net/rpc and "/foo.service/Bar" for gRPC).
	Name string

	// SourceAddr is the client's (or forwarding server's) IP address.
	SourceAddr net.Addr

	// Type of operation to be performed (e.g. read or write).
	Type OperationType
}

// Handler enforces rate limits for incoming RPCs.
type Handler struct {
	cfg      *atomic.Pointer[HandlerConfig]
	delegate HandlerDelegate

	limiter multilimiter.RateLimiter
}

type HandlerConfig struct {
	multilimiter.Config

	// GlobalMode configures the action that will be taken when a global rate-limit
	// has been exhausted.
	//
	// Note: in the future there'll be a separate Mode for IP-based limits.
	GlobalMode Mode

	// GlobalWriteConfig configures the global rate limiter for write operations.
	GlobalWriteConfig multilimiter.LimiterConfig

	// GlobalReadConfig configures the global rate limiter for read operations.
	GlobalReadConfig multilimiter.LimiterConfig
}

type HandlerDelegate interface {
	// IsLeader is used to determine whether the operation is being performed
	// against the cluster leader, such that if it can _only_ be performed by
	// the leader (e.g. write operations) we don't tell clients to retry against
	// a different server.
	IsLeader() bool
}

// NewHandler creates a new RPC rate limit handler.
func NewHandler(cfg HandlerConfig, delegate HandlerDelegate) *Handler {
	limiter := multilimiter.NewMultiLimiter(cfg.Config)
	limiter.UpdateConfig(cfg.GlobalWriteConfig, globalWrite.ConfigKey())
	limiter.UpdateConfig(cfg.GlobalReadConfig, globalRead.ConfigKey())

	h := &Handler{
		cfg:      new(atomic.Pointer[HandlerConfig]),
		delegate: delegate,
		limiter:  limiter,
	}
	h.cfg.Store(&cfg)

	return h
}

// Run the limiter cleanup routine until the given context is canceled.
//
// Note: this starts a goroutine.
func (h *Handler) Run(ctx context.Context) {
	h.limiter.Run(ctx)
}

// Allow returns an error if the given operation is now allowed to proceed
// because of an exhausted rate-limit.
func (h *Handler) Allow(op Operation) error {
	// TODO(NET-1383): actually implement the rate limiting logic.
	//
	// Example:
	//	if !h.limiter.Allow(globalWrite) {
	//	}
	return nil
}

// TODO(NET-1379): call this on `consul reload`.
func (h *Handler) UpdateConfig(cfg HandlerConfig) {
	h.cfg.Store(&cfg)
	h.limiter.UpdateConfig(cfg.GlobalWriteConfig, globalWrite.ConfigKey())
	h.limiter.UpdateConfig(cfg.GlobalReadConfig, globalRead.ConfigKey())
}

func (h *Handler) GetGlobalReadLimiterConfig() (*multilimiter.LimiterConfig, bool) {
	return h.limiter.GetConfig(globalRead.ConfigKey())
}

func (h *Handler) GetGlobalWriteLimiterConfig() (*multilimiter.LimiterConfig, bool) {
	return h.limiter.GetConfig(globalWrite.ConfigKey())
}

func (h *Handler) GetConfig() *HandlerConfig {
	return h.cfg.Load()
}

var (
	// globalWrite identifies the global rate limit applied to write operations.
	globalWrite = globalLimit("global.write")

	// globalRead identifies the global rate limit applied to read operations.
	globalRead = globalLimit("global.read")
)

type globalLimit string

// Key satisfies the multilimiter.LimitedEntity interface. It returns the key
// of the leaf node in which the limiter is stored.
func (gl globalLimit) Key() []byte {
	return multilimiter.Key(gl.ConfigKey(), []byte("limiter"))
}

// ConfigKey is the key of the multilimiter tree node in which the config lives.
//
// TODO: we have to do this beacause the multilimiter doesn't currently support
// setting config directly on a leaf node, which we were eventually going to do
// for per-identity/per-tenant limits. Maybe we should do that sooner?
func (gl globalLimit) ConfigKey() []byte { return []byte(gl) }
