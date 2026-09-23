package api

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"strings"
)

const (
	DefaultAPIAddr = "127.0.0.1:8080"
	DefaultOpsAddr = "127.0.0.1:9090"
)

// Config contains the small, immutable startup contract for the HTTP process.
// Config 保存 HTTP 进程的小型不可变启动契约。
type Config struct {
	APIAddr       string
	OpsAddr       string
	ServerVersion string
}

// DefaultConfig returns local-only listener defaults.
// DefaultConfig 返回仅监听本机的默认值。
func DefaultConfig() Config {
	return Config{
		APIAddr:       DefaultAPIAddr,
		OpsAddr:       DefaultOpsAddr,
		ServerVersion: "unknown",
	}
}

// ParseConfig parses only the documented command-line flags.
// ParseConfig 只解析文档化的命令行参数。
func ParseConfig(args []string, stdout, stderr io.Writer) (Config, bool, error) {
	cfg := DefaultConfig()
	fs := flag.NewFlagSet("newim-server", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintln(stdout, "Usage: newim-server [--api-addr host:port] [--ops-addr host:port]")
		fmt.Fprintln(stdout, "  --api-addr string  API listener address (default 127.0.0.1:8080)")
		fmt.Fprintln(stdout, "  --ops-addr string  Ops listener address (default 127.0.0.1:9090)")
	}
	fs.StringVar(&cfg.APIAddr, "api-addr", cfg.APIAddr, "API listener address")
	fs.StringVar(&cfg.OpsAddr, "ops-addr", cfg.OpsAddr, "Ops listener address")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.Usage()
			return Config{}, true, nil
		}
		return Config{}, false, fail(CodeInvalidArgument)
	}
	if fs.NArg() != 0 {
		return Config{}, false, fail(CodeInvalidArgument)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, false, err
	}
	return cfg, false, nil
}

// Validate rejects malformed and duplicate listener addresses at startup.
// Validate 在启动时拒绝畸形或重复的监听地址。
func (c Config) Validate() error {
	if err := validateAddr(c.APIAddr); err != nil {
		return fail(CodeInvalidConfig)
	}
	if err := validateAddr(c.OpsAddr); err != nil {
		return fail(CodeInvalidConfig)
	}
	if c.APIAddr == c.OpsAddr && !strings.HasSuffix(c.APIAddr, ":0") {
		return fail(CodeInvalidConfig)
	}
	if strings.TrimSpace(c.ServerVersion) == "" {
		return fail(CodeInvalidConfig)
	}
	return nil
}

func validateAddr(value string) error {
	if strings.TrimSpace(value) == "" {
		return errors.New("empty address")
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil || port == "" {
		return errors.New("invalid address")
	}
	if strings.ContainsAny(host, " \t\n\r") || strings.ContainsAny(port, " \t\n\r") {
		return errors.New("invalid address")
	}
	return nil
}
