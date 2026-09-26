package redisx

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// The oldest Redis this connector runs against. `XAUTOCLAIM`, which the transport reclaims
// commands with, arrived in 6.2, so on an older server the reclaim fails on every pass
// while the process reports itself healthy. It is what README.md documents and what CI
// runs a pass against.
const (
	minMajor = 6
	minMinor = 2
)

// MinServerVersion is the floor in the form an operator reads it.
var MinServerVersion = fmt.Sprintf("%d.%d", minMajor, minMinor)

// RequireServerVersion refuses a server older than MinServerVersion, or one that will not
// say which version it is.
//
// The version comes from `HELLO`, which every server at or above the floor answers and
// which carries it as a field of its own, rather than from `INFO`, whose text a managed
// Redis is free to trim. A server that does not know `HELLO` is older than 6.0 and so
// below the floor on that alone. One that answers without a version it can be held to is
// refused as well: starting on a guess is how a command the server rejects turns into a
// loop that fails quietly on every pass, which is what this exists to rule out.
func (c *Client) RequireServerVersion(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	version, found, err := c.ServerVersion(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("redisx: ask the server its version: %w", err)
		}
		return fmt.Errorf("redisx: the server did not accept HELLO (%w), so it is older than Redis 6.0 "+
			"or does not say which version it runs; this connector needs Redis %s or newer", err, MinServerVersion)
	}
	return checkServerVersion(version, found)
}

// ServerVersion is the version the server reports in its answer to `HELLO`, and whether
// the answer carried one.
func (c *Client) ServerVersion(ctx context.Context) (version string, found bool, err error) {
	// The protocol the connection already speaks, so asking does not switch it. go-redis
	// has already turned an unset one into 3 by the time a client exists.
	reply, err := c.Do(ctx, "HELLO", c.Options().Protocol).Result()
	if err != nil {
		return "", false, err
	}
	version, found = helloVersion(reply)
	return version, found, nil
}

// helloVersion reads the version field out of a HELLO reply, which is a map under RESP3
// and a flat list of names and values under RESP2.
func helloVersion(reply any) (version string, found bool) {
	switch fields := reply.(type) {
	case map[any]any:
		value, ok := fields["version"]
		if !ok {
			return "", false
		}
		version, ok := value.(string)
		return version, ok
	case []any:
		for i := 0; i+1 < len(fields); i += 2 {
			if name, ok := fields[i].(string); ok && name == "version" {
				version, ok := fields[i+1].(string)
				return version, ok
			}
		}
	}
	return "", false
}

// checkServerVersion holds a reported version to the floor.
func checkServerVersion(version string, found bool) error {
	if !found {
		return fmt.Errorf("redisx: the server answered HELLO without a version; "+
			"this connector needs Redis %s or newer and will not start on a server it cannot hold to that", MinServerVersion)
	}
	major, minor, ok := parseVersion(version)
	if !ok {
		return fmt.Errorf("redisx: the server reports version %q, which is not a version this connector can read; "+
			"it needs Redis %s or newer", version, MinServerVersion)
	}
	if major < minMajor || major == minMajor && minor < minMinor {
		return fmt.Errorf("redisx: the server runs Redis %s, and this connector needs %s or newer (XAUTOCLAIM)",
			version, MinServerVersion)
	}
	return nil
}

// parseVersion reads the major and minor out of "6.2.24". Anything after the minor is
// ignored, so a patch or a build suffix does not decide the answer.
func parseVersion(version string) (major, minor int, ok bool) {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return 0, 0, false
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, false
	}
	minor, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, false
	}
	return major, minor, true
}
