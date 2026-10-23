// Package microvm runs a worker's runs each in a microVM of its own (docs/hosted.md, "Hosted
// automation, as built"): Cloud Hypervisor on KVM, the guest's root and every directory it sees
// shared from the host through virtio-fs, and no network device. The guest's init is ok itself
// (ok worker guest); commands reach it over vsock (ok worker vm-exec), and its only way out is the
// host's egress proxy, over vsock too.
package microvm

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// The guest's vsock ports. The guest listens on ExecPort; the host on the others, which the guest
// connects to as CID 2.
const (
	// ConfigPort is where the guest reads its GuestConfig as it boots.
	ConfigPort = 1024
	// ExecPort is where the guest runs commands.
	ExecPort = 1025
	// EgressPort is the host's egress proxy, an HTTP CONNECT proxy the guest forwards
	// 127.0.0.1:ProxyPort to.
	EgressPort = 1080
	// CachePort is the remote cache, which the guest forwards CacheSocket to.
	CachePort = 1081
)

// ProxyPort is the guest's loopback port of the host's egress proxy: HTTPS_PROXY's.
const ProxyPort = 3128

// CacheSocket is the guest's Unix socket of the remote cache: Bazel's --remote_proxy.
const CacheSocket = "/run/ok/cache.sock"

// Mount is a host directory the guest sees at the same path, through a virtio-fs tag.
type Mount struct {
	Tag      string `json:"tag"`
	Path     string `json:"path"`
	ReadOnly bool   `json:"read_only"`
}

// GuestConfig is what the guest is told as it boots.
type GuestConfig struct {
	Mounts []Mount `json:"mounts"`
	// UID and GID are who commands run as: the worker's, who owns what the shares hold.
	UID int `json:"uid"`
	GID int `json:"gid"`
	// Egress forwards the guest's 127.0.0.1:ProxyPort to the host's egress proxy; Cache its
	// CacheSocket to the remote cache.
	Egress bool `json:"egress"`
	Cache  bool `json:"cache"`
}

// Request is what ExecPort is asked, as a line of JSON: a command to run, or, with PowerOff, that
// the guest stop.
type Request struct {
	Argv     []string `json:"argv,omitempty"`
	Dir      string   `json:"dir,omitempty"`
	Env      []string `json:"env,omitempty"`
	PowerOff bool     `json:"power_off,omitempty"`
}

// What a client sends after its Request: the command's process group signaled.
const (
	Terminate byte = 'T'
	Kill      byte = 'K'
)

// Frames ExecPort answers with: the command's output, then its end.
const (
	FrameStdout byte = 'o'
	FrameStderr byte = 'e'
	// FrameExit's payload is the exit status as a big-endian int32: a signal's death -signal.
	FrameExit byte = 'x'
	// FrameError's is why the command did not start, which ends the exchange.
	FrameError byte = 'r'
)

// maxFrame bounds a frame's payload.
const maxFrame = 1 << 20

// WriteFrame writes one frame.
func WriteFrame(w io.Writer, kind byte, payload []byte) error {
	if len(payload) > maxFrame {
		return fmt.Errorf("microvm: a frame of %d bytes", len(payload))
	}
	var head [5]byte
	head[0] = kind
	binary.BigEndian.PutUint32(head[1:], uint32(len(payload)))
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// ReadFrame reads one frame.
func ReadFrame(r io.Reader) (byte, []byte, error) {
	var head [5]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(head[1:])
	if n > maxFrame {
		return 0, nil, fmt.Errorf("microvm: a frame of %d bytes", n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return head[0], payload, nil
}

// ExitStatus is a FrameExit's payload's status.
func ExitStatus(payload []byte) (int, error) {
	if len(payload) != 4 {
		return 0, errors.New("microvm: an exit frame is four bytes")
	}
	return int(int32(binary.BigEndian.Uint32(payload))), nil
}

func exitPayload(status int) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(int32(status)))
	return b[:]
}

// readLine reads one line of JSON into v, a byte at a time so nothing after it is consumed.
func readLine(r io.Reader, v any) error {
	var line []byte
	var b [1]byte
	for {
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return err
		}
		if b[0] == '\n' {
			return json.Unmarshal(line, v)
		}
		if len(line) >= maxFrame {
			return errors.New("microvm: a request line too long")
		}
		line = append(line, b[0])
	}
}

func writeLine(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}
