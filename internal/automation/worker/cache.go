package worker

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/automation/microvm"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// The remote cache, as a run reaches it (docs/hosted.md, "Hosted automation, as built"): a gRPC
// pass-through on a socket of the run's own, forwarding the cache's methods to ok automation as
// the worker, naming the run on each call — so what the run reaches is the run's, whatever it
// sends.

// runHeader names the run a cache call is for (automation's service.RunHeader).
const runHeader = "x-ok-run"

// cacheServices are the services the pass-through forwards: Bazel's remote cache's.
var cacheServices = []string{
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/",
	"/build.bazel.remote.execution.v2.ActionCache/",
	"/build.bazel.remote.execution.v2.Capabilities/",
	"/google.bytestream.ByteStream/",
}

// rawCodec passes messages through as they came: the pass-through reads none of them.
type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.(*[]byte)
	if !ok {
		return nil, errors.New("the cache's pass-through carries raw messages")
	}
	return *b, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return errors.New("the cache's pass-through carries raw messages")
	}
	*b = append((*b)[:0], data...)
	return nil
}

func (rawCodec) Name() string { return "proto" }

// cacheProxy is a run's pass-through.
type cacheProxy struct {
	srv  *grpc.Server
	path string
}

// startCacheProxy serves a run's pass-through on a socket at path, forwarding to automation.
func startCacheProxy(path string, automation grpc.ClientConnInterface, run int64) (*cacheProxy, error) {
	_ = os.Remove(path)
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	id := strconv.FormatInt(run, 10)
	srv := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}), grpc.UnknownServiceHandler(func(_ any, in grpc.ServerStream) error {
		method, ok := grpc.MethodFromServerStream(in)
		if !ok || !forwarded(method) {
			return status.Errorf(codes.Unimplemented, "the cache's pass-through serves the remote cache alone, not %s", method)
		}
		ctx, cancel := context.WithCancel(in.Context())
		defer cancel()
		// What the run sent beside the call is not passed on: the run is named here, alone.
		ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(runHeader, id))
		out, err := automation.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, method, grpc.ForceCodec(rawCodec{}))
		if err != nil {
			return err
		}
		sent := make(chan error, 1)
		go func() {
			for {
				var m []byte
				if err := in.RecvMsg(&m); err != nil {
					if errors.Is(err, io.EOF) {
						err = out.CloseSend()
					}
					sent <- err
					return
				}
				if err := out.SendMsg(&m); err != nil {
					sent <- err
					return
				}
			}
		}()
		header, err := out.Header()
		if err != nil {
			return err
		}
		if err := in.SendHeader(header); err != nil {
			return err
		}
		for {
			var m []byte
			if err := out.RecvMsg(&m); err != nil {
				in.SetTrailer(out.Trailer())
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			if err := in.SendMsg(&m); err != nil {
				return err
			}
		}
	}))
	go srv.Serve(l)
	return &cacheProxy{srv: srv, path: path}, nil
}

func forwarded(method string) bool {
	for _, s := range cacheServices {
		if strings.HasPrefix(method, s) {
			return true
		}
	}
	return false
}

// Stop ends the pass-through, and the run's calls with it.
func (p *cacheProxy) Stop() {
	p.srv.Stop()
	_ = os.Remove(p.path)
	if dir := filepath.Dir(p.path); strings.HasPrefix(filepath.Base(dir), "okc") {
		_ = os.Remove(dir)
	}
}

// cacheFlags are Bazel's flags for the remote cache at a socket.
func cacheFlags(socket string) []string {
	return []string{"--remote_cache=unix:" + socket, "--remote_upload_local_results", "--remote_timeout=60s"}
}

// startCache starts a run's pass-through: in a microVM, on the host alone, which the guest's
// microvm.CacheSocket reaches; else in the run's slot's scratch, which no other slot's runs see.
func (cfg Config) startCache(run *automationpb.Run) (*cacheProxy, error) {
	dir := cfg.scratch
	if cfg.MicroVM != nil {
		var err error
		if dir, err = os.MkdirTemp("", "okc"); err != nil {
			return nil, err
		}
	}
	return startCacheProxy(filepath.Join(dir, "cache-"+strconv.FormatInt(run.GetId(), 10)+".sock"), cfg.Automation, run.GetId())
}

// runCacheFlags are a run's Bazel's flags for its remote cache, if it has one.
func (cfg Config) runCacheFlags() []string {
	switch {
	case cfg.cacheSocket == "":
		return nil
	case cfg.MicroVM != nil:
		return cacheFlags(microvm.CacheSocket)
	}
	return cacheFlags(cfg.cacheSocket)
}
