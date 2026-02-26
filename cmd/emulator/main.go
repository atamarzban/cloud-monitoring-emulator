package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/admin"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/promql"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/server"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/store"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/token"
)

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Create store.
	s := store.NewMemoryStore()

	// Create gRPC server (no TLS, no auth — it's an emulator).
	grpcServer := grpc.NewServer()
	metricSvc := server.NewMetricServiceServer(s)
	alertSvc := server.NewAlertPolicyServiceServer(s)
	monitoringpb.RegisterMetricServiceServer(grpcServer, metricSvc)
	monitoringpb.RegisterAlertPolicyServiceServer(grpcServer, alertSvc)
	reflection.Register(grpcServer)

	// Create grpc-gateway mux (in-process registration — no loopback dial).
	gwMux := runtime.NewServeMux()
	if err := monitoringpb.RegisterMetricServiceHandlerServer(context.Background(), gwMux, metricSvc); err != nil {
		logger.Error("failed to register grpc-gateway handler", "error", err)
		os.Exit(1)
	}
	if err := monitoringpb.RegisterAlertPolicyServiceHandlerServer(context.Background(), gwMux, alertSvc); err != nil {
		logger.Error("failed to register alert policy grpc-gateway handler", "error", err)
		os.Exit(1)
	}

	// Create HTTP mux for REST + admin + Prometheus API.
	httpMux := http.NewServeMux()
	httpMux.Handle("/v3/", gwMux)
	httpMux.Handle("/v1/", promql.NewHandler(s))
	httpMux.Handle("/admin/", admin.NewHandler(s))
	httpMux.Handle("/token", token.NewHandler())

	// Create listener.
	addr := fmt.Sprintf(":%d", *port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("failed to listen", "addr", addr, "error", err)
		os.Exit(1)
	}

	// Create cmux: multiplex gRPC and HTTP on the same port.
	m := cmux.New(lis)
	grpcLis := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpLis := m.Match(cmux.Any())

	// Start servers.
	go func() {
		if err := grpcServer.Serve(grpcLis); err != nil {
			logger.Error("gRPC server error", "error", err)
		}
	}()
	go func() {
		if err := http.Serve(httpLis, httpMux); err != nil {
			logger.Error("HTTP server error", "error", err)
		}
	}()

	logger.Info("cloud monitoring emulator started",
		"port", *port,
		"grpc", fmt.Sprintf("localhost:%d", *port),
		"rest", fmt.Sprintf("http://localhost:%d/v3/", *port),
		"promql", fmt.Sprintf("http://localhost:%d/v1/projects/{project}/location/global/prometheus/api/v1/", *port),
		"admin", fmt.Sprintf("http://localhost:%d/admin/", *port),
		"token", fmt.Sprintf("http://localhost:%d/token", *port),
	)

	// Graceful shutdown on SIGINT/SIGTERM.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		logger.Info("shutting down", "signal", sig)
		grpcServer.GracefulStop()
		lis.Close()
	}()

	if err := m.Serve(); err != nil {
		// cmux returns error when listener is closed — that's expected on shutdown.
		if !isClosedErr(err) {
			logger.Error("cmux serve error", "error", err)
			os.Exit(1)
		}
	}
}

func isClosedErr(err error) bool {
	return err != nil && (err.Error() == "mux: server closed" ||
		err.Error() == "mux: listener closed" ||
		isNetClosedErr(err))
}

func isNetClosedErr(err error) bool {
	if opErr, ok := err.(*net.OpError); ok {
		return opErr.Err.Error() == "use of closed network connection"
	}
	return false
}
