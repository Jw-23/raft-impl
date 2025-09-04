package internal

import (
	"net"

	"raft-impl/internal/raftpb"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Server 包装了 gRPC 服务器和 Raft 服务。
type Server struct {
	grpcServer *grpc.Server
	raftNode   *RaftNode
	listener   net.Listener
	logger     *zap.Logger
}

// NewServer 创建一个新的服务器实例。
func NewServer(port string, raftNode *RaftNode) (*Server, error) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		return nil, err
	}

	// 初始化 zap logger
	logger, _ := zap.NewProduction()

	grpcServer := grpc.NewServer()
	raftService := NewRaftService(raftNode)
	raftpb.RegisterRaftServiceServer(grpcServer, raftService)

	return &Server{
		grpcServer: grpcServer,
		raftNode:   raftNode,
		listener:   lis,
		logger:     logger,
	}, nil
}

// Start 启动 gRPC 服务器。
func (s *Server) Start() error {
	s.logger.Info("gRPC server listening", zap.String("address", s.listener.Addr().String()))
	if err := s.grpcServer.Serve(s.listener); err != nil {
		s.logger.Fatal("failed to serve", zap.Error(err))
		return err
	}
	return nil
}

// Stop 停止 gRPC 服务器。
func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
	if s.logger != nil {
		s.logger.Sync()
	}
}
