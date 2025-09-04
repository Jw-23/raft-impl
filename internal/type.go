package internal

import (
	"sync"
	"time"

	"raft-impl/internal/raftpb"

	"go.uber.org/zap"
)

// RaftNode 表示 Raft 集群中的单个节点。
type RaftNode struct {
	mu sync.Mutex // 用于保护对节点状态并发访问的互斥锁

	id    uint64            // 当前节点的唯一标识符
	peers map[uint64]string // 集群中其他节点的 ID 到其网络地址的映射

	role        int32       // 当前节点的角色（Follower, Candidate, 或 Leader），使用原子操作
	currentTerm uint64      // 服务器已知的最新任期，使用原子操作
	votedFor    uint64      // 在当前任期内投票给的候选人 ID（0 表示未投票）
	log         []*LogEntry // 日志条目；每个条目包含状态机的命令和领导者接收到该条目时的任期

	commitIndex uint64 // 已知被提交的最高日志条目的索引，使用原子操作
	lastApplied uint64 // 应用到状态机的最高日志条目的索引，使用原子操作

	// 以下是领导者节点上的易失性状态
	nextIndex  map[uint64]uint64 // 对于每个服务器，下一个要发送给该服务器的日志条目的索引
	matchIndex map[uint64]uint64 // 对于每个服务器，已知在该服务器上复制的最高日志条目的索引

	// 定时器和通道
	electionTimer  *time.Timer    // 选举定时器
	heartbeatTimer *time.Timer    // 心跳定时器
	applyCh        chan *ApplyMsg // 用于将提交的日志条目发送到状态机的通道

	logger *zap.Logger // 日志记录器

	// gRPC 客户端连接
	clients map[uint64]raftpb.RaftServiceClient // 到其他节点的 gRPC 客户端连接
}

// LogEntry 表示 Raft 日志中的单个条目。
type LogEntry struct {
	Term    uint64      // 该日志条目所属的任期
	Command interface{} // 状态机要执行的命令
}

// ApplyMsg 表示一个将要应用到状态机的命令。
type ApplyMsg struct {
	CommandValid bool        // 命令是否有效
	Command      interface{} // 要执行的命令
	CommandIndex uint64      // 命令在日志中的索引
}
