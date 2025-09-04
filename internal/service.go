package internal

import (
	"context"
	"raft-impl/internal/raftpb"
	"strconv"
	"sync/atomic"
)

// RaftService 实现了 raftpb.RaftServiceServer 接口。
// 它包装了 RaftNode 并处理传入的 RPC 请求。
type RaftService struct {
	raftpb.UnimplementedRaftServiceServer
	node *RaftNode
}

// NewRaftService 创建一个新的 RaftService。
func NewRaftService(node *RaftNode) *RaftService {
	return &RaftService{node: node}
}

// RequestVote 处理来自候选人的 RequestVote RPC。
func (s *RaftService) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	// 使用原子操作读取当前任期
	currentTerm := atomic.LoadUint64(&s.node.currentTerm)

	resp := &raftpb.RequestVoteResponse{
		Term:        int64(currentTerm),
		VoteGranted: false,
	}

	// 如果请求的任期小于当前任期，则拒绝投票
	if uint64(req.Term) < currentTerm {
		return resp, nil
	}

	// 如果请求的任期大于当前任期，则转换为跟随者
	if uint64(req.Term) > currentTerm {
		s.node.mu.Unlock() // 释放锁以便 becomeFollower 使用原子操作
		s.node.becomeFollower(uint64(req.Term))
		s.node.mu.Lock()                                     // 重新获取锁
		currentTerm = atomic.LoadUint64(&s.node.currentTerm) // 更新当前任期
	}

	candidateID, err := strconv.ParseUint(req.CandidateId, 10, 64)
	if err != nil {
		// Handle error, maybe log it
		return resp, nil
	}

	// 检查是否可以投票
	// 1. votedFor 为 0 (nil) 或与 candidateId 相同
	// 2. 候选人的日志至少与接收者的日志一样新
	canVote := (s.node.votedFor == 0 || s.node.votedFor == candidateID) &&
		isLogUpToDate(uint64(req.LastLogIndex), uint64(req.LastLogTerm), s.node)

	if canVote {
		s.node.votedFor = candidateID
		resp.VoteGranted = true
		// 投票后重置选举计时器
		s.node.electionTimer.Reset(randomElectionTimeout())
	}

	resp.Term = int64(currentTerm)
	return resp, nil
}

// AppendEntries 处理来自领导者的 AppendEntries RPC。
func (s *RaftService) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	// 使用原子操作读取当前任期
	currentTerm := atomic.LoadUint64(&s.node.currentTerm)

	resp := &raftpb.AppendEntriesResponse{
		Term:    int64(currentTerm),
		Success: false,
	}

	// 如果请求的任期小于当前任期，则拒绝
	if uint64(req.Term) < currentTerm {
		return resp, nil
	}

	// 检查当前节点的角色
	currentRole := atomic.LoadInt32(&s.node.role)

	// 如果收到来自新领导者的合法心跳，则成为跟随者
	if uint64(req.Term) > currentTerm {
		s.node.mu.Unlock() // 释放锁以便 becomeFollower 使用原子操作
		s.node.becomeFollower(uint64(req.Term))
		s.node.mu.Lock() // 重新获取锁
	} else if uint64(req.Term) == currentTerm && currentRole == RoleLeader {
		// 如果当前是领导者且收到相同任期的 AppendEntries，说明出现了网络分区后的双主情况
		// 根据 Raft 算法，应该退位变成跟随者
		s.node.mu.Unlock()
		s.node.becomeFollower(uint64(req.Term))
		s.node.mu.Lock()
	}

	// 确保转换为跟随者（可能是从 Candidate 变回 Follower，或从 Leader 退位）
	atomic.StoreInt32(&s.node.role, RoleFollower)
	s.node.electionTimer.Reset(randomElectionTimeout())

	// 日志一致性检查
	// 如果 prevLogIndex 处的日志条目的任期与 prevLogTerm 不匹配，则返回 false
	if uint64(req.PrevLogIndex) >= uint64(len(s.node.log)) || s.node.log[req.PrevLogIndex].Term != uint64(req.PrevLogTerm) {
		return resp, nil
	}

	// 附加任何新的日志条目
	newEntries := make([]*LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		newEntries[i] = &LogEntry{
			Term:    uint64(entry.Term),
			Command: entry.Command, // Assuming command is compatible
		}
	}
	s.node.log = append(s.node.log[:uint64(req.PrevLogIndex)+1], newEntries...)
	resp.Success = true

	// 如果领导者的 commitIndex 大于接收者的 commitIndex，则更新 commitIndex
	if uint64(req.LeaderCommit) > s.node.commitIndex {
		s.node.commitIndex = min(uint64(req.LeaderCommit), uint64(len(s.node.log)-1))
		// 可以在这里触发应用日志条目到状态机
	}

	return resp, nil
}

// isLogUpToDate 检查候选人的日志是否至少与当前节点的日志一样新。
// “新”的定义是：任期号更大，或者任期号相同但索引更大。
func isLogUpToDate(candidateLastLogIndex, candidateLastLogTerm uint64, rn *RaftNode) bool {
	lastLogIndex := uint64(len(rn.log) - 1)
	lastLogTerm := rn.log[lastLogIndex].Term

	if candidateLastLogTerm > lastLogTerm {
		return true
	}
	if candidateLastLogTerm == lastLogTerm && candidateLastLogIndex >= lastLogIndex {
		return true
	}
	return false
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
