package internal

import (
	"context"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"raft-impl/internal/raftpb"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	RoleFollower  = iota // 跟随者
	RoleCandidate        // 候选人
	RoleLeader           // 领导者
)

// Make 创建一个新的 Raft 节点。
func Make(id uint64, peers map[uint64]string, applyCh chan *ApplyMsg) *RaftNode {
	// 初始化 zap logger
	logger, _ := zap.NewProduction()

	rn := &RaftNode{
		id:             id,
		peers:          peers,
		role:           RoleFollower,
		currentTerm:    0,
		votedFor:       0,                                    // 0 表示未投票
		log:            []*LogEntry{{Term: 0, Command: nil}}, // 索引 0 是占位符，任期为0
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make(map[uint64]uint64),
		matchIndex:     make(map[uint64]uint64),
		applyCh:        applyCh,
		clients:        make(map[uint64]raftpb.RaftServiceClient),
		electionTimer:  time.NewTimer(randomElectionTimeout()),
		heartbeatTimer: time.NewTimer(100 * time.Millisecond),
		logger:         logger,
	}

	// 为每个对等节点创建 gRPC 客户端
	for peerId, addr := range peers {
		if peerId == id {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			rn.logger.Fatal("无法连接到对等节点",
				zap.Uint64("peerId", peerId),
				zap.Error(err))
		}
		client := raftpb.NewRaftServiceClient(conn)
		rn.clients[peerId] = client
	}

	// 为每个对等节点初始化 nextIndex 和 matchIndex
	for peerId := range peers {
		rn.nextIndex[peerId] = 1
		rn.matchIndex[peerId] = 0
	}

	// 在单独的 goroutine 中启动主循环
	go rn.Run()

	return rn
}

// Role 返回节点的当前角色。
func (rn *RaftNode) Role() int32 {
	return atomic.LoadInt32(&rn.role)
}

// CurrentTerm 返回节点的当前任期。
func (rn *RaftNode) CurrentTerm() uint64 {
	return atomic.LoadUint64(&rn.currentTerm)
}

// Run 是 Raft 节点的主循环。
func (rn *RaftNode) Run() {
	for {
		switch rn.Role() {
		case RoleFollower:
			rn.runFollower()
		case RoleCandidate:
			rn.runCandidate()
		case RoleLeader:
			rn.runLeader()
		}
	}
}

// runLeader 实现领导者的逻辑。
func (rn *RaftNode) runLeader() {
	// 发送初始心跳
	rn.sendHeartbeats()
	rn.heartbeatTimer.Reset(100 * time.Millisecond)

	for rn.Role() == RoleLeader {
		<-rn.heartbeatTimer.C
		rn.sendHeartbeats()
		rn.heartbeatTimer.Reset(100 * time.Millisecond)
		// 在实际实现中，您还需要检查响应
		// 如果发现更高的任期，可能会下台。
	}
}

// runFollower 实现跟随者的逻辑。
func (rn *RaftNode) runFollower() {
	rn.electionTimer.Reset(randomElectionTimeout())
	for rn.Role() == RoleFollower {
		<-rn.electionTimer.C
		rn.becomeCandidate()
		return // 退出跟随者循环
		// 在实际实现中，您还可以在此处处理传入的 RPC。
		// 例如 case rpc := <-rpcCh:
	}
}

// runCandidate 实现候选人的逻辑。
func (rn *RaftNode) runCandidate() {
	// 开始选举 - 使用原子操作增加任期
	atomic.AddUint64(&rn.currentTerm, 1)

	rn.mu.Lock()
	rn.votedFor = rn.id
	rn.electionTimer.Reset(randomElectionTimeout())
	// 获取当前任期和日志信息用于发送请求
	currentTerm := atomic.LoadUint64(&rn.currentTerm)
	lastLogIndex := uint64(len(rn.log) - 1)
	var lastLogTerm uint64
	if lastLogIndex < uint64(len(rn.log)) && rn.log[lastLogIndex] != nil {
		lastLogTerm = rn.log[lastLogIndex].Term
	} else {
		lastLogTerm = 0
	}
	rn.mu.Unlock()

	// 为自己投票 - 使用原子变量来避免竞争条件
	var votes int32 = 1
	voteCh := make(chan bool, len(rn.clients))

	// 向所有其他节点发送 RequestVote RPC
	for peerId, client := range rn.clients {
		go func(peerId uint64, client raftpb.RaftServiceClient) {
			req := &raftpb.RequestVoteRequest{
				Term:         int64(currentTerm),
				CandidateId:  strconv.FormatUint(rn.id, 10),
				LastLogIndex: int64(lastLogIndex),
				LastLogTerm:  int64(lastLogTerm),
			}

			resp, err := client.RequestVote(context.Background(), req)
			if err != nil {
				rn.logger.Error("向节点发送 RequestVote 失败",
					zap.Uint64("peerId", peerId),
					zap.Error(err))
				voteCh <- false
				return
			}

			// 使用原子操作检查当前任期
			if resp.Term > int64(atomic.LoadUint64(&rn.currentTerm)) {
				rn.becomeFollower(uint64(resp.Term))
				voteCh <- false
				return
			}

			voteCh <- resp.VoteGranted
		}(peerId, client)
	}

	// 等待选举结果或超时
	timeout := time.After(randomElectionTimeout())
	voteCount := 0
	for voteCount < len(rn.clients) {
		select {
		case granted := <-voteCh:
			if granted {
				atomic.AddInt32(&votes, 1)
				// 如果获得大多数选票，成为领导者
				if int(atomic.LoadInt32(&votes)) > len(rn.peers)/2 {
					rn.becomeLeader()
					return
				}
			}
			voteCount++
		case <-timeout:
			// 选举超时，重新开始选举
			return
		}
	}
}

// becomeFollower 将节点转换为跟随者。
func (rn *RaftNode) becomeFollower(term uint64) {
	atomic.StoreInt32(&rn.role, RoleFollower)
	atomic.StoreUint64(&rn.currentTerm, term)
	rn.mu.Lock()
	rn.votedFor = 0 // 重置 votedFor
	rn.mu.Unlock()
}

// becomeCandidate 将节点转换为候选人。
func (rn *RaftNode) becomeCandidate() {
	// 使用 CAS 来确保只有 Follower 可以成为 Candidate
	if !atomic.CompareAndSwapInt32(&rn.role, RoleFollower, RoleCandidate) {
		return // 如果当前不是 Follower，则不做任何事情
	}

	rn.mu.Lock()
	rn.votedFor = rn.id
	rn.electionTimer.Reset(randomElectionTimeout())
	rn.mu.Unlock()
}

// becomeLeader 将节点转换为领导者。
func (rn *RaftNode) becomeLeader() {
	// 使用 CAS 来确保只有 Candidate 可以成为 Leader
	if !atomic.CompareAndSwapInt32(&rn.role, RoleCandidate, RoleLeader) {
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	// 为所有对等节点初始化 nextIndex 和 matchIndex
	lastLogIndex := uint64(len(rn.log) - 1)
	for peerId := range rn.peers {
		rn.nextIndex[peerId] = lastLogIndex + 1
		rn.matchIndex[peerId] = 0
	}
}

// sendHeartbeats 向所有对等节点发送心跳（空的 AppendEntries RPC）。
func (rn *RaftNode) sendHeartbeats() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for peerId, client := range rn.clients {
		go func(peerId uint64, client raftpb.RaftServiceClient) {
			rn.mu.Lock()
			prevLogIndex := rn.nextIndex[peerId] - 1
			var prevLogTerm uint64
			if prevLogIndex < uint64(len(rn.log)) && rn.log[prevLogIndex] != nil {
				prevLogTerm = rn.log[prevLogIndex].Term
			} else {
				prevLogTerm = 0
			}
			// 使用原子操作读取当前任期
			currentTerm := atomic.LoadUint64(&rn.currentTerm)
			req := &raftpb.AppendEntriesRequest{
				Term:         int64(currentTerm),
				LeaderId:     strconv.FormatUint(rn.id, 10),
				PrevLogIndex: int64(prevLogIndex),
				PrevLogTerm:  int64(prevLogTerm),
				Entries:      []*raftpb.LogEntry{}, // 心跳是空的
				LeaderCommit: int64(rn.commitIndex),
			}
			rn.mu.Unlock()

			resp, err := client.AppendEntries(context.Background(), req)
			if err != nil {
				rn.logger.Error("向节点发送心跳失败",
					zap.Uint64("peerId", peerId),
					zap.Error(err))
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()
			// 使用原子操作检查当前任期
			if resp.Term > int64(atomic.LoadUint64(&rn.currentTerm)) {
				rn.mu.Unlock() // 释放锁以便 becomeFollower 使用原子操作
				rn.becomeFollower(uint64(resp.Term))
				rn.mu.Lock() // 重新获取锁以保证 defer 正常工作
				return
			}
			// 在实际实现中，如果心跳失败，您需要更新 nextIndex 并重试
		}(peerId, client)
	}
}

// Start 接收客户端的命令。只有领导者可以处理。
func (rn *RaftNode) Start(command interface{}) (int, int, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// 使用原子操作检查角色
	if atomic.LoadInt32(&rn.role) != RoleLeader {
		return -1, -1, false
	}

	// 使用原子操作读取当前任期
	currentTerm := atomic.LoadUint64(&rn.currentTerm)
	entry := &LogEntry{
		Term:    currentTerm,
		Command: command,
	}
	rn.log = append(rn.log, entry)
	index := len(rn.log) - 1

	// 立即向所有对等节点发送日志条目
	rn.replicateLog()

	return index, int(currentTerm), true
}

// replicateLog 向所有对等节点复制日志条目。
func (rn *RaftNode) replicateLog() {
	for peerId := range rn.peers {
		if peerId == rn.id {
			continue
		}
		go rn.sendAppendEntriesToPeer(peerId)
	}
}

// sendAppendEntriesToPeer 向单个对等节点发送 AppendEntries RPC。
func (rn *RaftNode) sendAppendEntriesToPeer(peerId uint64) {
	rn.mu.Lock()
	// 使用原子操作检查角色
	if atomic.LoadInt32(&rn.role) != RoleLeader {
		rn.mu.Unlock()
		return
	}

	nextIdx := rn.nextIndex[peerId]
	if nextIdx == 0 {
		nextIdx = 1 // defensive
	}
	prevLogIndex := nextIdx - 1
	var prevLogTerm uint64
	if prevLogIndex < uint64(len(rn.log)) && rn.log[prevLogIndex] != nil {
		prevLogTerm = rn.log[prevLogIndex].Term
	} else {
		prevLogTerm = 0
	}
	entries := rn.log[nextIdx:]

	// 使用原子操作读取当前任期
	currentTerm := atomic.LoadUint64(&rn.currentTerm)
	req := &raftpb.AppendEntriesRequest{
		Term:         int64(currentTerm),
		LeaderId:     strconv.FormatUint(rn.id, 10),
		PrevLogIndex: int64(prevLogIndex),
		PrevLogTerm:  int64(prevLogTerm),
		Entries:      toProtoEntries(entries),
		LeaderCommit: int64(rn.commitIndex),
	}
	client := rn.clients[peerId]
	rn.mu.Unlock()

	resp, err := client.AppendEntries(context.Background(), req)
	if err != nil {
		rn.logger.Error("向节点发送 AppendEntries 失败",
			zap.Uint64("peerId", peerId),
			zap.Error(err))
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	// 使用原子操作检查当前任期
	if resp.Term > int64(atomic.LoadUint64(&rn.currentTerm)) {
		rn.mu.Unlock() // 释放锁以便 becomeFollower 使用原子操作
		rn.becomeFollower(uint64(resp.Term))
		rn.mu.Lock() // 重新获取锁以保证 defer 正常工作
		return
	}

	if resp.Success {
		rn.nextIndex[peerId] = nextIdx + uint64(len(entries))
		rn.matchIndex[peerId] = rn.nextIndex[peerId] - 1
		rn.updateCommitIndex()
	} else {
		// 如果失败，减少 nextIndex 并重试
		if rn.nextIndex[peerId] > 1 {
			rn.nextIndex[peerId]--
		}
	}
}

// updateCommitIndex 更新领导者的 commitIndex。
func (rn *RaftNode) updateCommitIndex() {
	// 使用原子操作读取当前任期
	currentTerm := atomic.LoadUint64(&rn.currentTerm)

	// 寻找一个被大多数节点复制的日志条目索引 N
	// 要求 N > commitIndex 且 log[N].Term == currentTerm
	for N := len(rn.log) - 1; N > int(rn.commitIndex); N-- {
		if N >= len(rn.log) || rn.log[N] == nil || rn.log[N].Term != currentTerm {
			continue
		}
		count := 1 // 自己已经复制
		for peerId := range rn.peers {
			if peerId != rn.id && rn.matchIndex[peerId] >= uint64(N) {
				count++
			}
		}
		if count > len(rn.peers)/2 {
			rn.commitIndex = uint64(N)
			rn.applyCommittedLogs()
			break
		}
	}
}

// applyCommittedLogs 将已提交的日志应用到状态机。
func (rn *RaftNode) applyCommittedLogs() {
	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entry := rn.log[rn.lastApplied]
		applyMsg := &ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: rn.lastApplied,
		}
		rn.applyCh <- applyMsg
	}
}

// toProtoEntries 将内部 LogEntry 转换为 protobuf 的 LogEntry。
func toProtoEntries(entries []*LogEntry) []*raftpb.LogEntry {
	protoEntries := make([]*raftpb.LogEntry, len(entries))
	for i, entry := range entries {
		// 注意：Command 需要可序列化。这里我们假设它是 []byte。
		// 在实际应用中，您可能需要更复杂的序列化/反序列化逻辑。
		var cmdBytes []byte
		if cmd, ok := entry.Command.([]byte); ok {
			cmdBytes = cmd
		} else {
			// 如果不是 []byte，尝试进行某种形式的转换或记录错误。
			// 为了简单起见，我们这里留空。
		}

		protoEntries[i] = &raftpb.LogEntry{
			Term:    int64(entry.Term),
			Command: cmdBytes,
		}
	}
	return protoEntries
}

// randomElectionTimeout 生成一个 150ms 到 300ms 之间的随机选举超时。
func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// Close 关闭 Raft 节点并清理资源。
func (rn *RaftNode) Close() {
	if rn.logger != nil {
		rn.logger.Sync()
	}
}
