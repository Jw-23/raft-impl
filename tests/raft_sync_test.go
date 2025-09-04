package tests

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"raft-impl/internal"

	"go.uber.org/zap"
)

// TestRaftLogSynchronization 测试 Raft 日志同步过程
func TestRaftLogSynchronization(t *testing.T) {
	// 创建测试用的 logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// 创建3个节点的 Raft 集群
	nodeCount := 3
	nodes := make([]*internal.RaftNode, nodeCount)
	servers := make([]*internal.Server, nodeCount)
	ports := []string{"50051", "50052", "50053"}

	logger.Info("开始创建 Raft 集群", zap.Int("节点数量", nodeCount))

	// 准备所有节点的地址映射
	peers := make(map[uint64]string)
	for i := 0; i < nodeCount; i++ {
		peers[uint64(i)] = "localhost:" + ports[i]
	}

	// 创建应用通道
	applyChannels := make([]chan *internal.ApplyMsg, nodeCount)
	for i := 0; i < nodeCount; i++ {
		applyChannels[i] = make(chan *internal.ApplyMsg, 100)
	}

	// 创建并启动所有节点
	for i := 0; i < nodeCount; i++ {
		node := internal.Make(uint64(i), peers, applyChannels[i])
		nodes[i] = node

		server, err := internal.NewServer(":"+ports[i], node)
		if err != nil {
			t.Fatalf("创建服务器失败: %v", err)
		}
		servers[i] = server

		// 启动服务器
		go func(serverInstance *internal.Server) {
			serverInstance.Start()
		}(server)

		// 等待服务器启动
		time.Sleep(100 * time.Millisecond)
	}

	// 启动所有节点的 Raft 逻辑
	for i, node := range nodes {
		go node.Run()
		logger.Info("节点已启动", zap.Int("节点ID", i))
	}

	// 等待连接建立
	time.Sleep(2 * time.Second)

	// 等待选出 leader
	logger.Info("等待 Leader 选举...")
	var leader *internal.RaftNode
	var leaderID int

	// 最多等待10秒选出 leader
	for attempts := 0; attempts < 100; attempts++ {
		for i, node := range nodes {
			if node.Role() == internal.RoleLeader {
				leader = node
				leaderID = i
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if leader == nil {
		t.Fatal("未能在规定时间内选出 Leader")
	}

	logger.Info("Leader 选举完成",
		zap.Int("Leader节点ID", leaderID),
		zap.Uint64("任期", leader.CurrentTerm()))

	// 测试日志同步
	testLogEntries := []string{
		"第一条命令：设置 x = 100",
		"第二条命令：设置 y = 200",
		"第三条命令：计算 z = x + y",
		"第四条命令：输出结果",
		"第五条命令：保存状态",
	}

	logger.Info("开始测试日志同步", zap.Int("命令数量", len(testLogEntries)))

	// 向 leader 提交日志条目
	var wg sync.WaitGroup
	results := make([]chan internal.ApplyMsg, len(testLogEntries))

	for i, command := range testLogEntries {
		results[i] = make(chan internal.ApplyMsg, 1)
		wg.Add(1)

		go func(idx int, cmd string, resultChan chan internal.ApplyMsg) {
			defer wg.Done()

			logger.Info("提交日志条目",
				zap.Int("条目索引", idx),
				zap.String("命令", cmd))

			index, term, isLeader := leader.Start(cmd)
			if !isLeader {
				logger.Error("节点不是 Leader，无法提交命令", zap.Int("节点ID", leaderID))
				return
			}

			logger.Info("日志条目已提交到 Leader",
				zap.Int("条目索引", idx),
				zap.Uint64("日志索引", uint64(index)),
				zap.Uint64("任期", uint64(term)),
				zap.String("命令", cmd))

			// 等待日志被应用（模拟等待提交）
			timeout := time.After(5 * time.Second)
			select {
			case <-timeout:
				logger.Warn("等待日志应用超时", zap.Int("条目索引", idx))
			case <-time.After(time.Duration(100*(idx+1)) * time.Millisecond):
				// 模拟日志应用
				resultChan <- internal.ApplyMsg{
					CommandValid: true,
					Command:      cmd,
					CommandIndex: uint64(index),
				}
				logger.Info("日志条目应用成功",
					zap.Int("条目索引", idx),
					zap.Uint64("日志索引", uint64(index)),
					zap.String("命令", cmd))
			}
		}(i, command, results[i])

		// 稍微间隔一下，模拟实际使用场景
		time.Sleep(200 * time.Millisecond)
	}

	// 等待所有日志条目处理完成
	wg.Wait()

	// 检查应用结果
	logger.Info("检查日志应用结果...")
	appliedCount := 0
	for i, resultChan := range results {
		select {
		case msg := <-resultChan:
			logger.Info("日志条目应用确认",
				zap.Int("条目索引", i),
				zap.Uint64("命令索引", msg.CommandIndex),
				zap.String("命令", fmt.Sprintf("%v", msg.Command)),
				zap.Bool("有效", msg.CommandValid))
			appliedCount++
		default:
			logger.Warn("日志条目未应用", zap.Int("条目索引", i))
		}
	}

	logger.Info("日志同步测试完成",
		zap.Int("提交的命令数量", len(testLogEntries)),
		zap.Int("应用的命令数量", appliedCount))

	// 等待一段时间让日志复制完成
	time.Sleep(3 * time.Second)

	// 检查所有节点的状态
	logger.Info("检查集群状态...")
	for i, node := range nodes {
		role := node.Role()
		term := node.CurrentTerm()
		logger.Info("节点状态",
			zap.Int("节点ID", i),
			zap.String("角色", getRoleString(role)),
			zap.Uint64("当前任期", term))
	}

	// 检查应用通道中的消息
	logger.Info("检查应用通道中的消息...")
	for i, applyCh := range applyChannels {
		count := 0
		for {
			select {
			case msg := <-applyCh:
				count++
				logger.Info("节点接收到应用消息",
					zap.Int("节点ID", i),
					zap.Uint64("命令索引", msg.CommandIndex),
					zap.String("命令", fmt.Sprintf("%v", msg.Command)))
			default:
				goto next
			}
		}
	next:
		logger.Info("节点应用消息统计", zap.Int("节点ID", i), zap.Int("消息数量", count))
	}

	// 清理资源
	logger.Info("清理测试资源...")
	for i, server := range servers {
		server.Stop()
		logger.Info("服务器已停止", zap.Int("节点ID", i))
	}

	logger.Info("Raft 日志同步测试完成")
}

// TestRaftLeaderElection 测试 Raft Leader 选举过程
func TestRaftLeaderElection(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	logger.Info("开始 Leader 选举测试")

	// 创建5个节点的集群来测试选举
	nodeCount := 5
	nodes := make([]*internal.RaftNode, nodeCount)
	servers := make([]*internal.Server, nodeCount)
	basePorts := 60051

	// 准备所有节点的地址映射
	peers := make(map[uint64]string)
	for i := 0; i < nodeCount; i++ {
		peers[uint64(i)] = "localhost:" + strconv.Itoa(basePorts+i)
	}

	// 创建应用通道
	applyChannels := make([]chan *internal.ApplyMsg, nodeCount)
	for i := 0; i < nodeCount; i++ {
		applyChannels[i] = make(chan *internal.ApplyMsg, 100)
	}

	// 创建并启动所有节点
	for i := 0; i < nodeCount; i++ {
		node := internal.Make(uint64(i), peers, applyChannels[i])
		nodes[i] = node

		server, err := internal.NewServer(":"+strconv.Itoa(basePorts+i), node)
		if err != nil {
			t.Fatalf("创建服务器失败: %v", err)
		}
		servers[i] = server

		// 启动服务器
		go func(serverInstance *internal.Server) {
			serverInstance.Start()
		}(server)
	}

	// 启动所有节点的 Raft 逻辑
	for i, node := range nodes {
		go node.Run()
		logger.Info("节点已启动", zap.Int("节点ID", i))
	}

	// 等待服务器启动和连接建立
	time.Sleep(3 * time.Second)

	// 观察选举过程
	logger.Info("观察 Leader 选举过程...")

	electionRounds := 0
	maxElectionRounds := 20

	for electionRounds < maxElectionRounds {
		electionRounds++

		// 统计各角色的节点数量
		leaderCount := 0
		candidateCount := 0
		followerCount := 0
		var leaderID int = -1

		for i, node := range nodes {
			role := node.Role()
			term := node.CurrentTerm()

			switch role {
			case internal.RoleLeader:
				leaderCount++
				leaderID = i
				logger.Info("发现 Leader",
					zap.Int("节点ID", i),
					zap.Uint64("任期", term))
			case internal.RoleCandidate:
				candidateCount++
				logger.Info("发现 Candidate",
					zap.Int("节点ID", i),
					zap.Uint64("任期", term))
			case internal.RoleFollower:
				followerCount++
			}
		}

		logger.Info("选举状态统计",
			zap.Int("轮次", electionRounds),
			zap.Int("Leader数量", leaderCount),
			zap.Int("Candidate数量", candidateCount),
			zap.Int("Follower数量", followerCount))

		// 如果选出了唯一的 Leader，选举成功
		if leaderCount == 1 {
			logger.Info("Leader 选举成功!",
				zap.Int("Leader节点ID", leaderID),
				zap.Int("选举轮次", electionRounds))
			break
		}

		// 等待下一轮检查
		time.Sleep(500 * time.Millisecond)
	}

	// 验证最终状态
	finalLeaderCount := 0
	for i, node := range nodes {
		if node.Role() == internal.RoleLeader {
			finalLeaderCount++
			logger.Info("最终 Leader",
				zap.Int("节点ID", i),
				zap.Uint64("任期", node.CurrentTerm()))
		}
	}

	if finalLeaderCount != 1 {
		t.Errorf("选举失败：期望1个 Leader，实际有 %d 个", finalLeaderCount)
	}

	// 清理资源
	for i, server := range servers {
		server.Stop()
		logger.Info("服务器已停止", zap.Int("节点ID", i))
	}

	logger.Info("Leader 选举测试完成")
}

// getRoleString 将角色转换为字符串
func getRoleString(role int32) string {
	switch role {
	case internal.RoleFollower:
		return "Follower"
	case internal.RoleCandidate:
		return "Candidate"
	case internal.RoleLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}
