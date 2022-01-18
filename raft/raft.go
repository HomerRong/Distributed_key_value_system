package raft

import (
	raftpb "Distributed_key_value_system/raftrpc"
	"context"
	"encoding/json"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

type NodeStates int

const (
	_ NodeStates = iota
	Leader
	Follower
	Candidate
)

// raft的server
type server struct {
	raftpb.RaftrpcServer
	r *Raft
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next int64
}

type Raft struct {
	//raft服务的地址
	NodeAddress string
	// raft集群中其他节点的地址
	PeerNodeAddress []string

	// raft中的日志，仅位于内存
	Log *RaftLog

	// 其他节点的log匹配信息
	Prs map[string]*Progress

	// 当前的term
	Terms int64

	// 心跳超时时间
	heartbeatTimeout int
	// 心跳逻辑时钟，只有leader才需要维护
	heartbeatElapsed int

	// 选举超时时间
	electionTimeout int
	// 选举逻辑时钟， 只有follower才需要维护
	electionElapsed int

	// leader的地址
	leaderAddress string

	// 当前的状态，leader, follower, candidate 中的一个
	State NodeStates

	// 选举选票的记录，超过一半才能当选
	voteRecord map[string]bool

	// 当前日志append记录，只有一半的节点返回成功，才能将日志commit
	appendRecord map[string]bool

	// 服务线程的关闭信号
	stopchan chan bool

	// 连接的本地leveldb数据库
	db *leveldb.DB
}

// Setting raft的设置
type Setting struct {
	NodeAddress      string
	HeartbeatTimeout int
	ElectionTimeout  int
	PeerNodeAddress  []string
}

// NewRaft raft初始化
func (r *Raft) NewRaft(raftSetting *Setting) {
	r.NodeAddress = raftSetting.NodeAddress
	r.PeerNodeAddress = raftSetting.PeerNodeAddress
	r.heartbeatTimeout = raftSetting.HeartbeatTimeout
	r.electionTimeout = raftSetting.ElectionTimeout
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.Terms = 0
	r.voteRecord = make(map[string]bool)
	r.appendRecord = make(map[string]bool)
	r.Prs = make(map[string]*Progress)
	r.Log = &RaftLog{}
	r.db = nil
	for i := 0; i < len(r.PeerNodeAddress); i++ {
		r.voteRecord[r.PeerNodeAddress[i]] = false
	}
	r.BecomeFollower(r.Terms)

	r.stopchan = make(chan bool)
	lis, err := net.Listen("tcp", r.NodeAddress)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	raftpb.RegisterRaftrpcServer(s, &server{r: r})
	log.Printf("raft server listening at %v", lis.Addr())

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	go func() {
		select {
		case stop := <-r.stopchan:
			if stop == true {
				//log.Println("stop")
				s.GracefulStop()
			}

		}
	}()

}

func (r *Raft) ConnectDB(db *leveldb.DB) {
	r.db = db
}

func (r *Raft) StopRaft() {
	r.stopchan <- true
}

// LogicalTimerChange 触发逻辑时钟加1
func (r *Raft) LogicalTimerChange() {
	if r.State == Leader {
		r.heartbeatElapsed += 1
	}
	if r.State == Follower {
		r.electionElapsed += 1
	}

	// 选举超时，发起新的选举
	if r.electionElapsed == r.electionTimeout {
		//log.Println("election timeout")
		r.electionElapsed = 0
		r.BecomeCandidate()
	}

	// leader每一个心跳周期发送heartbeat来维持权威
	if r.heartbeatElapsed == r.heartbeatTimeout {
		//log.Println("heartbeat timeout")
		r.sendHeartBeat()
		r.heartbeatElapsed = 0
	}
}

// HandleMessage 处理rpc请求
func (s *server) HandleMessage(ctx context.Context, message *raftpb.Message) (*raftpb.Response, error) {
	// 增加逻辑时钟的值
	s.r.LogicalTimerChange()

	// 收到心跳信息
	if message.GetMsgType() == raftpb.MessageType_MsgHeartBeat {
		s.r.leaderAddress = message.GetLeaderAddress()
		s.r.BecomeFollower(message.GetTerm())
		//log.Println("got heartBeat")
		s.r.electionElapsed = 0
		return &raftpb.Response{}, nil
	}

	// 收到选举信息
	if message.GetMsgType() == raftpb.MessageType_MagNewElection {
		//log.Println(message.GetTerm(), s.r.Log.entries[s.r.Log.LastIndex()].GetTerm())

		/*
			当前没有日志，或者最后一条日志的term小于leader的日志，或者term相同，日志的长度较短
			投票，否则拒绝投票
		*/
		if len(s.r.Log.entries) == 0 || s.r.Log.entries[s.r.Log.LastIndex()].GetTerm() < message.GetTerm() {
			return &raftpb.Response{MsgType: raftpb.MessageType_MagNewElection, Address: s.r.NodeAddress}, nil
		} else if s.r.Log.entries[s.r.Log.LastIndex()].GetTerm() == message.GetTerm() {
			if s.r.Log.LastIndex() <= message.GetIndex() {
				return &raftpb.Response{MsgType: raftpb.MessageType_MagNewElection, Address: s.r.NodeAddress}, nil
			}
		}

		return &raftpb.Response{MsgType: raftpb.MessageType_MsgEmpty}, nil
	}

	// 收到append信息
	if message.GetMsgType() == raftpb.MessageType_MsgAppend {
		if message.GetMatchCheck() == true {
			// 根据term和index判断是否match

			// index = -1, 说明没有日志，回复match
			if message.GetIndex() == -1 {
				return &raftpb.Response{MsgType: raftpb.MessageType_MsgAppend,
					Match:      true,
					MatchIndex: message.GetIndex(),
					Address:    s.r.NodeAddress}, nil
			}

			// 日志长度 - 1 小于发来的index， 不match
			if len(s.r.Log.entries)-1 < int(message.GetIndex()) {
				return &raftpb.Response{MsgType: raftpb.MessageType_MsgAppend,
					Match:   false,
					Address: s.r.NodeAddress}, nil
			}

			// 同一index下term不同，不match
			if s.r.Log.entries[message.GetIndex()].GetTerm() != message.GetTerm() {
				return &raftpb.Response{MsgType: raftpb.MessageType_MsgAppend,
					Match:   false,
					Address: s.r.NodeAddress}, nil
			}

			// 其他情况，返回match
			return &raftpb.Response{MsgType: raftpb.MessageType_MsgAppend,
				Match: true, MatchIndex: message.GetIndex(),
				Address: s.r.NodeAddress}, nil
		} else {
			// 收到leader的日志后，附加到自己的日志上
			s.r.Log.entries = s.r.Log.entries[:message.GetIndex()+1]
			for i := 0; i < len(message.Entries); i++ {
				s.r.Log.entries = append(s.r.Log.entries, *message.Entries[i])
			}
			s.r.Log.lastappliedIndex = message.GetIndex()

			// 写到本地数据库
			if s.r.db != nil {
				go func() {
					for i := 0; i < len(message.Entries); i++ {
						handleLogEntry(message.Entries[i], s.r.db)
						s.r.Log.lastappliedIndex++
					}

				}()
			}

			//log.Println(s.r.Log.entries)
		}

		return &raftpb.Response{MsgType: raftpb.MessageType_MsgAppend, AppendSuccess: true, Address: s.r.NodeAddress}, nil
	}
	return &raftpb.Response{MsgType: raftpb.MessageType_MsgEmpty}, nil
}

// 处理发送rpc后的返回结果
func handleResponse(r *Raft, response *raftpb.Response) {
	// 收到投票信息，检查选票是否过半，如过半，成为leader
	if response.GetMsgType() == raftpb.MessageType_MagNewElection && r.State == Candidate {
		r.voteRecord[response.GetAddress()] = true
		log.Printf("%v get vote from %v\n", r.NodeAddress, response.GetAddress())
		count := 0
		for i := 0; i < len(r.PeerNodeAddress); i++ {
			if r.voteRecord[r.PeerNodeAddress[i]] == true {
				count += 1
			}
		}

		if count+1 >= (len(r.PeerNodeAddress)+1)/2 {
			r.BecomeLeader(r.Terms)
		}
	}

	// 处理日志附加操作
	if response.GetMsgType() == raftpb.MessageType_MsgAppend && r.State == Leader {

		// Append 成功，重置match值
		if response.GetAppendSuccess() == true {
			r.Prs[response.GetAddress()].Match = -2
			r.appendRecord[response.GetAddress()] = true
			count := 0
			for i := 0; i < len(r.PeerNodeAddress); i++ {
				if r.appendRecord[r.PeerNodeAddress[i]] == true {
					count += 1
				}
			}

			if count+1 >= (len(r.PeerNodeAddress)+1)/2 {
				r.Log.committed = r.Log.LastIndex()
			}
			return
		}

		//没有match，减少对应follower的Next，继续重试
		if response.GetMatch() == false {
			r.Prs[response.GetAddress()].Next -= 1
			r.sendAppend(response.GetAddress())
		} else {
			r.Prs[response.GetAddress()].Match = response.GetMatchIndex()
			r.sendAppend(response.GetAddress())
		}
	}
}

// 发送消息
func sendMessage(r *Raft, address string, message *raftpb.Message) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			return
		}
	}(conn)
	c := raftpb.NewRaftrpcClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*30)
	defer cancel()
	res, err := c.HandleMessage(ctx, message)
	handleResponse(r, res)
	if err != nil {
		fmt.Println(err)
	}
}

func (r *Raft) sendHeartBeat() {
	for i := 0; i < len(r.PeerNodeAddress); i++ {
		sendMessage(r, r.PeerNodeAddress[i],
			&raftpb.Message{MsgType: raftpb.MessageType_MsgHeartBeat,
				Term:          r.Terms,
				LeaderAddress: r.NodeAddress})
	}
}

func (r *Raft) sendAppend(address string) {

	// 针对每一个follow发送append
	message := &raftpb.Message{}
	message.MsgType = raftpb.MessageType_MsgAppend
	loglen := len(r.Log.entries)
	if r.Prs[address].Next > 0 {

		message.Term = r.Log.entries[r.Prs[address].Next-1].GetTerm()
		message.Index = r.Prs[address].Next - 1
	} else {
		message.Index = -1
	}
	message.MatchCheck = true
	log.Printf("send to %v, match index is %v , Next index is %v\n", address, r.Prs[address].Match, r.Prs[address].Next)
	if r.Prs[address].Match+1 == r.Prs[address].Next || (r.Prs[address].Match == -1) {
		for i := r.Prs[address].Next; int(i) < loglen; i++ {
			message.Entries = append(message.Entries, &r.Log.entries[i])
		}
		message.MatchCheck = false
	}
	sendMessage(r, address, message)
}

func (r *Raft) BecomeLeader(term int64) {
	r.State = Leader
	r.heartbeatElapsed = 0
	r.Terms = term
	r.leaderAddress = r.NodeAddress
	for i := 0; i < len(r.PeerNodeAddress); i++ {
		r.Prs[r.PeerNodeAddress[i]] = &Progress{}
		r.Prs[r.PeerNodeAddress[i]].Next = r.Log.LastIndex() + 1
		r.Prs[r.PeerNodeAddress[i]].Match = -2
		r.appendRecord[r.PeerNodeAddress[i]] = false
	}
}

func (r *Raft) BecomeFollower(term int64) {
	r.State = Follower
	r.electionElapsed = 0
	r.Terms = term
	//log.Println("leader address is",r.leaderAddress)
}

func (r *Raft) BecomeCandidate() {
	r.Terms++
	r.State = Candidate
	// 发送选举信息
	for i := 0; i < len(r.PeerNodeAddress); i++ {
		if len(r.Log.entries) != 0 {
			sendMessage(r, r.PeerNodeAddress[i],
				&raftpb.Message{MsgType: raftpb.MessageType_MagNewElection,
					Term:  r.Log.entries[r.Log.LastIndex()].GetTerm(),
					Index: r.Log.LastIndex()})
		} else {
			sendMessage(r, r.PeerNodeAddress[i],
				&raftpb.Message{MsgType: raftpb.MessageType_MagNewElection,
					Term:  r.Terms - 1,
					Index: r.Log.LastIndex()})
		}

	}
}

// Append 日志附加操作接口
func (r *Raft) Append(data []byte) bool {
	ch1 := make(chan bool)
	if r.State == Leader {
		logEntry := raftpb.Entry{}
		logEntry.Term = r.Terms
		if len(r.Log.entries) != 0 {
			logEntry.Index = r.Log.LastIndex() + 1
		} else {
			logEntry.Index = 0
		}
		for i := 0; i < len(r.PeerNodeAddress); i++ {
			r.Prs[r.PeerNodeAddress[i]].Next = r.Log.LastIndex() + 1
		}
		logEntry.Data = data
		r.Log.Append(&logEntry)

		// 给其他副本发送消息，将该日志复制到其他副本上
		for i := 0; i < len(r.PeerNodeAddress); i++ {
			r.sendAppend(r.PeerNodeAddress[i])
		}

		go func() {
			for {
				//time.Sleep(time.Millisecond * 1)
				if r.Log.committed == logEntry.Index {
					ch1 <- true
				}
			}
		}()

		select {
		case vch1 := <-ch1:
			// 日志被提交，返回true
			if vch1 == true {
				return true
			}

		//超时还没有提交，返回false
		case <-time.After(time.Second):
			return false
		}

	}
	return false
}

// 日志应用到本地数据库的接口
func handleLogEntry(entry *raftpb.Entry, db *leveldb.DB) {
	kventry := KVentry{}

	err := json.Unmarshal(entry.GetData(), &kventry)
	if err != nil {
		log.Println(err)
	}

	if kventry.KVOperation == Put {
		err := db.Put(kventry.Key, kventry.Value, nil)
		if err != nil {
			log.Println(err)
		}
	} else if kventry.KVOperation == Del {
		err := db.Delete(kventry.Key, nil)
		if err != nil {
			log.Println(err)
		}
	}
}
