package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//

//Estrutura básica e eleição

const MinElectionTimeout = 500
const MaxElectionTimeout = 1000

const (
	Follower = iota
	Candidate
	Leader
)

type CommandTerm struct {
	Command interface{}
	Term    int
}

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        []CommandTerm
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	status       int
	currentTerm  int
	votedFor     int
	commitIndex  int
	lastLogIndex int
	nextIndex    []int
	matchIndex   []int

	log []CommandTerm

	applyMsg chan ApplyMsg

	lastAccessed time.Time
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.status == Leader
}

func randTimeout() time.Duration {
	randTimeout := MinElectionTimeout + rand.Intn(MaxElectionTimeout-MinElectionTimeout)
	return time.Duration(randTimeout) * time.Millisecond
}

func (rf *Raft) manageLeader() {
	rf.mu.Lock()

	if rf.status != Leader {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm
	me := rf.me
	peers := rf.peers

	rf.mu.Unlock()

	// Envia heartbeat para todos os outros servidores.
	for peer := range peers {
		if peer == me {
			continue
		}

		go func(peer int) {
			args := AppendEntriesArgs{
				Term:     term,
				LeaderId: me,
			}

			reply := AppendEntriesReply{}

			ok := rf.sendAppendEntries(peer, &args, &reply)

			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Verifica se ainda pertence ao mesmo termo.
			if rf.status != Leader ||
				rf.currentTerm != term {
				return
			}

			// Descobriu um termo superior.
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.status = Follower
				rf.lastAccessed = time.Now()

				rf.persist()
				return
			}
		}(peer)
	}

	// Intervalo entre heartbeats.
	time.Sleep(100 * time.Millisecond)
}

func (rf *Raft) manageCandidate() {
	timeout := randTimeout()

	rf.mu.Lock()

	if rf.status != Candidate {
		rf.mu.Unlock()
		return
	}

	// Inicia uma nova eleição.
	rf.currentTerm++
	term := rf.currentTerm
	rf.votedFor = rf.me
	rf.lastAccessed = time.Now()

	me := rf.me
	peers := rf.peers

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	rf.persist()
	rf.mu.Unlock()

	total := len(peers)
	majority := total/2 + 1

	// O candidato já votou em si mesmo.
	votes := 1

	// Caso exista apenas um servidor.
	if votes >= majority {
		rf.mu.Lock()

		if rf.status == Candidate &&
			rf.currentTerm == term {
			rf.status = Leader

			for peer := range rf.peers {
				rf.nextIndex[peer] = len(rf.log)
				rf.matchIndex[peer] = 0
			}
		}

		rf.mu.Unlock()
		return
	}

	type VoteResult struct {
		reply RequestVoteReply
		ok    bool
	}

	results := make(chan VoteResult, total-1)

	// Envia pedidos de voto.
	for peer := range peers {
		if peer == me {
			continue
		}

		go func(peer int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(peer, &args, &reply)

			results <- VoteResult{
				reply: reply,
				ok:    ok,
			}
		}(peer)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	finished := 0

	for finished < total-1 {
		select {
		case result := <-results:
			finished++

			if !result.ok {
				continue
			}

			rf.mu.Lock()

			// Descobriu um termo superior.
			if result.reply.Term > rf.currentTerm {
				rf.currentTerm = result.reply.Term
				rf.votedFor = -1
				rf.status = Follower
				rf.lastAccessed = time.Now()

				rf.persist()
				rf.mu.Unlock()
				return
			}

			// Verifica se ainda pertence à mesma eleição.
			if rf.status != Candidate ||
				rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}

			if result.reply.VoteGranted {
				votes++
			}

			// Recebeu a maioria dos votos.
			if votes >= majority {
				rf.status = Leader

				for peer := range rf.peers {
					rf.nextIndex[peer] = len(rf.log)
					rf.matchIndex[peer] = 0
				}

				rf.persist()
				rf.mu.Unlock()
				return
			}

			rf.mu.Unlock()

		case <-timer.C:
			return
		}
	}

	// Todas as respostas chegaram sem maioria (voto dividido).
	// Pela Figura 2, o candidato só inicia nova eleição quando o
	// timeout de eleição expira; esperar o restante do timer evita
	// incrementar o termo em rajadas e reduz novos votos divididos.
	<-timer.C
}

func (rf *Raft) manageFollower() {
	timeout := randTimeout()

	rf.mu.Lock()
	lastAccessed := rf.lastAccessed
	rf.mu.Unlock()

	time.Sleep(timeout)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Verifica se ainda é Follower.
	if rf.status != Follower {
		return
	}

	// Verifica se recebeu comunicação durante o timeout.
	if rf.lastAccessed != lastAccessed {
		return
	}

	// Inicia uma nova eleição.
	rf.status = Candidate
	rf.lastAccessed = time.Now()

	rf.persist()
}

func (rf *Raft) manageLifecycle() {
	for !rf.killed() {
		rf.mu.Lock()
		status := rf.status
		rf.mu.Unlock()

		if status == Follower {
			rf.manageFollower()

		} else if status == Candidate {
			rf.manageCandidate()

		} else if status == Leader {
			rf.manageLeader()
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

// example code to send an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(
	server int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) bool {
	return rf.peers[server].Call(
		"Raft.AppendEntries",
		args,
		reply,
	)
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(
	args *RequestVoteArgs,
	reply *RequestVoteReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// 1. Rejeita candidatos com termo antigo.
	if args.Term < rf.currentTerm {
		return
	}

	// 2. Atualiza o termo se necessário.
	// Não reinicia o timer aqui: pela Figura 2, o timer só é
	// reiniciado ao receber AppendEntries do líder atual ou ao
	// conceder o voto (feito no passo 5).
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = Follower
	}

	reply.Term = rf.currentTerm

	// 3. Verifica se o log do candidato está atualizado.
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	logIsUpToDate :=
		args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm &&
				args.LastLogIndex >= lastLogIndex)

	// 4. Verifica se ainda pode votar.
	canVote :=
		rf.votedFor == -1 ||
			rf.votedFor == args.CandidateId

	// 5. Concede o voto.
	if logIsUpToDate && canVote {
		rf.votedFor = args.CandidateId
		rf.status = Follower
		rf.lastAccessed = time.Now()

		reply.VoteGranted = true
	}

	rf.persist()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	// 1. Rejeita Leaders com termo antigo.
	if args.Term < rf.currentTerm {
		return
	}

	// 2. Atualiza o termo se necessário.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// 3. Reconhece o Leader.
	rf.status = Follower
	rf.lastAccessed = time.Now()

	reply.Term = rf.currentTerm

	// 4. Verifica se o índice anterior é válido.
	if args.PrevLogIndex < 0 ||
		args.PrevLogIndex >= len(rf.log) {
		return
	}

	// 5. Verifica se o termo anterior coincide.
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	// 6. Procura conflitos no log.
	index := 0

	for ; index < len(args.Entry); index++ {
		currentIndex := args.PrevLogIndex + 1 + index

		if currentIndex >= len(rf.log) {
			break
		}

		if rf.log[currentIndex].Term != args.Entry[index].Term {
			rf.log = rf.log[:currentIndex]
			break
		}
	}

	// 7. Adiciona as entradas restantes.
	if index < len(args.Entry) {
		rf.log = append(rf.log, args.Entry[index:]...)
	}

	rf.lastLogIndex = len(rf.log) - 1

	// 8. Atualiza o commitIndex.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(
			args.LeaderCommit,
			rf.lastLogIndex,
		)
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	rf.persist()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.status == Leader
	if !isLeader {
		return 0, 0, false
	}

	term = rf.currentTerm
	rf.log = append(rf.log, CommandTerm{
		Command: command,
		Term:    term,
	})
	rf.lastLogIndex = len(rf.log) - 1
	index = rf.lastLogIndex
	rf.persist()
	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Sinaliza para a goroutine de ciclo de vida encerrar, evitando
	// que instâncias de testes anteriores continuem rodando.
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Inicializa o estado do servidor.
	rf.status = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	// Inicializa o log com uma entrada fictícia.
	rf.log = []CommandTerm{
		{
			Command: nil,
			Term:    0,
		},
	}

	rf.lastLogIndex = len(rf.log) - 1

	// Inicializa os índices dos servidores.
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// Inicializa o canal de aplicação.
	rf.applyMsg = applyCh

	// Inicializa o timeout de eleição.
	rf.lastAccessed = time.Now()

	// Recupera o estado persistido.
	rf.readPersist(persister.ReadRaftState())

	// Inicia a goroutine responsável pelo ciclo de vida.
	go rf.manageLifecycle()

	return rf
}
