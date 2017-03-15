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
       "bytes"
       "encoding/gob"
//     "fmt"
       "labrpc"
//     "log"
       "math/rand"
       "sync"
       "time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
     Index       int
     Term        int
     Command     interface{}
     UseSnapshot bool
     Snapshot    []byte
}

type Log struct {
     Index   int // log index
     Term    int // the term when the entry was received by leader
     Command interface{}
}



//
// A Go object implementing a single Raft peer.
//
type Raft struct {
     mu        sync.Mutex
     peers     []*labrpc.ClientEnd
     persister *Persister
     me        int  // index into peers[]

     state        string  //"Leader"/"Follower"/"Candidate"
     CurrentTerm  int
     VotedFor     int   // Id of candidate that received most recent vote,otherwise -1

     log         []Log // log[0] stores the term and index of last  discarded log
     commitIndex int   // highest committed log entry
     startIndex  int   // discarded logs index

     killCh      chan struct{}  // killed
     DemoteCh    chan struct{}  // down to follower if set
     heartBeatCh chan HeartBeat // heartbeats
     applyCh     chan ApplyMsg  // log is applied


     nextIndex  []int
     matchIndex []int
}

type HeartBeat struct {
     leaderId     int
     term         int
     leaderCommit int
}

func (rf *Raft) GetState() (int, bool) {
     rf.mu.Lock()
     defer rf.mu.Unlock()
     return rf.CurrentTerm, rf.state == "Leader"
}

func (rf *Raft) Persister() *Persister {
     return rf.persister
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
     w := new(bytes.Buffer)
     e := gob.NewEncoder(w)
     e.Encode(rf.CurrentTerm)
     e.Encode(rf.VotedFor)
     e.Encode(rf.startIndex)
     e.Encode(rf.log)
     data := w.Bytes()
     rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
     r := bytes.NewBuffer(data)
     d := gob.NewDecoder(r)
     d.Decode(&rf.CurrentTerm)
     d.Decode(&rf.VotedFor)
     d.Decode(&rf.startIndex)
     d.Decode(&rf.log)

     rf.commitIndex = rf.startIndex
}


type RequestVoteArgs struct {
     Term        int  // candidate's vote
     CandidateId int  // candidate requesting for vote
     LastLogIndex int // index of candidate's last log entry
     LastLogTerm  int // term of candidate's last log entry
}


type RequestVoteReply struct {
     FollowerId    int
     Term        int  // current term, for candidate to update itself
     VoteGranted bool // true means candidate receives vote
}


type AppendEntriesArgs struct {
     IsHeartBeat bool //only true if its a heartbeat, LeaderId and Term only exist if true

     LeaderId    int
     Term        int

     PrevLogIndex int   // preceding log entry's index
     PrevLogTerm  int   //
     LeaderCommit int   // commit index of leader
     Entries      []Log //
}


type AppendEntriesReply struct {
     FollowerId  int
     Term        int
     Success     bool
     NextIndex   int
}


type InstallSnapshotArgs struct {
     LeaderId int
     Term     int

     LastIncludedIndex int
     LastIncludedTerm  int
     Snapshot          []byte
}


type InstallSnapshotReply struct {
     Success    bool

     FollowerId int
     Term       int
}


// Sends  RequestVote RPC
// Returns true if RPC is delivered
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply*RequestVoteReply) bool {
                   return rf.peers[server].Call("Raft.RequestVote", args,
                   reply)
}

//
// Append Entries RPC sender
//
func (rf* Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
                     return rf.peers[server].Call("Raft.AppendEntries",
                     args, reply)
}


func (rf* Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs,reply *InstallSnapshotReply) bool {
      return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}


func (rf *Raft) setCurrentTerm(currentTerm int) {
     if currentTerm != rf.CurrentTerm {
        rf.VotedFor = -1
        }
        rf.CurrentTerm = currentTerm
}

func (rf *Raft) Demote() {
     if rf.state != "Follower" {
        rf.state = "Follower"
        select {
                case rf.DemoteCh <- struct{}{}:
                default:
        }
     }
}

func (rf *Raft) lastLogIndex() (int, int) {
     return rf.log[len(rf.log) - 1].Index, rf.log[len(rf.log) - 1].Term
}

// Check if leader is behind, if so catch back up then turn into follower
func (rf *Raft) checkStaleLeader(tag string, term int, peer int) bool {
     if rf.CurrentTerm < term {
        rf.setCurrentTerm(term)
        rf.Demote()
        return true
     }
     return false
}


//
// RequestVote handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply){
        rf.mu.Lock()
        defer rf.mu.Unlock()
        defer rf.persist()

        reply.FollowerId = rf.me

        rf.checkStaleLeader("RequestVote", args.Term, args.CandidateId)

        // if candidate has stale term, reject
        if args.Term < rf.CurrentTerm {

           reply.VoteGranted = false
           reply.Term = rf.CurrentTerm
           return
        }
        lastLogIndex, lastLogTerm := rf.lastLogIndex()
        if lastLogTerm > args.LastLogTerm || (lastLogTerm ==args.LastLogTerm && lastLogIndex > args.LastLogIndex) {

           reply.VoteGranted = false
           reply.Term = rf.CurrentTerm
           return
        }

        
        if rf.VotedFor == -1 {
           rf.VotedFor = args.CandidateId
           reply.VoteGranted = true
        } else {
           reply.VoteGranted = (rf.VotedFor == args.CandidateId)
        }

        reply.Term = rf.CurrentTerm
}

//
// AppendEntries handler.
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
     rf.mu.Lock()
     defer rf.mu.Unlock()
     defer rf.persist()

     rf.checkStaleLeader("AppendEntries", args.Term,
     args.LeaderId)
     reply.FollowerId = rf.me
     reply.Term = rf.CurrentTerm
     reply.NextIndex = -1

     // If leader has stale term, reject.

     if args.Term < rf.CurrentTerm {
        reply.Success = false
        return
     }
     if args.IsHeartBeat {
        go func() {
           var heartBeat HeartBeat
           heartBeat.leaderId = args.LeaderId
           heartBeat.term = args.Term
           heartBeat.leaderCommit = args.LeaderCommit
           rf.heartBeatCh <- heartBeat
        }()
     ok, nextIndex := rf.LogIntegrity(args)
     reply.Success = ok
     reply.NextIndex = nextIndex

     if !reply.Success {
     }
     } else {
        ok, nextIndex := rf.LogIntegrity(args)
        if !ok {
           reply.Success = false
           reply.NextIndex = nextIndex
           return
        }
        if len(args.Entries) != 0 {
            different := false // if has inconsistent logs
           for i, j := 0, args.PrevLogIndex + 1; i < len(args.Entries); i++ {
               if j >= rf.startIndex + len(rf.log)  {
                  rf.log = append(rf.log, args.Entries[i])
               } else if j - rf.startIndex >= 1 {
                  if different || rf.log[j - rf.startIndex].Term != args.Entries[i].Term {
                     rf.log[j - rf.startIndex] =  args.Entries[i]
                     different = true
                  }
               }
               j+=1
           }
           if different && rf.startIndex +  len(rf.log) > args.PrevLogIndex + len(args.Entries) + 1 {
               rf.log = rf.log[0 : args.PrevLogIndex + len(args.Entries) + 1 - rf.startIndex]
           }
       }
       reply.Success = true
       newCommitIndex := args.LeaderCommit
       lastLogIndex, _ :=rf.lastLogIndex()
       if newCommitIndex >lastLogIndex {
          newCommitIndex = lastLogIndex
       }
       if rf.commitIndex < newCommitIndex {
          for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
              var applyMsg ApplyMsg
              applyMsg.Index    = i
              applyMsg.Term= rf.log[i-rf.startIndex].Term
              applyMsg.Command= rf.log[i-rf.startIndex].Command
              applyMsg.UseSnapshot= false
              rf.applyCh<- applyMsg
          }
          rf.commitIndex = newCommitIndex
      }
   }
}

//discard previous log entries
//

func (rf *Raft) TrashLogs(lastLogIndex int, lastLogTerm int) {
     rf.mu.Lock()
     defer rf.mu.Unlock()
     defer rf.persist()

     if rf.commitIndex < lastLogIndex {
        rf.commitIndex = lastLogIndex
     }

     discardFrom := lastLogIndex - rf.startIndex

     if discardFrom <= 0 {
        return
     } else if discardFrom >= len(rf.log) {

       rf.startIndex = lastLogIndex
       rf.log = make([]Log, 1)
       rf.log[0].Index = lastLogIndex
       rf.log[0].Term = lastLogTerm
     } else {
       retainLog := rf.log[discardFrom + 1:]
       rf.log[0].Index = rf.log[discardFrom].Index
       rf.log[0].Term = rf.log[discardFrom].Term
       rf.log = append(rf.log[0:1], retainLog...)
       rf.startIndex += discardFrom

       if lastLogIndex != 0 && (rf.log[0].Index != lastLogIndex ||  rf.log[0].Term != lastLogTerm) {
          rf.log = make([]Log, 1)
          rf.log[0].Index = lastLogIndex
          rf.log[0].Term =  lastLogTerm
       }
    }
}



func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
     rf.mu.Lock()
     defer rf.mu.Unlock()
     defer rf.persist()

     rf.checkStaleLeader("InstallSnapshot", args.Term, args.LeaderId)
     reply.FollowerId = rf.me
     reply.Term = rf.CurrentTerm
     reply.Success = false

     // if leader is stale reject
     if args.Term < rf.CurrentTerm {
        return
     }

     reply.Success = true
     if args.LastIncludedIndex <= rf.commitIndex {
        return
     }

     var applyMsg ApplyMsg
     applyMsg.UseSnapshot = true
     applyMsg.Index = args.LastIncludedIndex
     applyMsg.Term = args.LastIncludedTerm
     applyMsg.Snapshot = args.Snapshot
     rf.applyCh <- applyMsg
}

//
func (rf *Raft) LogIntegrity(args AppendEntriesArgs) (bool, int) {
     lastLogIndex, _ := rf.lastLogIndex()
     if lastLogIndex < args.PrevLogIndex {
        return false, lastLogIndex
     }
     if args.PrevLogIndex <= rf.commitIndex {
        return true, -1
     }
     if rf.log[args.PrevLogIndex - rf.startIndex].Term != args.PrevLogTerm {
        matchIndex := args.PrevLogIndex - 1
        for ; matchIndex > rf.startIndex; matchIndex-- {
            if rf.log[args.PrevLogIndex - rf.startIndex].Term != rf.log[matchIndex - rf.startIndex].Term \
{
                       break
            }
        }
        if matchIndex == rf.startIndex {
           matchIndex = rf.log[0].Index
        }
        return false, matchIndex
    }
    return true, -1
}

func ElectionTimer() time.Duration {
     //randomize election between 250ms and 300ms.
     return time.Duration(250 + rand.Intn(50)) * time.Millisecond
}

func (rf *Raft) FLoop() {
     rf.mu.Lock()
     rf.state = "Follower"
     rf.mu.Unlock()

     timer := time.NewTimer(ElectionTimer())

     for {
         select {
                case <-rf.killCh: {
                     return
                }
                case heartBeat := <-rf.heartBeatCh: {
                     rf.mu.Lock()
                     if heartBeat.term < rf.CurrentTerm {
                     } else {
                       rf.setCurrentTerm(heartBeat.term)
                       rf.persist()
                       timer.Reset(ElectionTimer())
                     }
                     rf.mu.Unlock()
                }
                case <-timer.C: {
                     go rf.CLoop()
                     return
                }
         }
      }
}

//
func (rf *Raft) CLoop() {
     rf.mu.Lock()
     rf.state = "Candidate"
     rf.mu.Unlock()

     for {
         rf.mu.Lock()
         rf.setCurrentTerm(rf.CurrentTerm + 1)
         rf.VotedFor = rf.me
         rf.persist()
         rf.mu.Unlock()
         replies:=make(chan RequestVoteReply)
         for i:=range rf.peers {
             if i == rf.me {
                continue
             }
             go func(i int) {
                var args RequestVoteArgs
                var reply RequestVoteReply

                rf.mu.Lock()

                lastLogIndex, lastLogTerm := rf.lastLogIndex()
                args.LastLogIndex = lastLogIndex
                args.LastLogTerm = lastLogTerm
                args.CandidateId = rf.me
                args.Term = rf.CurrentTerm

                rf.mu.Unlock()

                if rf.sendRequestVote(i, args, &reply) {
                   replies <- reply
                }
             }(i)
       }
votes :=1
Other:
        for {
            select {
                   case <-rf.killCh: {
                        return
                   }
                   case <- rf.DemoteCh: {
                        go rf.FLoop()
                        return
                   }
                   case reply := <-replies: {
                        rf.mu.Lock()
                        leave := false
                        if rf.checkStaleLeader("CandidateLoop", reply.Term,reply.FollowerId) {
                           rf.persist()
                           go rf.FLoop()
                           leave = true
                        } else if reply.VoteGranted {
                           votes += 1
                           if (votes > len(rf.peers)/2) {
                              go rf.LLoop()
                              leave = true
                           }
                        }
                        rf.mu.Unlock()
                        if leave {
                           return
                        }
                   }

                   case <- time.After(ElectionTimer()): {
                        break Other
                   }
             }
        }
   }
}

//
func (rf *Raft) LogsSync(server int) {
     for {
         rf.mu.Lock()
         lostLeadership := rf.state != "Leader"
         lastLogIndex, _ := rf.lastLogIndex()
         sendSnapshot := false

         if rf.nextIndex[server] <= rf.startIndex {
            sendSnapshot = true
         }
         var argsAppend AppendEntriesArgs
         var replyAppend AppendEntriesReply
         var argsSnapshot InstallSnapshotArgs
         var replySnapshot InstallSnapshotReply

         if !lostLeadership {
            if sendSnapshot {
               argsSnapshot.LeaderId = rf.me
               argsSnapshot.Term = rf.CurrentTerm
               argsSnapshot.LastIncludedIndex = rf.log[0].Index
               argsSnapshot.LastIncludedTerm = rf.log[0].Term
               argsSnapshot.Snapshot = rf.persister.ReadSnapshot()

            } else {
               argsAppend.IsHeartBeat = false
               argsAppend.LeaderCommit = rf.commitIndex
               argsAppend.LeaderId = rf.me
               argsAppend.Term = rf.CurrentTerm
               argsAppend.PrevLogIndex = rf.nextIndex[server] - 1
               argsAppend.PrevLogTerm = rf.log[rf.nextIndex[server] - 1 - rf.startIndex].Term

               for j := argsAppend.PrevLogIndex + 1; j <= lastLogIndex; j++{
                   argsAppend.Entries = append(argsAppend.Entries, rf.log[j- rf.startIndex])
               }
           }
        }

         rf.mu.Unlock()
        if lostLeadership {
           return
        }

        var repliedTerm int

        if sendSnapshot {
           rf.mu.Lock()
           rf.mu.Unlock()
           if !rf.sendInstallSnapshot(server, argsSnapshot, &replySnapshot) {
              return
           }
           repliedTerm = replySnapshot.Term
        } else {

          if !rf.sendAppendEntries(server, argsAppend, &replyAppend) {
             return
          }

          repliedTerm = replyAppend.Term
        }

        rf.mu.Lock()
        if rf.checkStaleLeader("SyncLog", repliedTerm, server) {
           rf.persist()
           rf.mu.Unlock()
           return
        }

        setLastIndex := func(index int) {
             if rf.nextIndex[server] < index + 1 {
                 rf.nextIndex[server] = index + 1
             }
             if rf.matchIndex[server] < index {
                 rf.matchIndex[server] = index
             }
        }

        if sendSnapshot {
            if replySnapshot.Success {
               setLastIndex(argsSnapshot.LastIncludedIndex)
               rf.mu.Unlock()
               return
            }
        } else {
            if replyAppend.Success {
               setLastIndex(lastLogIndex)

               rf.mu.Unlock()
                return

            } else if replyAppend.NextIndex != -1 {
               rf.nextIndex[server] = replyAppend.NextIndex + 1
            }
        }
        rf.mu.Unlock()
    }
}

//
func (rf *Raft) LLoop() {
     rf.mu.Lock()

     rf.state = "Leader"
     rf.nextIndex = make([]int, len(rf.peers))
     rf.matchIndex = make([]int, len(rf.peers))
     lastLogIndex, _ := rf.lastLogIndex()
     for i := 0; i < len(rf.nextIndex); i++ {
         rf.nextIndex[i] = lastLogIndex + 1
     }

     rf.mu.Unlock()

     for i := range rf.peers {
         if i != rf.me {
            go rf.LogsSync(i)
         }
     }

     for {
         rf.mu.Lock()
         newCommitIndex := rf.commitIndex

         for c := rf.startIndex + len(rf.log) - 1; c >rf.commitIndex; c-- {
             if rf.log[c - rf.startIndex].Term == rf.CurrentTerm {

                count := 1
                for i := range rf.peers {
                    if i == rf.me {
                       continue
                    }
                    if rf.matchIndex[i] >= c {
                       count += 1
                    }
                }

                if count > len(rf.peers) / 2 {
                      newCommitIndex = c
                    break
                }
             }
          }

          if newCommitIndex != rf.commitIndex {
              for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {

                  var applyMsg ApplyMsg
                  applyMsg.Index = i
                  applyMsg.Term = rf.log[i - rf.startIndex].Term
                  applyMsg.Command = rf.log[i - rf.startIndex].Command
                  applyMsg.UseSnapshot = false
                  rf.applyCh <- applyMsg
              }

              rf.commitIndex = newCommitIndex
              for i := range rf.peers {
                  if i != rf.me {
                     go rf.LogsSync(i)
                  }
              }
          }
            rf.mu.Unlock()

          replies := make(chan AppendEntriesReply)
          for i := range rf.peers {
              if i == rf.me {
                 continue
              }

              go func(i int) {
                 var args AppendEntriesArgs
                 var reply AppendEntriesReply

                 rf.mu.Lock()

                 lostLeadership := rf.state != "Leader"
                 missingLog := rf.nextIndex[i] < rf.startIndex + len(rf.log)

                 if !lostLeadership {

                    args.IsHeartBeat = true
                    args.LeaderCommit = rf.commitIndex
                    args.LeaderId = rf.me
                    args.Term = rf.CurrentTerm

                     if !missingLog {
                       args.PrevLogIndex = rf.nextIndex[i] - 1
                       args.PrevLogTerm = rf.log[rf.nextIndex[i] - 1 -rf.startIndex].Term
                    }
                 }
                 rf.mu.Unlock()
                 if lostLeadership {
                    return
                 }

                 if missingLog || (rf.sendAppendEntries(i, args,&reply) && !reply.Success) {
                    go rf.LogsSync(i)
                    if !missingLog {
                       replies <- reply
                    }
                 }
              }(i)
          }
Other:
        for {
            select{
                case <-rf.killCh: {
                     return
                }
                case <-rf.DemoteCh: {
                     go rf.FLoop()
                     return
                }

                case <-time.After(time.Duration(100) * time.Millisecond): {
                     break Other
                }

                case reply := <-replies: {
                rf.mu.Lock()

                leave := false
                if rf.checkStaleLeader("Heartbeat", reply.Term, reply.FollowerId) {
                     rf.persist()
                     go rf.FLoop()
                     leave = true
                } else {
                }
                rf.mu.Unlock()

                if leave {
                   return
                 }
             }
          }
      }
   }
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
     rf.mu.Lock()
     defer rf.mu.Unlock()

     if rf.state != "Leader" {
        return -1, -1, false
        }

     logIndex, _ := rf.lastLogIndex()
     logIndex += 1

     var log Log
     log.Index = logIndex
     log.Term = rf.CurrentTerm
     log.Command = command

     rf.log = append(rf.log, log)
     rf.persist()

     for i := range rf.peers {
         if i != rf.me {
            go rf.LogsSync(i)
         }
     }
     return logIndex, log.Term, true
}

func (rf *Raft) Kill() {
     rf.killCh <- struct{}{}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
     rf := &Raft{}
     rf.peers = peers
     rf.persister = persister
     rf.me = me

     rf.log = make([]Log, 1)
     rf.log[0].Index = 0
     rf.log[0].Term = 0

     rf.commitIndex = 0
     rf.startIndex = 0

     rf.state = "Follower"
     rf.CurrentTerm = 0
     rf.VotedFor = -1

     rf.killCh = make(chan struct{})
     rf.DemoteCh = make(chan struct{})
     rf.heartBeatCh = make(chan HeartBeat)
     rf.applyCh = applyCh

     // Initialize from state persisted before a crash.
     rf.readPersist(persister.ReadRaftState())



     // Start as follower.
      go rf.FLoop()

     return rf
}
















