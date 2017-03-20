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
     CurrentTerm  int   //Last term Server has seen
     VotedFor     int   // Id of candidate that received most recent vote,otherwise -1

     log         []Log // log[0] stores the term and index of last  discarded log
     commitIndex int   // highest committed log entry
     startIndex  int   // discarded logs index
                       // logs = [ 0, startIndex + 1, startIndex + 2, ... ]
			  // invariant: commitIndex >= startIndex

     killCh      chan struct{}  // if the peer has been killed
     DemoteCh    chan struct{}  // demotes server to follower state if set
     heartBeatCh chan HeartBeat // If set indicates it is a heartbeat rather than full Raft object
     applyCh     chan ApplyMsg  // log is applied

     //Only used by machines in leader state to look at other machines, meaning nextIndex[leader] and matchIndex[leader] are undefined 
     nextIndex  []int //for each server, the index of the next log needed by that server
     matchIndex []int //for each server, the index of the highest log entry confirmed as being replicated on that server
}

//Heartbeat object, sent out periodically by leader to maintain authority/correctness with other servers

type HeartBeat struct {
     leaderId     int //ID # of server that is leader
     term         int //what term it is
     leaderCommit int //Highest log entry committed by leader
}

//Return the server's current term and whether it thinks that it is the leader
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

//Object consisting of the args for RequestVote RPCs

type RequestVoteArgs struct {
     Term        int  // candidate's vote
     CandidateId int  // candidate requesting for vote
     LastLogIndex int // index of candidate's last log entry
     LastLogTerm  int // term of candidate's last log entry
}

//Object containing the structure of replies from RequestVote RPCs

type RequestVoteReply struct {
     FollowerId    int
     Term        int  // current term, for candidate to update itself
     VoteGranted bool // true means candidate receives vote
}

//Object containing args for AppendEntries RPCs

type AppendEntriesArgs struct {
     IsHeartBeat bool //only true if its a heartbeat, the object only consists of leaderID and term in this case

     LeaderId    int //Leader's ID
     Term        int //Leader's term

     PrevLogIndex int   // index of most recent log entry
     PrevLogTerm  int   //term of PrevLogIndex
     LeaderCommit int   // commit index of leader
     Entries      []Log //log entries to be stored
}

//Object containing the structure of replies from AppendEntries RPCs

type AppendEntriesReply struct {
     FollowerId  int
     Term        int  //CurrentTerm, used by leader to update itself
     Success     bool //used when checking if logs are consistent 
     NextIndex   int  //possibly used as PrevLogIndex in future AppendEntries RPCs
}

//Object containing args for InstallSnapshot RPCs

type InstallSnapshotArgs struct {
     LeaderId int
     Term     int //Term of leader

     LastIncludedIndex int //Snapshots replace all entries up to and including this index
     LastIncludedTerm  int //Term of LastIncludedIndex
     Snapshot          []byte //Snapshot data
}

//Object containing the structure of replies from InstallSnapshot RPCs

type InstallSnapshotReply struct {
     Success    bool //tool for checking if the RPC was successful in installing the snapshot

     FollowerId int
     Term       int //CurrentTerm, from which leader can update itself to make sure its concurrent 
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

//InstallSnapshot RPC sender

func (rf* Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs,reply *InstallSnapshotReply) bool {
      return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

//If server is not on given currentTerm, resets it to not having voted for a leader and updates it to the currentTerm

func (rf *Raft) setCurrentTerm(currentTerm int) {
     if currentTerm != rf.CurrentTerm {
        rf.VotedFor = -1
        }
        rf.CurrentTerm = currentTerm
}

//If server is not currently a follower demotes it to follower state

func (rf *Raft) Demote() {
     if rf.state != "Follower" {
        rf.state = "Follower"
        select {
                case rf.DemoteCh <- struct{}{}:
                default:
        }
     }
}

//Returns the index and term of the last log

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
	
	//Reject request if Candidate's logs are behind receiver's logs
        lastLogIndex, lastLogTerm := rf.lastLogIndex()
        if lastLogTerm > args.LastLogTerm || (lastLogTerm ==args.LastLogTerm && lastLogIndex > args.LastLogIndex) {

           reply.VoteGranted = false
           reply.Term = rf.CurrentTerm
           return
        }

        //Since receiver has vetted candidate, if it has yet to vote for anyone, vote for candidate
        if rf.VotedFor == -1 {
           rf.VotedFor = args.CandidateId
           reply.VoteGranted = true
	
	//Otherwise check to see if it has voted for candidate by comparing ID of who it voted for to candidate ID
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

     // If leader has stale term, reject request

     if args.Term < rf.CurrentTerm {
        reply.Success = false
        return
     }

     //If this is a heartbeat, send it into heartbeat channel
     if args.IsHeartBeat {
	//Heartbeat allows leader to concurrently check followers' logs and let them know it is still alive
	//(lightweight and concurrent through goroutine)
        go func() {
           var heartBeat HeartBeat
           heartBeat.leaderId = args.LeaderId
           heartBeat.term = args.Term
           heartBeat.leaderCommit = args.LeaderCommit
           rf.heartBeatCh <- heartBeat
        }()
	     
     //If log ends up being inconsistent, returns false but allows leader to retry
     ok, nextIndex := rf.LogIntegrity(args)
     reply.Success = ok
     reply.NextIndex = nextIndex

     } else {
	     
	//Same process as with heartbeat, checks if logs are consistent and if not returns false but lets leader retry
        ok, nextIndex := rf.LogIntegrity(args)
        if !ok {
           reply.Success = false
           reply.NextIndex = nextIndex
           return
        }
        if len(args.Entries) != 0 {
	    
	    //If there are logs, get rid of the bad ones and append new ones
            different := false //Marker of whether there are inconsistent logs
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
	   //If the logs are inconsistent update them to match
           if different && rf.startIndex +  len(rf.log) > args.PrevLogIndex + len(args.Entries) + 1 {
               rf.log = rf.log[0 : args.PrevLogIndex + len(args.Entries) + 1 - rf.startIndex]
           }
       }
       reply.Success = true
	     
       //Update commit index to match
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

//discard previous log entries (those before lastlogindex)
//Used by cleanup service to ensure Raft does not exceed space limitations 
//by discarding committed logs after they are executed

func (rf *Raft) TrashLogs(lastLogIndex int, lastLogTerm int) {
     rf.mu.Lock()
     defer rf.mu.Unlock()
     defer rf.persist()

     if rf.commitIndex < lastLogIndex {
        rf.commitIndex = lastLogIndex
     }

     //prepares to discard logs up to the last log
     discardFrom := lastLogIndex - rf.startIndex

     //Cannot discard if lastlog is same or ahead of current log
     if discardFrom <= 0 {
        return
     } else if discardFrom >= len(rf.log) {
       //Commit index is too ahead, remake log with length 1 using the most recent log info
       rf.startIndex = lastLogIndex
       rf.log = make([]Log, 1)
       rf.log[0].Index = lastLogIndex
       rf.log[0].Term = lastLogTerm
     } else {
       //Otherwise, discard all old logs
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

//Handles InstallSnapshot RPCs

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
     rf.mu.Lock()
     defer rf.mu.Unlock()
     defer rf.persist()

     rf.checkStaleLeader("InstallSnapshot", args.Term, args.LeaderId)
     reply.FollowerId = rf.me
     reply.Term = rf.CurrentTerm
     reply.Success = false

     // if leader's term is stale reject request
     if args.Term < rf.CurrentTerm {
        return
     }

     reply.Success = true
	
     //If the snapshot itself is stale reject request	
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

//Checks for log consistency by cross-referencing the entry at PrevLogIndex with PrevLogTerm 
//(the two should match up)

func (rf *Raft) LogIntegrity(args AppendEntriesArgs) (bool, int) {
     lastLogIndex, _ := rf.lastLogIndex()
     if lastLogIndex < args.PrevLogIndex {
	//logs are missing
        return false, lastLogIndex
     }
     if args.PrevLogIndex <= rf.commitIndex {
	//committed log has to be consistent, as the last log recognized by AppendEntries is either the same as it
	//or precedes
        return true, -1
     }
     if rf.log[args.PrevLogIndex - rf.startIndex].Term != args.PrevLogTerm {
     // Skip all logs whose term == rf.log[args.PrevLogIndex].Term
	 
        matchIndex := args.PrevLogIndex - 1
        for ; matchIndex > rf.startIndex; matchIndex-- {
            if rf.log[args.PrevLogIndex - rf.startIndex].Term != rf.log[matchIndex - rf.startIndex].Term {
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
     //randomize election timer between 250ms and 300ms.
     return time.Duration(250 + rand.Intn(50)) * time.Millisecond
}

//Loop executed by machines in the follower state

func (rf *Raft) FLoop() {	
     rf.mu.Lock()
     rf.state = "Follower"
     rf.mu.Unlock()

     timer := time.NewTimer(ElectionTimer())

     for {
         select {
		 
		//if machine has been killed, end loop
                case <-rf.killCh: {
                     return
                }
		 
		//Follower has received heartbeat
                case heartBeat := <-rf.heartBeatCh: {
                     rf.mu.Lock()
                     if heartBeat.term < rf.CurrentTerm {
		     //The leader is a stale leader, ignore and let the next time's firing handles it
                     } else {
                       rf.setCurrentTerm(heartBeat.term)
                       rf.persist()
                       timer.Reset(ElectionTimer())
                     }
                     rf.mu.Unlock()
                }
                case <-timer.C: {
		     //The timer has run out and the follower has not heard from the leader
		     //we assume this means the leader is down, so the follower will convert to a candidate
		     //and begin a new election
			
                     go rf.CLoop()
                     return
                }
         }
      }
}

//Loop executed by machines in the candidate state (run during elections)

func (rf *Raft) CLoop() {
     rf.mu.Lock()
     rf.state = "Candidate"
     rf.mu.Unlock()

     for {
	     
	 //begins election by incrementing current term by one and voting for itself
         rf.mu.Lock()
         rf.setCurrentTerm(rf.CurrentTerm + 1)
         rf.VotedFor = rf.me
         rf.persist()
         rf.mu.Unlock()
	     
	 //Goes out and concurrently asks each other server to vote for it
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
//Starts with one vote because it voted for itself
votes :=1
	     
//Next process (only starts after response from each other server)
Other:
        for {
            select {
		   
		   //If this server has been killed, exit the function
                   case <-rf.killCh: {
                        return
                   }
		    
		   //If this server is demoted, convert to follower state
                   case <- rf.DemoteCh: {
                        go rf.FLoop()
                        return
                   }
		    
		   //Server has received replies to its request for votes and must check what they are
                   case reply := <-replies: {
                        rf.mu.Lock()
                        leave := false
			   
			//If this server is stale, catch up and then convert to follower state
                        if rf.checkStaleLeader("CandidateLoop", reply.Term,reply.FollowerId) {
                           rf.persist()
                           go rf.FLoop()
                           leave = true
				
                        } else if reply.VoteGranted {
			   //give server its well-earned vote
                           votes += 1
                           if (votes > len(rf.peers)/2) {
			      
			      //counts after every vote gained to see if it has a majority. If so,
			      //this candidate has won the election! it now becomes the leader
                              go rf.LLoop()
                              leave = true
                           }
                        }
                        rf.mu.Unlock()
                        if leave {
                           return
                        }
                   }

		   //Election is over even though no one appears to have won as time has run out
                   case <- time.After(ElectionTimer()): {
                        break Other
                   }
             }
        }
   }
}

//Called by leader to sync logs to machines in follower state
//The actual synchronization process is run in a separate goroutine in the leader loop
func (rf *Raft) LogsSync(server int) {
     for {
         rf.mu.Lock()
         lostLeadership := rf.state != "Leader"
         lastLogIndex, _ := rf.lastLogIndex()
         sendSnapshot := false

	 //Send a snapshot if the log meant to be replicated is discarded
         if rf.nextIndex[server] <= rf.startIndex {
            sendSnapshot = true
         }
         var argsAppend AppendEntriesArgs
         var replyAppend AppendEntriesReply
         var argsSnapshot InstallSnapshotArgs
         var replySnapshot InstallSnapshotReply

	 //check leadership at this point as nextindex will most likely be wrong if leadership was lost
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

	       //append each log entry from the one after prevlogindex to the very last log to entries
               for j := argsAppend.PrevLogIndex + 1; j <= lastLogIndex; j++{
                   argsAppend.Entries = append(argsAppend.Entries, rf.log[j- rf.startIndex])
               }
           }
        }

         rf.mu.Unlock()
	    
	//Exit if leadership was lost
        if lostLeadership {
           return
        }

        var repliedTerm int

	//Send either InstallSnapshot or AppendEntries, depepnding on what was specified, to sync the logs
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

	//Update nextIndex and matchIndex for server.
        setLastIndex := func(index int) {
             if rf.nextIndex[server] < index + 1 {
                 rf.nextIndex[server] = index + 1
             }
             if rf.matchIndex[server] < index {
                 rf.matchIndex[server] = index
             }
        }

	//Check for successful reply from either InstallSnapshot or Appendentries and set the last index based on which was used
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

//Loop executed by machines in the leader state

func (rf *Raft) LLoop() {
     rf.mu.Lock()

     rf.state = "Leader"
	
     //reinitialize nextindex and matchindex in case there are now more or less active servers
     rf.nextIndex = make([]int, len(rf.peers))
     rf.matchIndex = make([]int, len(rf.peers))
     lastLogIndex, _ := rf.lastLogIndex()
     for i := 0; i < len(rf.nextIndex); i++ {
         rf.nextIndex[i] = lastLogIndex + 1
     }

     rf.mu.Unlock()

     // Try AppendEntries asynchronously if followers miss some.
     for i := range rf.peers {
         if i != rf.me {
            go rf.LogsSync(i)
         }
     }

     for {
	 //first update commitIndex
         rf.mu.Lock()
         newCommitIndex := rf.commitIndex

	 //check to see how many of the followers have replicated logs
         for c := rf.startIndex + len(rf.log) - 1; c >rf.commitIndex; c-- {
             if rf.log[c - rf.startIndex].Term == rf.CurrentTerm {
                
		count := 1
                for i := range rf.peers {
                    if i == rf.me {
                       continue
                    }
			
		    //increment count by 1 for each follower that has replicated the logs
                    if rf.matchIndex[i] >= c {
                       count += 1
                    }
                }
		
		//Escape if more than half have replicated, we can now commit
                if count > len(rf.peers) / 2 {
                      newCommitIndex = c
                    break
                }
             }
          }

          if newCommitIndex != rf.commitIndex {
  	      // Log is replicated to majority, commit log on leader.
              for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {

                  var applyMsg ApplyMsg
                  applyMsg.Index = i
                  applyMsg.Term = rf.log[i - rf.startIndex].Term
                  applyMsg.Command = rf.log[i - rf.startIndex].Command
                  applyMsg.UseSnapshot = false
                  rf.applyCh <- applyMsg
              }

              rf.commitIndex = newCommitIndex
	      // Also let each follower commit
              for i := range rf.peers {
                  if i != rf.me {
                     go rf.LogsSync(i)
                  }
              }
          }
            rf.mu.Unlock()

	  //Send out heartbeat
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

		 // Check leadership here as nextIndex may not be valid if lose leadership
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

		 // Synchronize logs if inconsistent or missing
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
		    
		//This server has been killed, exit
                case <-rf.killCh: {
                     return
                }
		    
		//This server has been demoted to follower state, enter follower loop and exit
                case <-rf.DemoteCh: {
                     go rf.FLoop()
                     return
                }

		//Heartbeats are broadcast every 100 msecs. That amount of time has passed so we start another round of heartbeats
                case <-time.After(time.Duration(100) * time.Millisecond): {
                     break Other
                }

		//We hear a reply to our heartbeat from a follower
                case reply := <-replies: {
                rf.mu.Lock()

                leave := false
			
		//Check to see if we are stale. If so, convert to follower state and exit
                if rf.checkStaleLeader("Heartbeat", reply.Term, reply.FollowerId) {
                     rf.persist()
                     go rf.FLoop()
                     leave = true
                } else {
		//Leader has successfully received acknowledgement of heartbeat from follower
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

//
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
//
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
















