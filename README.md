# Dylan_Sams_Code_Sample
This is my implementation of the Raft Consensus Algorithm (https://raft.github.io/) in Golang initially for BUCS451: Distributed Systems.

The features implemented in this version are:
* Leader election
* Log replication
* Snapshot and log compaction
* Persistent state for crash recovery

For notes on the general functioning of Raft please consult either the link provided above or Raft.md

This sample also contains the tests used to evaluate our raft environment, found in src/raft/test_test.go. To run from command line, assuming you have golang installed:

$ cd src/raft

$ go test


