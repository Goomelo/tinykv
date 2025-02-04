// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState,_,err := storage.InitialState()
	if err != nil{
		panic(err)
	}
	firstIndex,_  := storage.FirstIndex()
	lastIndex,_ := storage.LastIndex()
	entrys := make([]pb.Entry,0)
	if firstIndex<= lastIndex {
		entrys,err = storage.Entries(firstIndex, lastIndex+1)
		if err!=nil{
			panic(err)
		}
	}
	return &RaftLog{
		storage:    storage,
		committed:  hardState.Commit,
		applied:    firstIndex - 1,
		stabled:    lastIndex,
		entries: 	entrys,
		firstIndex: firstIndex,
	}

}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 && l.stabled+1 >= l.firstIndex {
		return l.entries[l.stabled-l.firstIndex+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0{
		return l.entries[l.applied - l.firstIndex+1: min(l.committed-l.firstIndex+1,uint64(len(l.entries)))]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var snapLast uint64
	if !IsEmptySnap(l.pendingSnapshot) {
		snapLast = l.pendingSnapshot.Metadata.Index
	}
	if len(l.entries) > 0 {
		return max(l.entries[len(l.entries)-1].Index, snapLast)
	}
	index, _ := l.storage.FirstIndex()
	return max(index-1, snapLast)
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && i >= l.firstIndex {
		if i > l.LastIndex() {
			return 0, ErrUnavailable
		}
		return l.entries[i-l.firstIndex].Term, nil
	}
	term, err := l.storage.Term(i)
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		if i < l.pendingSnapshot.Metadata.Index {
			return term, ErrCompacted
		}
	}
	return term, err
}

//append entry to entries
func (l *RaftLog) appendEntries(entries ...pb.Entry)  {
	l.entries = append(l.entries,entries...)
}

// Entries return entries [lo,hi)
func (l *RaftLog) Entries(lo uint64,hi uint64)  []pb.Entry{
	if lo >= l.firstIndex && hi - l.firstIndex <= uint64(len(l.entries)){
		return l.entries[lo-l.firstIndex: hi -l.firstIndex]
	}
	ents, _ := l.storage.Entries(lo,hi)
	return ents
}

func (l *RaftLog) RemoveEntriesAfter(lo uint64)  {
	l.stabled = min(l.stabled, lo-1)
	if lo-l.firstIndex >= uint64(len(l.entries)) {
		return
	}
	l.entries = l.entries[:lo-l.firstIndex]
}