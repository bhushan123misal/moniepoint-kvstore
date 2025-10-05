from dataclasses import dataclass
from typing import List, Optional


@dataclass
class LogEntry:
    index: int
    term: int
    op: int
    key: str
    value: Optional[str]

@dataclass
class RequestVoteArgs:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

@dataclass
class RequestVoteRes:
    term: int
    vote_granted: bool

@dataclass
class AppendEntriesArgs:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int

@dataclass
class AppendEntriesRes:
    term: int
    success: bool
    match_index: int
