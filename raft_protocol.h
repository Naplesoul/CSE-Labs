#ifndef raft_protocol_h
#define raft_protocol_h

#include <vector>

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
   OK,
   RETRY,
   RPCERR,
   NOENT,
   IOERR
};

class request_vote_args {
public:
    // Your code here
    int term;
    int candidate_id;
    int last_log_index;
    int last_log_term;

    request_vote_args() {}
    request_vote_args(int _term, int _candidate_id, int _last_log_index, int _last_log_term):
        term(_term), candidate_id(_candidate_id), last_log_index(_last_log_index), last_log_term(_last_log_term) {}
};

marshall& operator<<(marshall &m, const request_vote_args& args);
unmarshall& operator>>(unmarshall &u, request_vote_args& args);


class request_vote_reply {
public:
    // Your code here
    int term;
    bool vote_granted;
};

marshall& operator<<(marshall &m, const request_vote_reply& reply);
unmarshall& operator>>(unmarshall &u, request_vote_reply& reply);

template<typename command>
class log_entry {
public:
    int term;
    command cmd;

    log_entry(): term(-1) {}
    log_entry(int _term, command _cmd): term(_term), cmd(_cmd) {}
};

template<typename command>
marshall& operator<<(marshall &m, const log_entry<command>& entry) {
    // Your code here
    char buf[256];
    memset(buf, 0, 256);
    entry.cmd.serialize(buf, 256);
    std::string command_str(buf);
    m << entry.term << command_str;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, log_entry<command>& entry) {
    // Your code here
    std::string command_str;
    u >> entry.term >> command_str;
    entry.cmd.deserialize(command_str.c_str(), command_str.size());
    return u;
}

template<typename command>
class append_entries_args {
public:
    // Your code here
    int term;
    int leader_id;
    int prev_log_idx;
    int prev_log_term;
    int leader_commit;
    std::vector<log_entry<command>> entries;

    append_entries_args() {}
    append_entries_args(int _term, int _leader_id, int _prev_log_idx, int _prev_log_term,
        int _leader_commit, std::vector<log_entry<command>> _entries):
        term(_term),
        leader_id(_leader_id),
        prev_log_idx(_prev_log_idx),
        prev_log_term(_prev_log_term),
        leader_commit(_leader_commit),
        entries(_entries) {}
};

template<typename command>
marshall& operator<<(marshall &m, const append_entries_args<command>& args) {
    // Your code here
    m << args.term << args.leader_id << args.prev_log_idx
        << args.prev_log_term << args.leader_commit << args.entries;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, append_entries_args<command>& args) {
    // Your code here
    u >> args.term >> args.leader_id >> args.prev_log_idx
        >> args.prev_log_term >> args.leader_commit >> args.entries;
    return u;
}

class append_entries_reply {
public:
    // Your code here
    int term;
    bool success;
};

marshall& operator<<(marshall &m, const append_entries_reply& reply);
unmarshall& operator>>(unmarshall &u, append_entries_reply& reply);


class install_snapshot_args {
public:
    // Your code here
};

marshall& operator<<(marshall &m, const install_snapshot_args& args);
unmarshall& operator>>(unmarshall &m, install_snapshot_args& args);


class install_snapshot_reply {
public:
    // Your code here
};

marshall& operator<<(marshall &m, const install_snapshot_reply& reply);
unmarshall& operator>>(unmarshall &m, install_snapshot_reply& reply);


#endif // raft_protocol_h