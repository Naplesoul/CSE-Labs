#include "raft_protocol.h"

marshall& operator<<(marshall &m, const request_vote_args& args) {
    // Your code here
    m << args.term << args.candidate_id << args.last_log_index << args.last_log_term;
    return m;

}
unmarshall& operator>>(unmarshall &u, request_vote_args& args) {
    // Your code here
    u >> args.term >> args.candidate_id >> args.last_log_index >> args.last_log_term;
    return u;
}

marshall& operator<<(marshall &m, const request_vote_reply& reply) {
    // Your code here
    m << reply.term << reply.vote_granted;
    return m;
}

unmarshall& operator>>(unmarshall &u, request_vote_reply& reply) {
    // Your code here
    u >> reply.term >> reply.vote_granted;
    return u;
}

marshall& operator<<(marshall &m, const append_entries_reply& reply) {
    // Your code here
    m << reply.term << reply.success;
    return m;
}
unmarshall& operator>>(unmarshall &u, append_entries_reply& reply) {
    // Your code here
    u >> reply.term >> reply.success;
    return u;
}

marshall& operator<<(marshall &m, const install_snapshot_args& args) {
    // Your code here
    m << args.term << args.leader_id << args.last_included_idx << args.last_included_term << args.data;
    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_args& args) {
    // Your code here
    u >> args.term >> args.leader_id >> args.last_included_idx >> args.last_included_term >> args.data;
    return u; 
}

marshall& operator<<(marshall &m, const install_snapshot_reply& reply) {
    // Your code here
    m << reply.term;
    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_reply& reply) {
    // Your code here
    u >> reply.term;
    return u;
}