#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <time.h> 
#include <set>
#include <vector>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template<typename command>
class persistent_log {
private:
    std::vector<log_entry<command>> in_mem_log;
    size_t start_idx;
    log_entry<command> error_entry;

public:
persistent_log(): start_idx(0) {}

log_entry<command> &operator[](size_t idx) {
    if (idx < start_idx) {
        printf("try to access log[%ld], which is already in snapshot", idx);
        return error_entry;
    }
    return in_mem_log[idx - start_idx];
}

size_t size() {
    return in_mem_log.size() + start_idx;
}

void append(log_entry<command> &entry) {
    in_mem_log.push_back(entry);
}
};


template<typename state_machine, typename command>
class raft {

static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");


friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        auto now = \
        std::chrono::duration_cast<std::chrono::milliseconds>(\
            std::chrono::system_clock::now().time_since_epoch()\
        ).count();\
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while(0);

public:
    raft(
        rpcs* rpc_server,
        std::vector<rpcc*> rpc_clients,
        int idx, 
        raft_storage<command>* storage,
        state_machine* state    
    );
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node. 
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped(). 
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false. 
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool* thread_pool;
    raft_storage<command>* storage;              // To persist the raft log
    state_machine* state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs* rpc_server;               // RPC server to recieve and handle the RPC requests
    std::vector<rpcc*> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                     // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;

    std::thread* background_election;
    std::thread* background_ping;
    std::thread* background_commit;
    std::thread* background_apply;

    // Your code here:

    int heartbeat_timeout;
    int election_timeout;

    // persistent states
    // current candidate it is voting for, -1 means null
    int vote_for;
    int current_term;
    persistent_log<command> log;

    // violate states
    int commit_idx;
    int last_applied;
    std::set<int> vote_for_me;
    std::chrono::system_clock::time_point last_received_heartbeat_time;
    std::chrono::system_clock::time_point election_start_time;

    // violate states for leader
    std::vector<int> next_idx;
    std::vector<int> match_idx;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply& reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply& reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply& reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply);


private:
    bool is_stopped();
    int num_nodes() {return rpc_clients.size();}

    // background workers    
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:

    void set_current_term(int);
    void set_vote_for(int);
    void persist_log();

};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs* server, std::vector<rpcc*> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),   
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    role(follower),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr),
    heartbeat_timeout(500),
    current_term(0)
{
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here: 
    // Do the initialization
    srand(time(0));
    // generate randomly between 300 to 500
    election_timeout = rand() % 200 + 300;
}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;    
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    mtx.lock();
    term = current_term;
    bool is_leader = role == leader;
    mtx.unlock();
    return is_leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Your code here:
    last_received_heartbeat_time = std::chrono::system_clock::now();
    
    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:

    term = current_term;
    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    return true;
}



/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply& reply) {
    // Your code here:
    mtx.lock();

    reply.term = current_term;

    if (args.term < current_term) {
        mtx.unlock();
        reply.vote_granted = false;
        return 0;
    }
    
    last_received_heartbeat_time = std::chrono::system_clock::now();
    int current_log_idx = log.size() - 1;
    int current_log_term = current_log_idx < 0 ? -1 : log[current_log_idx].term;

    if (args.term > current_term) {
        set_current_term(args.term);
        role = raft_role::follower;
        set_vote_for(-1);
        reply.term = current_term;
    }
    
    if (vote_for != -1 && vote_for != args.candidate_id) {
        mtx.unlock();
        reply.vote_granted = false;
        return 0;
    }

    if (current_log_term > args.last_log_term) {
        mtx.unlock();
        reply.vote_granted = false;
        return 0;
    }

    if (current_log_term < args.last_log_term || current_log_idx <= args.last_log_index) {
        set_vote_for(args.candidate_id);
        mtx.unlock();
        reply.vote_granted = true;
        return 0;
    }

    mtx.unlock();
    reply.vote_granted = false;
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    // Your code here:
    if (reply.vote_granted)
        printf("%d vote for %d in term %d\n", target, my_id, arg.term);
        
    if (role != raft_role::candidate || arg.term < current_term) {
        return;
    }

    if (reply.vote_granted) {
        mtx.lock();
        vote_for_me.insert(target);
        if (vote_for_me.size() > rpc_clients.size() / 2) {
            printf("%d become leader in term %d\n", my_id, current_term);
            role = raft_role::leader;
        }
        mtx.unlock();
    }
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    // Your code here:
    mtx.lock();
    printf("%d receive heartbeat from %d at term %d\n", my_id, arg.leader_id, current_term);
    
    reply.term = current_term;

    if (arg.term < current_term) {
        mtx.unlock();
        reply.success = false;
        return 0;
    }

    last_received_heartbeat_time = std::chrono::system_clock::now();
    if (arg.leader_id != my_id) {
        role = raft_role::follower;
    }

    if (arg.term > current_term) {
        set_current_term(arg.term);
        role = raft_role::follower;
    }

    mtx.unlock();
    reply.success = true;
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    // Your code here:
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply& reply) {
    // Your code here:
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply) {
    // Your code here:
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g. 1s).

    
    while (true) {
        if (is_stopped()) return;
        // Your code here:

        mtx.lock();
        if (role == raft_role::follower) {
            std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
            int time = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_received_heartbeat_time).count();
            if (time >= heartbeat_timeout) {
                printf("%d start election\n", my_id);
                role = raft_role::candidate;
                ++current_term;
                vote_for_me.clear();
                set_vote_for(-1);
                election_start_time = std::chrono::system_clock::now();

                int current_log_idx = log.size() - 1;
                int current_log_term = current_log_idx < 0 ? -1 : log[current_log_idx].term;

                request_vote_args args(current_term, my_id, current_log_idx, current_log_term);
                mtx.unlock();

                int server_number = rpc_clients.size();
                for (int i = 0; i < server_number; ++i) {
                    thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
                }
            } else {
                mtx.unlock();
            }
        } else if (role == raft_role::candidate) {
            std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
            int time = std::chrono::duration_cast<std::chrono::milliseconds>(now - election_start_time).count();
            if (time >= election_timeout) {
                printf("%d quit election\n", my_id);
                role = raft_role::follower;
            }
            mtx.unlock();
        } else {
            mtx.unlock();
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.        
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:

        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    // Hints: You should check the commit index and the apply index.
    //        Update the apply index and apply the log if commit_index > apply_index

    
    while (true) {
        if (is_stopped()) return;
        // Your code here:

        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:

        mtx.lock();
        if (role == raft_role::leader) {
            int server_number = rpc_clients.size();
            int prev_log_idx = log.size() - 1;
            int prev_log_term = prev_log_idx < 0 ? -1 : log[prev_log_idx].term;
            append_entries_args<command> args(current_term, my_id, prev_log_idx, prev_log_term,
                commit_idx, std::vector<log_entry<command>>());
            mtx.unlock();
            for (int i = 0; i < server_number; ++i) {
                // printf("%d sending heartbeat to %d\n", my_id, i);
                thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
            }
        } else {
            mtx.unlock();
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(100)); // Change the timeout here!
    }    
    return;
}


/******************************************************************

                        Other functions

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::set_current_term(int _current_term) {
    current_term = _current_term;
    // TODO: persist current_term in raft_storage

}

template<typename state_machine, typename command>
void raft<state_machine, command>::set_vote_for(int _vote_for) {
    vote_for = _vote_for;
    // TODO: persist vote_for in raft_storage

}

template<typename state_machine, typename command>
void raft<state_machine, command>::persist_log() {
    // TODO: persist log in raft_storage

}

#endif // raft_h