#include "rpc.h"
#include "raft_state_machine.h"
#include "common.h"


using shard_dispatch = int (*)(int key, int shard_num);

class chdb_command : public raft_command {
public:
    enum command_type {
        CMD_NONE = 0xdead,   // Do nothing
        CMD_GET,        // Get a key-value pair
        CMD_PUT,        // Put a key-value pair
        TX_BEGIN,
        TX_PREPARE,
        TX_COMMIT,
        TX_ABORT
    };

    // TODO: You may add more fields for implementation.
    struct result {
        std::chrono::system_clock::time_point start;
        int key, value, tx_id;
        command_type tp;

        bool done;
        std::mutex mtx; // protect the struct
        std::condition_variable cv; // notify the caller
    };

    chdb_command();

    chdb_command(command_type tp, const int &key, const int &value, const int &tx_id);

    chdb_command(const chdb_command &cmd);

    virtual ~chdb_command() {}

    command_type cmd_tp;
    int key, value, tx_id, should_send_rpc;
    std::shared_ptr<result> res;

    virtual int size() const override;

    virtual void serialize(char *buf, int size) const override;

    virtual void deserialize(const char *buf, int size);
};

marshall &operator<<(marshall &m, const chdb_command &cmd);

unmarshall &operator>>(unmarshall &u, chdb_command &cmd);

class chdb_state_machine : public raft_state_machine {
private:
    std::vector<chdb_command> log;
    std::vector<chdb_command> get_tx_log(int tx_id);
    std::set<int> get_shard_offset(int tx_id);

    int shard_num() const {
        return this->node->rpc_clients.size();
    }
public:
    int base_port;
    rpc_node *node;
    shard_dispatch dispatch;
    chdb_state_machine() {}
    chdb_state_machine(int base_port, rpc_node *node , shard_dispatch dispatch):
        base_port(base_port),
        node(node),
        dispatch(dispatch) {}
    virtual ~chdb_state_machine() {}

    // Apply a log to the state machine.
    // TODO: Implement this function.
    virtual void apply_log(raft_command &cmd) override;

    // Generate a snapshot of the current state.
    // In Chdb, you don't need to implement this function
    virtual std::vector<char> snapshot() {
        return std::vector<char>();
    }

    // Apply the snapshot to the state mahine.
    // In Chdb, you don't need to implement this function
    virtual void apply_snapshot(const std::vector<char> &) {}
};