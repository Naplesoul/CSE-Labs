#include "chdb_state_machine.h"
#include "protocol.h"

chdb_command::chdb_command(): chdb_command(chdb_command::CMD_NONE, 0, 0, 0) {
    // TODO: Your code here
}

chdb_command::chdb_command(command_type tp, const int &key, const int &value, const int &tx_id)
        : cmd_tp(tp), key(key), value(value), tx_id(tx_id), should_send_rpc(0), res(std::make_shared<result>()) {
    // TODO: Your code here
    res->start = std::chrono::system_clock::now();
    res->key = key;
    res->tx_id = tx_id;
    res->tp = tp;
    res->done = false;
}

chdb_command::chdb_command(const chdb_command &cmd) :
        cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), tx_id(cmd.tx_id), should_send_rpc(cmd.should_send_rpc), res(cmd.res) {
    // TODO: Your code here
}

int chdb_command::size() const {
    return sizeof(*this);
}

void chdb_command::serialize(char *buf, int size) const {
    // TODO: Your code here
    assert(size >= this->size());
    *((command_type *)buf) = cmd_tp;
    *((int *)(buf + sizeof(command_type))) = key;
    *((int *)(buf + sizeof(command_type) + sizeof(int))) = value;
    *((int *)(buf + sizeof(command_type) + 2 * sizeof(int))) = tx_id;
    *((int *)(buf + sizeof(command_type) + 3 * sizeof(int))) = should_send_rpc;
}

void chdb_command::deserialize(const char *buf, int size) {
    // TODO: Your code here
    cmd_tp = *((command_type *)buf);
    key = *((int *)(buf + sizeof(command_type)));
    value = *((int *)(buf + sizeof(command_type) + sizeof(int)));
    tx_id = *((int *)(buf + sizeof(command_type) + 2 * sizeof(int)));
    should_send_rpc = *((int *)(buf + sizeof(command_type) + 3 * sizeof(int)));
}

marshall &operator<<(marshall &m, const chdb_command &cmd) {
    // TODO: Your code here
    m << (int)cmd.cmd_tp << cmd.key << cmd.value;
    return m;
}

unmarshall &operator>>(unmarshall &u, chdb_command &cmd) {
    // TODO: Your code here
    int cmd_tp;
    u >> cmd_tp >> cmd.key >> cmd.value;
    cmd.cmd_tp = (chdb_command::command_type)cmd_tp;
    cmd.should_send_rpc = 0;
    return u;
}

void chdb_state_machine::apply_log(raft_command &cmd) {
    // TODO: Your code here
    chdb_command &chdb_cmd = dynamic_cast<chdb_command&>(cmd);
    int shard_offset = dispatch(chdb_cmd.key, shard_num());
    
    if (!chdb_cmd.should_send_rpc) {
        if (chdb_cmd.cmd_tp == chdb_command::CMD_PUT) {
            related_shards[chdb_cmd.tx_id].insert(shard_offset);
        }
        return;
    }
    
    std::unique_lock<std::mutex> lock(chdb_cmd.res->mtx);

    if (chdb_cmd.cmd_tp == chdb_command::CMD_GET) {
        chdb_protocol::operation_var var(chdb_cmd.tx_id, chdb_cmd.key, 0);
        int r = 0;
        this->node->template call(base_port + shard_offset, chdb_protocol::Get, var, r);
        printf("GET %d = %d on shard %d\n", chdb_cmd.key, r, shard_offset);
        chdb_cmd.res->value = r;

    } else if (chdb_cmd.cmd_tp == chdb_command::CMD_PUT) {
        related_shards[chdb_cmd.tx_id].insert(shard_offset);
        chdb_protocol::operation_var var(chdb_cmd.tx_id, chdb_cmd.key, chdb_cmd.value);
        int r = 0;
        this->node->template call(base_port + shard_offset, chdb_protocol::Put, var, r);
        printf("PUT %d = %d on shard %d\n", chdb_cmd.key, chdb_cmd.value, shard_offset);
        chdb_cmd.res->value = r;

    } else if (chdb_cmd.cmd_tp == chdb_command::TX_PREPARE) {
        const std::set<int> &shard_offset = related_shards[chdb_cmd.tx_id];
        int base_port = this->node->port();
        chdb_protocol::prepare_var var;
        var.tx_id = chdb_cmd.tx_id;
        for (auto offset : shard_offset) {
            int r = 0;
            printf("Preparing shard %d\n", offset);
            node->template call(base_port + offset, chdb_protocol::Prepare, var, r);
            if (r != chdb_protocol::prepare_ok) {
                printf("Preparing shard %d failed\n", offset);
                chdb_cmd.res->value = chdb_protocol::prepare_not_ok;
                chdb_cmd.res->done = true;
                chdb_cmd.res->cv.notify_all();
                return;
            }
        }
        printf("Preparing shard succeeded\n");
        chdb_cmd.res->value = chdb_protocol::prepare_ok;

    } else if (chdb_cmd.cmd_tp == chdb_command::TX_BEGIN) {
        printf("TX[%d] begin\n", chdb_cmd.tx_id);

    } else if (chdb_cmd.cmd_tp == chdb_command::TX_COMMIT) {
        const std::set<int> &shard_offset = related_shards[chdb_cmd.tx_id];
        int base_port = this->node->port();
        chdb_protocol::commit_var var;
        var.tx_id = chdb_cmd.tx_id;
        for (auto offset : shard_offset) {
            int r = 0;
            node->template call(base_port + offset, chdb_protocol::Commit, var, r);
            if (r != chdb_protocol::prepare_ok) {
                printf("Unexpected situation: sending commit to shard %d while not prepared\n", offset);
                assert(0);
                chdb_cmd.res->value = -1;
                chdb_cmd.res->done = true;
                chdb_cmd.res->cv.notify_all();
                return;
            }
        }
        printf("TX[%d] committed\n", chdb_cmd.tx_id);
        related_shards.erase(chdb_cmd.tx_id);
        chdb_cmd.res->value = 0;

    } else if (chdb_cmd.cmd_tp == chdb_command::TX_ABORT) {
        const std::set<int> &shard_offset = related_shards[chdb_cmd.tx_id];
        int base_port = this->node->port();
        chdb_protocol::rollback_var var;
        var.tx_id = chdb_cmd.tx_id;
        for (auto offset : shard_offset) {
            int r = 0;
            node->template call(base_port + offset, chdb_protocol::Rollback, var, r);
        }
        printf("TX[%d] aborted\n", chdb_cmd.tx_id);
        related_shards.erase(chdb_cmd.tx_id);
        chdb_cmd.res->value = 0;

    } else if (chdb_cmd.cmd_tp == chdb_command::TX_ROLLBACK) {
        const std::set<int> &shard_offset = related_shards[chdb_cmd.tx_id];
        int base_port = this->node->port();
        chdb_protocol::rollback_var var;
        var.tx_id = chdb_cmd.tx_id;
        for (auto offset : shard_offset) {
            int r = 0;
            node->template call(base_port + offset, chdb_protocol::Rollback, var, r);
        }
        printf("TX[%d] rollbacked\n", chdb_cmd.tx_id);
        related_shards[chdb_cmd.tx_id].clear();
        chdb_cmd.res->value = 0;

    } else {
        assert(0);
    }
    
    chdb_cmd.res->done = true;
    chdb_cmd.res->cv.notify_all();
}