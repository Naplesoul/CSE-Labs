#include "ch_db.h"

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::operation_var &var, int &r) {
    // TODO: Your code here
    if (proc == chdb_protocol::Get) {
        chdb_command cmd(chdb_command::CMD_GET, var.key, var.value, var.tx_id);
        append_log(cmd);
        
        std::unique_lock<std::mutex> lock(cmd.res->mtx);
        if (!cmd.res->done) {
            ASSERT(cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2500)) == std::cv_status::no_timeout,
            "GET command timeout");
        }
        r = cmd.res->value;
    } else if (proc == chdb_protocol::Put) {
        chdb_command cmd(chdb_command::CMD_PUT, var.key, var.value, var.tx_id);
        append_log(cmd);
        std::unique_lock<std::mutex> lock(cmd.res->mtx);
        if (!cmd.res->done) {
            ASSERT(cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2500)) == std::cv_status::no_timeout,
            "GET command timeout");
        }
        r = cmd.res->value;
    } else {
        assert(0);
    }

    return 0;
}

chdb_protocol::prepare_state view_server::tx_can_commit(int tx_id) {
    chdb_command cmd(chdb_command::TX_PREPARE, 0, 0, tx_id);
    append_log(cmd);
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if (!cmd.res->done) {
        if (cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2500)) != std::cv_status::no_timeout) {
            printf("PREPARE command timeout, retry...\n");

            append_log(cmd);
            ASSERT(cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2500)) == std::cv_status::no_timeout,
            "PREPARE command timeout, failed!");
        }
    }
    return chdb_protocol::prepare_state(cmd.res->value);
}

int view_server::tx_begin(int tx_id) {
    chdb_command cmd(chdb_command::TX_BEGIN, 0, 0, tx_id);
    append_log(cmd);
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if (!cmd.res->done) {
        ASSERT(cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2500)) == std::cv_status::no_timeout,
        "BEGIN command timeout");
    }
    return cmd.res->value;
}

int view_server::tx_commit(int tx_id) {
    chdb_command cmd(chdb_command::TX_COMMIT, 0, 0, tx_id);
    append_log(cmd);
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if (!cmd.res->done) {
        ASSERT(cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2500)) == std::cv_status::no_timeout,
        "COMMIT command timeout");
    }
    return cmd.res->value;
}

int view_server::tx_abort(int tx_id) {
    chdb_command cmd(chdb_command::TX_ABORT, 0, 0, tx_id);
    append_log(cmd);
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if (!cmd.res->done) {
        ASSERT(cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2500)) == std::cv_status::no_timeout,
        "COMMIT command timeout");
    }
    return cmd.res->value;
}

int view_server::tx_rollback(int tx_id) {
    chdb_command cmd(chdb_command::TX_ROLLBACK, 0, 0, tx_id);
    append_log(cmd);
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if (!cmd.res->done) {
        ASSERT(cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2500)) == std::cv_status::no_timeout,
        "ROLLBACK command timeout");
    }
    return cmd.res->value;
}

void view_server::append_log(chdb_command &entry) {
    entry.should_send_rpc = 1;
    int leader = raft_group->check_exact_one_leader();
    int term, index;
    printf("append log cmd_ty: %d, key: %d, val: %d, tx_id: %d\n", entry.cmd_tp, entry.key, entry.value, entry.tx_id);
    while (!raft_group->nodes[leader]->new_command(entry, term, index)) {
        leader = raft_group->check_exact_one_leader();
    }
}

view_server::lock_state view_server::aquire_lock(int key, int tx_id) {
    // use wait-die algorithm to solve dead lock
    // tx_id is used to compare the begining time of each tx
    mtx.lock();
    auto lock = locks.find(key);

    if (lock == locks.end()) {
        auto new_lock = locks.emplace(key, tx_id);
        assert(new_lock.first->second.lock.try_lock());
        mtx.unlock();
        return lock_ok;
    }

    if (lock->second.lock.try_lock()) {
        mtx.unlock();
        lock->second.time = tx_id;
        return lock_ok;
    }

    if (lock->second.time < tx_id) {
        // the resourse is locked and current tx is younger, die
        mtx.unlock();
        return lock_die;
    }

    // the resourse is locked and current tx is older, wait
    mtx.unlock();
    lock->second.lock.lock();
    lock->second.time = tx_id;
    return lock_ok;
}

void view_server::release_lock(int key) {
    auto lock = locks.find(key);
    if (lock == locks.end()) {
        assert(0);
    }
    lock->second.lock.unlock();
}

view_server::~view_server() {
#if RAFT_GROUP
    delete this->raft_group;
#endif
    delete this->node;
}