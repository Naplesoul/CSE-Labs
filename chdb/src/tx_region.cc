#include "tx_region.h"


int tx_region::put(const int key, const int val) {
    // TODO: Your code here
    int r;
    if (locked_keys.find(key) == locked_keys.end()) {
        while (this->db->vserver->aquire_lock(key, tx_id) != view_server::lock_ok) {
            rollback_redo();
        }
        locked_keys.insert(key);
    }
    log.emplace_back(key, val);
    this->db->vserver->execute(key,
                               chdb_protocol::Put,
                               chdb_protocol::operation_var(tx_id, key, val),
                               r);
    return r;
}

int tx_region::get(const int key) {
    // TODO: Your code here
    int r;
    if (locked_keys.find(key) == locked_keys.end()) {
        while (this->db->vserver->aquire_lock(key, tx_id) != view_server::lock_ok) {
            rollback_redo();
        }
        locked_keys.insert(key);
    }
    this->db->vserver->execute(key,
                               chdb_protocol::Get,
                               chdb_protocol::operation_var(tx_id, key, 0),
                               r);
    return r;
}

int tx_region::tx_can_commit() {
    // TODO: Your code here
    return this->db->vserver->tx_can_commit(tx_id);
}

int tx_region::tx_begin() {
    // TODO: Your code here
    printf("tx[%d] begin\n", tx_id);
    return this->db->vserver->tx_begin(tx_id);
}

int tx_region::tx_commit() {
    // TODO: Your code here
    printf("tx[%d] commit\n", tx_id);
    return this->db->vserver->tx_commit(tx_id);
}

int tx_region::tx_abort() {
    // TODO: Your code here
    printf("tx[%d] abort\n", tx_id);
    return this->db->vserver->tx_abort(tx_id);
}

int tx_region::rollback_redo() {
    bool should_rollback_redo = true;
    while (should_rollback_redo) {
        should_rollback_redo = false;
        if (log.size() > 0) this->db->vserver->tx_rollback(tx_id);
        release_locks();
        for (auto &entry : log) {
            if (this->db->vserver->aquire_lock(entry.key, tx_id) != view_server::lock_ok) {
                should_rollback_redo = true;
                break;
            }
        }
    }
    
    return 0;
}

void tx_region::release_locks() {
    for (int key : locked_keys) {
        this->db->vserver->release_lock(key);
    }
}