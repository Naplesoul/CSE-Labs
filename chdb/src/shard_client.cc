#include "shard_client.h"


int shard_client::put(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here

    // prepare undo log
    undo_log_entry log_entry;
    auto entry = store[primary_replica].find(var.key);
    if (entry == store[primary_replica].end()) {
        log_entry.has_old_val = false;
        r = 0;
        // wirte value
        store[primary_replica][var.key] = value_entry(var.value);
    } else {
        log_entry.has_old_val = true;
        r = entry->second.value;
        log_entry.old_val = r;
        // write value
        entry->second.value = var.value;
    }
    log_entry.key = var.key;
    log_entry.new_val = var.value;

    auto tx_logs = undo_log.find(var.tx_id);
    if (tx_logs == undo_log.end()) {
        undo_log[var.tx_id] = std::list<undo_log_entry>({ log_entry });
    } else {
        tx_logs->second.push_back(log_entry);
    }
    
    return 0;
}

int shard_client::get(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    r = store[primary_replica][var.key].value;
    return 0;
}

int shard_client::commit(chdb_protocol::commit_var var, int &r) {
    // TODO: Your code here
    if (undo_log.find(var.tx_id) != undo_log.end() && !active) {
        r = chdb_protocol::prepare_not_ok;
    } else {
        r = chdb_protocol::prepare_ok;
        replicate_tx(var.tx_id);
        undo_log.erase(var.tx_id);
    }
    return 0;
}

int shard_client::rollback(chdb_protocol::rollback_var var, int &r) {
    // TODO: Your code here
    if (undo_log.find(var.tx_id) == undo_log.end()) {
        r = 0;
    } else {
        for (auto entry = undo_log[var.tx_id].rbegin(); entry != undo_log[var.tx_id].rend(); ++entry) {
            if (entry->has_old_val)
                store[primary_replica][entry->key] = value_entry(entry->old_val);
            else
                store[primary_replica].erase(entry->key);
        }
    }
    return 0;
}

int shard_client::check_prepare_state(chdb_protocol::check_prepare_state_var var, int &r) {
    // TODO: Your code here
    if (undo_log.find(var.tx_id) != undo_log.end() && !active) {
        r = chdb_protocol::prepare_not_ok;
    } else {
        r = chdb_protocol::prepare_ok;
    }
    return 0;
}

int shard_client::prepare(chdb_protocol::prepare_var var, int &r) {
    // TODO: Your code here
    if (undo_log.find(var.tx_id) != undo_log.end() && !active) {
        r = chdb_protocol::prepare_not_ok;
    } else {
        r = chdb_protocol::prepare_ok;
    }
    return 0;
}

void shard_client::replicate_tx(int tx_id) {
    if (undo_log.find(tx_id) != undo_log.end()) {
        int store_size = store.size();
        for (int i = 0; i < store_size; ++i) {
            if (i != primary_replica) {
                for (auto &entry : undo_log[tx_id]) {
                    store[i][entry.key] = value_entry(entry.new_val);
                }
            }
        }
    }
}

shard_client::~shard_client() {
    delete node;
}