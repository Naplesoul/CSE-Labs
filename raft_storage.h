#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>
#include <sstream>
#include <vector>

#define init_buf_size 16

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();
    // Your code here
    void persist_current_term(int);
    void persist_vote_for(int);
    void persist_log(size_t start_idx, int last_included_term, const std::vector<log_entry<command>> &log_entries);
    void append_log(size_t actual_size, const log_entry<command> &entry);
    void persist_snapshot(const std::vector<char> &snapshot_data);

    int read_current_term();
    int read_vote_for();
    void read_log(size_t &start_idx, int &last_included_term, std::vector<log_entry<command>> &log_entries);
    void read_snapshot(std::vector<char> &snapshot_data);
private:
    std::mutex mtx;
    std::fstream number_storage;
    std::fstream log_storage;
    std::fstream snapshot_storage;
    char *cmd_buf;
    int cmd_buf_size;
};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir): cmd_buf(new char[init_buf_size]), cmd_buf_size(init_buf_size) {
    // Your code here

    std::stringstream number_filename;
    std::stringstream log_filename;
    std::stringstream snapshot_filename;

    number_filename << dir << "/number.rft";
    log_filename << dir << "/log.rft";
    snapshot_filename << dir << "/snapshot.rft";

    FILE *file = fopen(number_filename.str().c_str(), "a+");
    fclose(file);
    number_storage.open(number_filename.str(), std::ios::in | std::ios::out | std::ios::binary);

    file = fopen(log_filename.str().c_str(), "a+");
    fclose(file);
    log_storage.open(log_filename.str(), std::ios::in | std::ios::out | std::ios::binary);

    file = fopen(snapshot_filename.str().c_str(), "a+");
    fclose(file);
    snapshot_storage.open(snapshot_filename.str(), std::ios::in | std::ios::out | std::ios::binary);

    number_storage.seekg(0, std::ios::end);
    unsigned long file_length = number_storage.tellg();
    if (file_length < 2 * sizeof(int)) {
        char buf[sizeof(int)];
        number_storage.seekg(0);
        *((int *)buf) = 0;
        number_storage.write(buf, sizeof(int));
        *((int *)buf) = -1;
        number_storage.write(buf, sizeof(int));

        number_storage.flush();
    }

    log_storage.seekg(0, std::ios::end);
    file_length = log_storage.tellg();

    if (file_length < 2 * sizeof(size_t) + sizeof(int)) {
        std::vector<log_entry<command>> empty;
        empty.emplace_back();
        persist_log(0, 0, empty);
    }

    snapshot_storage.seekg(0, std::ios::end);
    file_length = snapshot_storage.tellg();
    if (file_length <= sizeof(size_t)) {
        char buf[sizeof(size_t)];
        *((size_t *)buf) = 0;
        snapshot_storage.seekg(0);
        snapshot_storage.write(buf, sizeof(size_t));

        snapshot_storage.flush();
    }
}

template<typename command>
raft_storage<command>::~raft_storage() {
   // Your code here
   number_storage.close();
   log_storage.close();
   delete []cmd_buf;
}

template<typename command>
void raft_storage<command>::persist_current_term(int current_term) {
    mtx.lock();
    char buf[sizeof(int)];
    *((int *)buf) = current_term;
    number_storage.seekg(0);
    number_storage.write(buf, sizeof(int));
    number_storage.flush();
    mtx.unlock();
}


template<typename command>
void raft_storage<command>::persist_vote_for(int vote_for) {
    mtx.lock();
    char buf[sizeof(int)];
    *((int *)buf) = vote_for;
    number_storage.seekg(sizeof(int));
    number_storage.write(buf, sizeof(int));
    number_storage.flush();
    mtx.unlock();
}

template<typename command>
int raft_storage<command>::read_current_term() {
    mtx.lock();
    char buf[sizeof(int)];
    number_storage.seekg(0);
    number_storage.read(buf, sizeof(int));
    mtx.unlock();
    return *((int *)buf);
}

template<typename command>
int raft_storage<command>::read_vote_for() {
    mtx.lock();
    char buf[sizeof(int)];
    number_storage.seekg(sizeof(int));
    number_storage.read(buf, sizeof(int));
    mtx.unlock();
    return *((int *)buf);
}

template<typename command>
void raft_storage<command>::persist_log(size_t start_idx, int last_included_term, const std::vector<log_entry<command>> &log_entries) {
    mtx.lock();

    log_storage.seekg(2 * sizeof(size_t) + sizeof(int));
    char buf[sizeof(size_t)];
    for (const log_entry<command> &entry : log_entries) {
        *((int *)buf) = entry.term;
        log_storage.write(buf, sizeof(int));
        
        int cmd_size = entry.cmd.size();
        *((int *)buf) = cmd_size;
        log_storage.write(buf, sizeof(int));

        if (cmd_size > cmd_buf_size) {
            delete []cmd_buf;
            cmd_buf = new char[cmd_size];
            cmd_buf_size = cmd_size;
        }

        entry.cmd.serialize(cmd_buf, cmd_size);
        log_storage.write(cmd_buf, cmd_size);
    }

    log_storage.seekg(0);

    *((size_t *)buf) = start_idx;
    log_storage.write(buf, sizeof(size_t));

    *((int *)buf) = last_included_term;
    log_storage.write(buf, sizeof(int));

    *((size_t *)buf) = log_entries.size();
    log_storage.write(buf, sizeof(size_t));

    log_storage.flush();
    
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::append_log(size_t actual_size, const log_entry<command> &entry) {
    mtx.lock();

    char buf[sizeof(size_t)];
    log_storage.seekg(0, std::ios::end);
    *((int *)buf) = entry.term;
    log_storage.write(buf, sizeof(int));

    int cmd_size = entry.cmd.size();
    *((int *)buf) = cmd_size;
    log_storage.write(buf, sizeof(int));

    if (cmd_size > cmd_buf_size) {
        delete []cmd_buf;
        cmd_buf = new char[cmd_size];
        cmd_buf_size = cmd_size;
    }

    entry.cmd.serialize(cmd_buf, cmd_size);
    log_storage.write(cmd_buf, cmd_size);

    log_storage.seekg(sizeof(size_t) + sizeof(int));
    *((size_t *)buf) = actual_size;
    log_storage.write(buf, sizeof(size_t));

    log_storage.flush();
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::read_log(size_t &start_idx, int &last_included_term, std::vector<log_entry<command>> &log_entries) {
    mtx.lock();
    char buf[sizeof(size_t)];

    log_storage.seekg(0);
    log_storage.read(buf, sizeof(size_t));
    start_idx = *((size_t *)buf);

    log_storage.read(buf, sizeof(int));
    last_included_term = *((int *)buf);

    log_storage.read(buf, sizeof(size_t));
    size_t actual_size = *((size_t *)buf);

    for (size_t i = 0; i < actual_size; ++i) {
        log_storage.read(buf, sizeof(int));
        int term = *((int *)buf);

        log_storage.read(buf, sizeof(int));
        int cmd_size = *((int *)buf);

        command cmd;

        if (cmd_size > cmd_buf_size) {
            delete []cmd_buf;
            cmd_buf = new char[cmd_size];
            cmd_buf_size = cmd_size;
        }

        log_storage.read(cmd_buf, cmd_size);
        cmd.deserialize(cmd_buf, cmd_size);
        
        log_entries.emplace_back(term, cmd);
    }

    mtx.unlock();
}

template<typename command>
void raft_storage<command>::persist_snapshot(const std::vector<char> &snapshot_data) {
    mtx.lock();

    snapshot_storage.seekg(sizeof(size_t));
    char *byte_buf = new char;

    for (const char &byte : snapshot_data) {
        *byte_buf = byte;
        snapshot_storage.write(byte_buf, 1);
    }

    delete byte_buf;

    char buf[sizeof(size_t)];
    *((size_t *)buf) = snapshot_data.size();
    snapshot_storage.seekg(0);
    snapshot_storage.write(buf, sizeof(size_t));

    snapshot_storage.flush();
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::read_snapshot(std::vector<char> &snapshot_data) {
    mtx.lock();

    char buf[sizeof(size_t)];
    snapshot_storage.seekg(0);
    snapshot_storage.read(buf, sizeof(size_t));
    size_t size = *((size_t *)buf);

    char *byte_buf = new char[size];
    snapshot_storage.read(byte_buf, size);
    for (size_t i = 0; i < size; ++i) {
        snapshot_data.push_back(byte_buf[i]);
    }

    mtx.unlock();
}

#endif // raft_storage_h