#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>
#include <sstream>
#include <vector>

#define buf_size 16

static_assert(buf_size >= sizeof(size_t));

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();
    // Your code here
    void persist_current_term(int);
    void persist_vote_for(int);
    void persist_log(const std::vector<log_entry<command>> &);
    void append_log(size_t, const log_entry<command> &);
    int read_current_term();
    int read_vote_for();
    void read_log(std::vector<log_entry<command>> &);
private:
    std::mutex mtx;
    std::fstream number_storage;
    std::fstream log_storage;
};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir) {
    // Your code here
    std::stringstream number_filename;
    std::stringstream log_filename;

    number_filename << dir << "/number.rft";
    log_filename << dir << "/log.rft";

    FILE *file = fopen(number_filename.str().c_str(), "a+");
    fclose(file);
    number_storage.open(number_filename.str(), std::ios::in | std::ios::out | std::ios::binary);

    file = fopen(log_filename.str().c_str(), "a+");
    fclose(file);
    log_storage.open(log_filename.str(), std::ios::in | std::ios::out | std::ios::binary);

    number_storage.seekg(0, std::ios::end);
    unsigned long file_length = number_storage.tellg();
    if (file_length < 2 * sizeof(int)) {
        char buf[sizeof(int)];
        number_storage.seekg(0);
        *((int *)buf) = 0;
        number_storage.write(buf, sizeof(int));
        *((int *)buf) = -1;
        number_storage.write(buf, sizeof(int));
    }

    log_storage.seekg(0, std::ios::end);
    file_length = log_storage.tellg();

    if (file_length <= sizeof(size_t)) {
        std::vector<log_entry<command>> empty;
        empty.emplace_back();
        persist_log(empty);
    }
}

template<typename command>
raft_storage<command>::~raft_storage() {
   // Your code here
   number_storage.close();
   log_storage.close();
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
void raft_storage<command>::persist_log(const std::vector<log_entry<command>> &log_entries) {
    mtx.lock();

    log_storage.seekg(sizeof(size_t));
    char buf[buf_size];
    for (const log_entry<command> &entry : log_entries) {
        int size = entry.cmd.size();
        assert(size <= buf_size);
        *((int *)buf) = entry.term;
        log_storage.write(buf, sizeof(int));
        entry.cmd.serialize(buf, size);
        log_storage.write(buf, size);
    }

    log_storage.seekg(0);
    *((size_t *)buf) = log_entries.size();
    log_storage.write(buf, sizeof(size_t));
    log_storage.flush();
    
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::append_log(size_t total_size, const log_entry<command> &entry) {
    mtx.lock();

    char buf[buf_size];
    log_storage.seekg(0, std::ios::end);
    *((int *)buf) = entry.term;
    log_storage.write(buf, sizeof(int));

    int size = entry.cmd.size();
    assert(size <= buf_size);

    entry.cmd.serialize(buf, size);
    log_storage.write(buf, size);

    log_storage.seekg(0);
    *((size_t *)buf) = total_size;
    log_storage.write(buf, sizeof(size_t));
    log_storage.flush();
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::read_log(std::vector<log_entry<command>> &log_entries) {
    mtx.lock();

    log_storage.seekg(0);
    char buf[buf_size];
    log_storage.read(buf, sizeof(size_t));
    size_t total_size = *((size_t *)buf);

    for (size_t i = 0; i < total_size; ++i) {
        log_storage.read(buf, sizeof(int));
        int term = *((int *)buf);

        command cmd;
        int size = cmd.size();
        assert(size <= buf_size);

        log_storage.read(buf, size);
        cmd.deserialize(buf, size);
        
        log_entries.emplace_back(term, cmd);
    }

    mtx.unlock();
}

#endif // raft_storage_h