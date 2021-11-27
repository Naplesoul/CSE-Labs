#include "raft_state_machine.h"


kv_command::kv_command() : kv_command(CMD_NONE, "", "") { }

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) : 
    cmd_tp(tp), key(key), value(value), res(std::make_shared<result>())
{
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) :
    cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() { }

int kv_command::size() const {
    // Your code here:
    return sizeof(command_type) + 2 * sizeof(int) + key.size() + value.size();
}


void kv_command::serialize(char* buf, int size) const {
    // Your code here:
    assert(size >= this->size());
    *((command_type *)buf) = cmd_tp;
    *((int *)(buf + sizeof(command_type))) = key.size();
    *((int *)(buf + sizeof(command_type) + sizeof(int))) = value.size();
    memcpy(buf + sizeof(command_type) + 2 * sizeof(int), key.data(), key.size());
    memcpy(buf + sizeof(command_type) + 2 * sizeof(int) + key.size(), value.data(), value.size());
    return;
}

void kv_command::deserialize(const char* buf, int size) {
    // Your code here:
    cmd_tp = *((command_type *)buf);
    int key_size = *((int *)(buf + sizeof(command_type)));
    int val_size = *((int *)(buf + sizeof(command_type) + sizeof(int)));
    assert((size_t)size >= sizeof(command_type) + 2 * sizeof(int) + key_size + val_size);
    key = std::string(buf + sizeof(command_type) + 2 * sizeof(int), key_size);
    value = std::string(buf + sizeof(command_type) + 2 * sizeof(int) + key_size, val_size);
    return;
}

marshall& operator<<(marshall &m, const kv_command& cmd) {
    // Your code here:
    m << (int)cmd.cmd_tp << cmd.key << cmd.value;
    return m;
}

unmarshall& operator>>(unmarshall &u, kv_command& cmd) {
    // Your code here:
    int cmd_tp;
    u >> cmd_tp >> cmd.key >> cmd.value;
    cmd.cmd_tp = (kv_command::command_type)cmd_tp;
    return u;
}

kv_state_machine::~kv_state_machine() {

}

void kv_state_machine::apply_log(raft_command &cmd) {
    kv_command &kv_cmd = dynamic_cast<kv_command&>(cmd);
    std::unique_lock<std::mutex> lock(kv_cmd.res->mtx);
    // Your code here:
    kv_cmd.res->start = std::chrono::system_clock::now();

    switch (kv_cmd.cmd_tp)
    {
    case kv_command::command_type::CMD_NONE:
        kv_cmd.res->succ = true;
        break;

    case kv_command::command_type::CMD_GET:
        kv_cmd.res->key = kv_cmd.key;

        mtx.lock();
        kv_cmd.res->value = map[kv_cmd.key];
        mtx.unlock();

        if (kv_cmd.res->value.length() == 0) {
            kv_cmd.res->succ = false;
        } else {
            kv_cmd.res->succ = true;
        }
        break;

    case kv_command::command_type::CMD_PUT:
        kv_cmd.res->key = kv_cmd.key;

        mtx.lock();
        kv_cmd.res->value = map[kv_cmd.key];
        if (kv_cmd.value.length() > 0) {
            map[kv_cmd.key] = kv_cmd.value;
        }
        mtx.unlock();

        if (kv_cmd.res->value.length() == 0) {
            kv_cmd.res->succ = true;
            kv_cmd.res->value = kv_cmd.value;
        } else {
            kv_cmd.res->succ = false;
        }
        break;

    case kv_command::command_type::CMD_DEL:
        kv_cmd.res->key = kv_cmd.key;

        mtx.lock();
        kv_cmd.res->value = map[kv_cmd.key];
        map.erase(kv_cmd.key);
        mtx.unlock();

        if (kv_cmd.res->value.length() == 0) {
            kv_cmd.res->succ = false;
        } else {
            kv_cmd.res->succ = true;
        }
        break;
    
    default:
        break;
    }
    kv_cmd.res->done = true;
    kv_cmd.res->cv.notify_all();
    return;
}

std::vector<char> kv_state_machine::snapshot() {
    // Your code here:
    std::stringstream buf;
    mtx.lock();
    for (auto it = map.begin(); it != map.end(); ++it) {
        buf << it->first << ':' << it->second << ':';
    }
    mtx.unlock();

    std::string s_buf = buf.str();
    std::vector<char> snapshot_data;
    for (const char &byte : s_buf) {
        snapshot_data.push_back(byte);
    }
    return snapshot_data;
}

void kv_state_machine::apply_snapshot(const std::vector<char>& snapshot) {
    // Your code here:
    std::stringstream buf;
    for (const char &byte : snapshot) {
        buf << byte;
    }

    std::string s_buf = buf.str();
    std::map<std::string, std::string> new_map;
    
    while (s_buf.length() > 0) {
        size_t pos = s_buf.find(':');
        std::string key = s_buf.substr(0, pos);
        s_buf.erase(0, pos + 1);

        pos = s_buf.find(':');
        std::string value = s_buf.substr(0, pos);

        s_buf.erase(0, pos + 1);

        new_map[key] = value;
    }

    mtx.lock();
    map = new_map;
    mtx.unlock();

    return;    
}
