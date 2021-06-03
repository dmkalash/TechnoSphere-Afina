#ifndef AFINA_NETWORK_MT_NONBLOCKING_CONNECTION_H
#define AFINA_NETWORK_MT_NONBLOCKING_CONNECTION_H

#include <cstring>

#include "afina/Storage.h"
#include "afina/logging/Service.h"
#include "afina/execute/Command.h"
#include "spdlog/logger.h"

#include <sys/epoll.h>
#include <atomic>
#include <deque>

#include "protocol/Parser.h"


namespace Afina {
namespace Network {
namespace MTnonblock {

class Connection {
public:
    Connection(int s, std::shared_ptr<Afina::Storage> ps) : _socket(s), pStorage(ps), _output_only(false) {
        std::memset(&_event, 0, sizeof(struct epoll_event));
        _event.data.ptr = this;
    }

    inline bool isAlive() const { return running; }

    void Start();

protected:
    void OnError();
    void OnClose();
    void DoRead();
    void DoWrite();

private:
    friend class Worker;
    friend class ServerImpl;
    const size_t MAX_QUEUE_SIZE_HIGH = 100;
    const size_t MAX_QUEUE_SIZE_LOW = 90;
    const size_t IOVEC_SIZE = 32;

    std::shared_ptr<spdlog::logger> _logger;
    std::shared_ptr<Afina::Storage> pStorage;

    std::size_t _write_offset;
    std::deque<std::string> _results;

    std::size_t arg_remains;
    Protocol::Parser parser;
    std::string argument_for_command;
    std::unique_ptr<Execute::Command> command_to_execute;

    std::size_t _read_bytes;
    char client_buffer[4096] = "";

    std::atomic<bool> running;
    int _socket;
    struct epoll_event _event;

    bool _output_only;
};

} // namespace MTnonblock
} // namespace Network
} // namespace Afina

#endif // AFINA_NETWORK_MT_NONBLOCKING_CONNECTION_H