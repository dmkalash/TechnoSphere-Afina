#include "Connection.h"
#include "ServerImpl.h"

#include <atomic>
#include <iostream>
#include <sys/uio.h>
#include <sys/socket.h>
#include <sys/types.h>

namespace Afina {
namespace Network {
namespace MTnonblock {

// See Connection.h
void Connection::Start() {
    _logger->info("Start st_blocking network service");
    running.store(true, std::memory_order_relaxed);
    _event.events = EPOLLIN | EPOLLRDHUP | EPOLLERR;
    _write_offset = 0;
    _read_bytes = 0;
    _results.clear();
    _output_only = false;
}

// See Connection.h
void Connection::OnError() {
    running.store(false, std::memory_order_relaxed);
    _event.events = 0;
}

// See Connection.h
void Connection::OnClose() {
    running.store(false, std::memory_order_relaxed);
    _event.events = 0;
}

// See Connection.h
void Connection::DoRead() {
    std::atomic_thread_fence(std::memory_order_acquire);
    try {
        int readed_bytes = read(_socket, client_buffer + _read_bytes, sizeof(client_buffer) - _read_bytes);
        if (readed_bytes > 0) {
            readed_bytes += _read_bytes;
            std::size_t parser_offset = 0;

            while (readed_bytes > 0) {
                if (!command_to_execute) {
                    std::size_t parsed = 0;
                    if (parser.Parse(client_buffer + parser_offset, readed_bytes, parsed)) {
                        command_to_execute = parser.Build(arg_remains);
                        if (arg_remains > 0) {
                            arg_remains += 2;
                        }
                    }

                    if (parsed == 0) {
                        _read_bytes = readed_bytes;
                        std::memmove(client_buffer, client_buffer + parser_offset, _read_bytes);
                        break;
                    } else {
                        parser_offset += parsed;
                        readed_bytes -= parsed;
                    }
                }

                if (command_to_execute && arg_remains > 0) {
                    std::size_t to_read = std::min(arg_remains, std::size_t(readed_bytes));
                    argument_for_command.append(client_buffer + parser_offset, to_read);

                    arg_remains -= to_read;
                    readed_bytes -= to_read;
                    parser_offset += to_read;
                }

                if (command_to_execute && arg_remains == 0) {

                    std::string result = "";
                    if (argument_for_command.size()) {
                        argument_for_command.resize(argument_for_command.size() - 2);
                    }
                    command_to_execute->Execute(*pStorage, argument_for_command, result);

                    result += "\r\n";
                    if (_results.empty()) {
                        _event.events |= EPOLLOUT;
                    }
                    _results.push_back(result);
                    if (_results.size() >= MAX_QUEUE_SIZE_HIGH) {
                        _event.events &= ~EPOLLIN;
                    }

                    // Prepare for the next command
                    command_to_execute.reset();
                    argument_for_command.resize(0);
                    parser.Reset();
                }
            } // while (readed_bytes)
            if (readed_bytes == 0) {
                _read_bytes = 0;
            }
        } else if (readed_bytes == 0) {
            _output_only = true;
            // _logger->debug("Connection closed");
        } else if (!(errno == EAGAIN || errno == EINTR)) {
            throw std::runtime_error(std::string(strerror(errno)));
        }
    } catch (std::runtime_error &ex) {
        // _logger->error("Failed to process connection on descriptor {}: {}", _socket, ex.what());
        std::cerr << ("Failed to process connection on descriptor {}: {}", _socket, ex.what()) << std::endl;
        _results.push_back("ERROR\r\n");
        if (_results.empty()) {
            _event.events |= EPOLLOUT;
        }
        shutdown(_socket, SHUT_RD);
        _output_only = true;
        _event.events &= ~EPOLLIN;
    }
    std::atomic_thread_fence(std::memory_order_release);
}

// See Connection.h
void Connection::DoWrite() {
    std::atomic_thread_fence(std::memory_order_acquire);
    iovec iovecs[IOVEC_SIZE] = {};
    auto it = _results.begin();
    iovecs[0].iov_base = &((*it)[0]) + _write_offset;
    iovecs[0].iov_len = it->size() - _write_offset;
    ++it;
    size_t in_iovec = 1;

    while (it != _results.end() && in_iovec < IOVEC_SIZE) {
        iovecs[in_iovec].iov_base = &((*it)[0]);
        iovecs[in_iovec].iov_len = it->size();
        it++;
        in_iovec++;
    }

    int written = 0;
    if ((written = writev(_socket, iovecs, in_iovec)) > 0) {
        for (size_t i = 0; i < in_iovec; ++i) {
            if (written >= iovecs[i].iov_len) {
                written -= iovecs[i].iov_len;
                _results.pop_front();
            }
            else {
                break;
            }
        }
        _write_offset = written;
    } else if (written < 0 && !(errno == EAGAIN || errno == EINTR)) {
        this->OnError();
    }

    if (_results.size() < MAX_QUEUE_SIZE_LOW && !_output_only) {
        _event.events |= EPOLLIN;
    }

    if (_results.empty()) {
        _event.events &= ~EPOLLOUT;
    }

    if (_output_only && _results.empty()) {
        shutdown(_socket, SHUT_WR);
        OnClose();
    }
    std::atomic_thread_fence(std::memory_order_release);
}

} // namespace MTnonblock
} // namespace Network
} // namespace Afina