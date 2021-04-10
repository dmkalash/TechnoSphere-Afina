#include "Connection.h"

#include <iostream>
#include <unistd.h>
#include <cassert>
#include <sys/uio.h>


namespace Afina {
namespace Network {
namespace MTnonblock {


// See Connection.h
void Connection::Start() {
    std::unique_lock<std::mutex> lock(_lock);
    _logger->debug("Start. Socket: {}", _socket);
    running.store(true);
    _event.events = EPOLLIN | EPOLLRDHUP | EPOLLERR;
    command_to_execute.reset();
    argument_for_command.resize(0);
    parser.Reset();
}


// See Connection.h
void Connection::OnError() {
    std::unique_lock<std::mutex> lock(_lock);
    _logger->debug("Error. Socket: {}", _socket);
    running.store(false);
}


// See Connection.h
void Connection::OnClose() {
    std::unique_lock<std::mutex> lock(_lock);
    _logger->debug("Closing. Socket: {}", _socket);
    running.store(false);
}


// See Connection.h
void Connection::DoRead() {
    std::unique_lock<std::mutex> lock(_lock);
    _logger->debug("Reading. Socket: {}", _socket);
    int client_socket = _socket;
    try {
        int readed_bytes = -1;
        while ((readed_bytes = read(client_socket, client_buffer + _read_bytes,
                                    sizeof(client_buffer) - _read_bytes)) > 0) {
            _logger->debug("Have {} bytes", readed_bytes);
            _read_bytes += readed_bytes;

            while (_read_bytes > 0) {
                if (!command_to_execute) {
                    std::size_t parsed = 0;
                    if (parser.Parse(client_buffer, _read_bytes, parsed)) {
                        _logger->debug("Command: {} in {} bytes", parser.Name(), parsed);
                        command_to_execute = parser.Build(arg_remains);
                        if (arg_remains > 0) {
                            arg_remains += 2;
                        }
                    }

                    if (parsed == 0) {
                        break;
                    } else {
                        std::memmove(client_buffer, client_buffer + parsed, _read_bytes - parsed);
                        _read_bytes -= parsed;
                    }
                }

                if (command_to_execute && arg_remains > 0) {
                    std::size_t to_read = std::min(arg_remains, std::size_t(_read_bytes));
                    argument_for_command.append(client_buffer, to_read);

                    std::memmove(client_buffer, client_buffer + to_read, _read_bytes - to_read);
                    arg_remains -= to_read;
                    _read_bytes -= to_read;
                }

                if (command_to_execute && arg_remains == 0) {
                    _logger->debug("Execute command");

                    std::string result;
                    command_to_execute->Execute(*pStorage, argument_for_command, result);

                    result += "\r\n";
                    if (_results.empty()) {
                        _event.events = EPOLLOUT | EPOLLRDHUP | EPOLLERR;
                    }
                    _results.push_back(result);

                    command_to_execute.reset();
                    argument_for_command.resize(0);
                    parser.Reset();
                }
            }
        }
    } catch (std::runtime_error &ex) {
        _logger->error("Failed to process connection on descriptor {}: {}", client_socket, ex.what());
    }
}

// See Connection.h
void Connection::DoWrite() {
    std::unique_lock<std::mutex> lock(_lock);
    if (_results.empty() == false) {
        _logger->debug("Writing. Socket: {}", _socket);

        struct iovec buffers[N];
        auto _results_it = _results.begin();
        std::size_t max_size = std::min(std::size_t(N), _results.size());

        for (auto i = 0; i < max_size; ++i, ++_results_it) {
            buffers[i].iov_base = &(*_results_it)[0];
            buffers[i].iov_len = _results_it->size();
        }

        buffers[0].iov_base = (char *) buffers[0].iov_base + _first_byte;
        buffers[0].iov_len -= _first_byte;

        auto amount_placed_bytes = writev(_socket, buffers, max_size);
        if (amount_placed_bytes == -1) {
            throw std::runtime_error(std::string(strerror(errno)));
        }
        _first_byte += amount_placed_bytes;

        _results_it = _results.begin();
        for (const auto &result : _results) {
            if (_first_byte < result.size()) {
                break;
            }
            _first_byte -= result.size();
            _results_it++;
        }

        _results.erase(_results.begin(), _results_it);

        if (_results.empty()) {
            _event.events = EPOLLIN | EPOLLRDHUP | EPOLLERR;
        }
    }
}

} // namespace MTnonblock
} // namespace Network
} // namespace Afina
