#ifndef AFINA_NETWORK_MT_NONBLOCKING_SERVER_H
#define AFINA_NETWORK_MT_NONBLOCKING_SERVER_H

#include <mutex>
#include <set>
#include <thread>
#include <vector>
#include <unordered_set>

#include "Connection.h"
#include <afina/network/Server.h>

namespace spdlog {
class logger;
}

namespace Afina {
namespace Network {
namespace MTnonblock {

// Forward declaration, see Worker.h
class Worker;

/**
 * # Network resource manager implementation
 * Epoll based server
 */
class ServerImpl : public Server {
public:
    ServerImpl(std::shared_ptr<Afina::Storage> ps, std::shared_ptr<Logging::Service> pl);
    ~ServerImpl();

    // See Server.h
    void Start(uint16_t port, uint32_t acceptors, uint32_t workers) override;

    // See Server.h
    void Stop() override;

    // See Server.h
    void Join() override;

    // _running_workers++
    void IncreaseWorkerNum();

    // _running_workers--
    void DecreaseWorkerNum();

    // last worker must close
    void CloseAllConnections();

    void EraseConnection(Connection* pc);

protected:
    void OnRun();
    void OnNewConnection();

private:
    friend class Worker;

    // logger to use
    std::shared_ptr<spdlog::logger> _logger;

    // Port to listen for new connections, permits access only from
    // inside of accept_thread
    // Read-only
    uint16_t listen_port;

    // Socket to accept new connection on, shared between acceptors
    int _server_socket;

    // Threads that accepts new connections, each has private epoll instance
    // but share global server socket
    std::vector<std::thread> _acceptors;

    // EPOLL instance shared between workers
    int _data_epoll_fd;

    // Curstom event "device" used to wakeup workers
    int _event_fd;

    // threads serving read/write requests
    std::vector<Worker> _workers;

    // Opened client sockets
    std::unordered_set<Connection *> _client_sockets;

    // Running workers
    std::size_t _running_workers;

    // mutex on set
    std::mutex _sockets_mutex;
};

} // namespace MTnonblock
} // namespace Network
} // namespace Afina

#endif // AFINA_NETWORK_MT_NONBLOCKING_SERVER_H