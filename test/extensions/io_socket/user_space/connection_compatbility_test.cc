#include <memory>

#include "source/common/network/address_impl.h"
#include "source/common/network/connection_impl.h"
#include "source/common/network/io_socket_handle_impl.h"
#include "source/common/network/listen_socket_impl.h"
#include "source/common/network/raw_buffer_socket.h"
#include "source/common/network/utility.h"
#include "source/extensions/io_socket/user_space/io_handle_impl.h"

#include "test/mocks/api/mocks.h"
#include "test/mocks/event/mocks.h"
#include "test/mocks/network/mocks.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using testing::Invoke;

namespace Envoy {
namespace Extensions {
namespace IoSocket {
namespace UserSpace {
namespace {

// This class verifies client connection can be established with user space socket.
class InternalClientConnectionImplTest : public testing::Test {
public:
  InternalClientConnectionImplTest()
      : api_(Api::createApiForTest()), dispatcher_(api_->allocateDispatcher("test_thread")) {}

  void SetUp() override {
    std::tie(io_handle_, io_handle_peer_) = IoHandleFactory::createIoHandlePair();
    local_addr_ = *io_handle_->localAddress();
    remote_addr_ = *io_handle_->peerAddress();
  }
  Api::ApiPtr api_;
  Event::DispatcherPtr dispatcher_;
  std::unique_ptr<IoHandleImpl> io_handle_;
  std::unique_ptr<IoHandleImpl> io_handle_peer_;
  Network::MockConnectionCallbacks connection_callbacks;
  std::unique_ptr<Network::ClientConnectionImpl> client_;
  Network::Address::InstanceConstSharedPtr local_addr_;
  Network::Address::InstanceConstSharedPtr remote_addr_;
};

TEST_F(InternalClientConnectionImplTest, Basic) {
  client_ = std::make_unique<Network::ClientConnectionImpl>(
      *dispatcher_,
      std::make_unique<Network::ConnectionSocketImpl>(std::move(io_handle_), local_addr_,
                                                      remote_addr_),
      nullptr, std::make_unique<Network::RawBufferSocket>(), nullptr, nullptr);
  client_->connect();
  client_->noDelay(true);
  dispatcher_->run(Event::Dispatcher::RunType::Block);

  client_->close(Network::ConnectionCloseType::NoFlush);
}

TEST_F(InternalClientConnectionImplTest, ConnectCallbacksAreInvoked) {
  client_ = std::make_unique<Network::ClientConnectionImpl>(
      *dispatcher_,
      std::make_unique<Network::ConnectionSocketImpl>(std::move(io_handle_), local_addr_,
                                                      remote_addr_),
      nullptr, std::make_unique<Network::RawBufferSocket>(), nullptr, nullptr);
  client_->addConnectionCallbacks(connection_callbacks);
  client_->connect();
  client_->noDelay(true);
  EXPECT_CALL(connection_callbacks, onEvent(_))
      .WillOnce(Invoke([&](Network::ConnectionEvent event) -> void {
        EXPECT_EQ(event, Network::ConnectionEvent::Connected);
        dispatcher_->exit();
      }));
  dispatcher_->run(Event::Dispatcher::RunType::Block);
  EXPECT_CALL(connection_callbacks, onEvent(Network::ConnectionEvent::LocalClose));

  client_->close(Network::ConnectionCloseType::NoFlush);
}

TEST_F(InternalClientConnectionImplTest, ConnectFailed) {
  client_ = std::make_unique<Network::ClientConnectionImpl>(
      *dispatcher_,
      std::make_unique<Network::ConnectionSocketImpl>(std::move(io_handle_), local_addr_,
                                                      remote_addr_),
      nullptr, std::make_unique<Network::RawBufferSocket>(), nullptr, nullptr);
  client_->addConnectionCallbacks(connection_callbacks);
  client_->connect();
  client_->noDelay(true);

  io_handle_peer_->close();
  EXPECT_CALL(connection_callbacks, onEvent(Network::ConnectionEvent::RemoteClose));
  dispatcher_->run(Event::Dispatcher::RunType::Block);

  client_->close(Network::ConnectionCloseType::NoFlush);
}

// Reproduces a connection leak on internal listener (user-space) sockets.
//
// When TCP proxy enables half-close on a connection and then calls close(FlushWrite) with pending
// write data after the peer IO handle has been destroyed, the connection enters
// DelayedCloseState::CloseAfterFlush but no events ever fire to complete the close:
//   - Half-close suppresses the Closed event (closeInternal enables Write only, not Write|Closed).
//   - The destroyed peer makes isPeerWritable() return false, so no Write event is scheduled.
//   - Without a delayed_close_timeout, there is no timer fallback.
//
// On real TCP sockets this path cannot be reached because a write() to a dead peer returns EPIPE,
// which triggers PostIoAction::Close in RawBufferSocket::doWrite(). On user-space sockets the
// write event never fires, so the error is never discovered and the connection leaks.
//
// In production this manifests as accumulating sni_listener downstream connections (~26 KB each)
// when using internal listeners with dynamic forward proxy.
TEST_F(InternalClientConnectionImplTest, FlushWriteAfterPeerDestroyWithHalfCloseLeaks) {
  client_ = std::make_unique<Network::ClientConnectionImpl>(
      *dispatcher_,
      std::make_unique<Network::ConnectionSocketImpl>(std::move(io_handle_), local_addr_,
                                                      remote_addr_),
      nullptr, std::make_unique<Network::RawBufferSocket>(), nullptr, nullptr);
  client_->addConnectionCallbacks(connection_callbacks);
  // TCP proxy enables half-close on both downstream and upstream connections.
  client_->enableHalfClose(true);
  client_->connect();

  // Wait for the Connected event before writing.
  EXPECT_CALL(connection_callbacks, onEvent(Network::ConnectionEvent::Connected))
      .WillOnce(Invoke([&](Network::ConnectionEvent) { dispatcher_->exit(); }));
  dispatcher_->run(Event::Dispatcher::RunType::Block);

  // Write 24 bytes (simulating a TLS close_notify forwarded by TCP proxy).
  // Data moves to the connection's write_buffer_. activateFileEvents(Write) schedules a callback
  // but we do NOT run the dispatcher, so the data stays in the write buffer.
  Buffer::OwnedImpl data(std::string(24, 'x'));
  client_->write(data, false);
  EXPECT_EQ(0, data.length());

  // Destroy the peer (simulates the pool/upstream side of the internal listener closing first).
  // This calls setWriteEnd() and onPeerDestroy() on the connection's IO handle, setting
  // peer_handle_ = nullptr. With the fix, onPeerDestroy() also activates a Write event.
  io_handle_peer_->close();

  // Now close with FlushWrite while data is pending and the peer is gone.
  // closeInternal() sees 24 bytes pending, sets delayed_close_state_ = CloseAfterFlush, and
  // calls enableFileEvents(Write) (no Closed because half-close is enabled).
  // With the fix, isPeerWritable() returns true when write_shutdown_ is set (peer destroyed),
  // so a Write event is scheduled. Without the fix, no events would fire and the connection
  // would leak in Closing state forever.
  client_->close(Network::ConnectionCloseType::FlushWrite);

  // The Write event fires, onWriteReady() calls doWrite() which attempts io_handle->write().
  // The write returns SOCKET_ERROR_INVAL (peer destroyed) → PostIoAction::Close →
  // closeSocket(RemoteClose). This is analogous to EPIPE on a real TCP socket.
  EXPECT_CALL(connection_callbacks, onEvent(Network::ConnectionEvent::RemoteClose));
  dispatcher_->run(Event::Dispatcher::RunType::NonBlock);

  EXPECT_EQ(Network::Connection::State::Closed, client_->state());
}
} // namespace
} // namespace UserSpace
} // namespace IoSocket
} // namespace Extensions
} // namespace Envoy
