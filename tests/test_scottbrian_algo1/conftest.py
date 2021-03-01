"""conftest.py module for testing."""
import time

import pytest

#from ibapi.client import EClient  # type: ignore
from ibapi.connection import Connection  # type: ignore
from ibapi.connection import *
from scottbrian_algo1.algo_api import AlgoApp
from scottbrian_utils.diag_msg import diag_msg
import queue





# class TAlgoApp(AlgoApp):
#     """Class used to test the algo app."""
#     def __init__(self) -> None:
#         """Initialize the test algo app."""
#         diag_msg('entered')
#         AlgoApp.__init__(self)
#         # self.run_thread = Thread(target=self.run)
#         diag_msg('exiting')

    # def run(self) -> None:
    #     """Run loop for testing."""
    #     diag_msg('entered')
    #     for i in range(5):
    #         time.sleep(1)
    #         if i == 3:
    #             diag_msg('about to call nextValidId')
    #             self.nextValidId(1)


@pytest.fixture(scope='function')
def algo_app(monkeypatch) -> "AlgoApp":
    """Instantiate and return an AlgoApp for testing.

    Returns:
        An instance of AlgoApp
    """

    # def mock_client_run(self) -> None:
    #     """Run loop for testing."""
    #     diag_msg('entered')
    #     for i in range(5):
    #         time.sleep(1)
    #         if i == 3:
    #             diag_msg('about to call nextValidId')
    #             self.nextValidId(1)

    # monkeypatch.setattr(EClient, 'run', mock_client_run)

    # def mock_client_connect(self,
    #                         ip_addr: str,
    #                         port: int,
    #                         client_id: int) -> None:
    #     diag_msg('ip_addr:', ip_addr,
    #              'port:', port,
    #              'client_id:', client_id)
    #     # algo_app.nextValidId(1)
    #     return None

    # monkeypatch.setattr(EClient, "connect", mock_client_connect)

    def mock_connection_connect(self):
        diag_msg('entered')
        try:
            self.socket = socket.socket()
        # TO DO list the exceptions you want to catch
        except socket.error:
            diag_msg('socket instantiation failed')
            if self.wrapper:
                self.wrapper.error(NO_VALID_ID, FAIL_CREATE_SOCK.code(), FAIL_CREATE_SOCK.msg())

        try:
            pass
            # self.socket.connect((self.host, self.port))
        except socket.error:
            diag_msg('socket error')
            if self.wrapper:
                self.wrapper.error(NO_VALID_ID, CONNECT_FAIL.code(), CONNECT_FAIL.msg())

        self.socket.settimeout(1)  # non-blocking

        diag_msg('exiting')

    monkeypatch.setattr(Connection, "connect", mock_connection_connect)

    def mock_connection_sendMsg(self, msg):
        diag_msg('entered')
        logger.debug("acquiring lock")
        self.lock.acquire()
        logger.debug("acquired lock")
        if not self.isConnected():
            logger.debug("sendMsg attempted while not connected, releasing lock")
            self.lock.release()
            return 0
        try:
            nSent = len(msg)  # self.socket.send(msg)
            mock_send_recv.send_msg(msg)
        except socket.error:
            logger.debug("exception from sendMsg %s", sys.exc_info())
            raise
        finally:
            logger.debug("releasing lock")
            self.lock.release()
            logger.debug("release lock")

        logger.debug("sendMsg: sent: %d", nSent)
        diag_msg('exiting')
        return nSent

    monkeypatch.setattr(Connection, "sendMsg", mock_connection_sendMsg)

    def mock_connection_recvAllMsg(self):
        cont = True
        allbuf = b""

        allbuf = mock_send_recv.recv_msg()

        # while cont and self.isConnected():
        #     buf = self.socket.recv(4096)
        #     allbuf += buf
        #     logger.debug("len %d raw:%s|", len(buf), buf)
        #
        #     if len(buf) < 4096:
        #         cont = False

        return allbuf

    monkeypatch.setattr(Connection, "_recvAllMsg", mock_connection_recvAllMsg)
    a_algo_app = AlgoApp()
    return a_algo_app

class MockSendRecv:
    def __init__(self):
        self.msg_send_q = queue.Queue()
        self.msg_rcv_q = queue.Queue()

    def send_msg(self, msg):
        diag_msg('entered', msg)

    def recv_msg(self) -> bytes:
        diag_msg('entered')
        msg = b""
        return msg

mock_send_recv = MockSendRecv()
