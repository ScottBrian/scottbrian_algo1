"""conftest.py module for testing."""

from datetime import datetime, timedelta, timezone
# from datetime import timedelta

import pytest

#from ibapi.client import EClient  # type: ignore
from ibapi.connection import Connection  # type: ignore
from ibapi.connection import *
from scottbrian_algo1.algo_api import AlgoApp
from scottbrian_utils.file_catalog import FileCatalog
from scottbrian_utils.diag_msg import diag_msg
import queue
from pathlib import Path

proj_dir = Path.cwd().resolve().parents[1]  # back two directories

test_cat = FileCatalog({'symbols': Path(proj_dir / 't_datasets/symbols.csv')})


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
        # diag_msg('entered')
        try:
            self.socket = socket.socket()
        # TO DO list the exceptions you want to catch
        except socket.error:
            # diag_msg('socket instantiation failed')
            if self.wrapper:
                self.wrapper.error(NO_VALID_ID, FAIL_CREATE_SOCK.code(), FAIL_CREATE_SOCK.msg())

        try:
            pass   # <-- bypass real
            # self.socket.connect((self.host, self.port))  # <--- real
        except socket.error:
            # diag_msg('socket error')
            if self.wrapper:
                self.wrapper.error(NO_VALID_ID, CONNECT_FAIL.code(), CONNECT_FAIL.msg())

        self.socket.settimeout(1)  # non-blocking

        # diag_msg('exiting')

    monkeypatch.setattr(Connection, "connect", mock_connection_connect)

    def mock_connection_disconnect(self):
        # diag_msg('entered')
        self.lock.acquire()
        try:
            if self.socket is not None:
                logger.debug("disconnecting")
                # self.socket.close()  # <-- real
                self.socket = None
                logger.debug("disconnected")
                if self.wrapper:
                    self.wrapper.connectionClosed()
        finally:
            # diag_msg('about to release conn lock')
            self.lock.release()
            # diag_msg('conn lock released')
        # diag_msg('exiting')

    monkeypatch.setattr(Connection, "disconnect", mock_connection_disconnect)

    def mock_connection_sendMsg(self, msg):
        # diag_msg('entered with msg:', msg)
        logger.debug("acquiring lock")
        self.lock.acquire()
        logger.debug("acquired lock")
        if not self.isConnected():
            logger.debug("sendMsg attempted while not connected, releasing lock")
            self.lock.release()
            return 0
        try:
            # nSent = self.socket.send(msg)  # <-- real
            nSent = len(msg)  # <-- mock
            mock_send_recv.send_msg(msg)  # <-- mock
        # except socket.error:  # <-- real
        except queue.Full:  # <-- mock
            logger.debug("exception from sendMsg %s", sys.exc_info())
            raise
        finally:
            logger.debug("releasing lock")
            self.lock.release()
            logger.debug("release lock")

        logger.debug("sendMsg: sent: %d", nSent)
        # diag_msg('exiting')
        return nSent

    monkeypatch.setattr(Connection, "sendMsg", mock_connection_sendMsg)

    def mock_connection_recvMsg(self):
        if not self.isConnected():
            logger.debug("recvMsg attempted while not connected, releasing lock")
            return b""
        try:
            buf = mock_send_recv.recv_msg()  # <-- mock
            # buf = self._recvAllMsg()  # <-- real
            # receiving 0 bytes outside a timeout means the connection is either
            # closed or broken
            if len(buf) == 0:
                logger.debug("socket either closed or broken, disconnecting")
                self.disconnect()
        # except socket.timeout:  # <-- real
        except queue.Empty:  # <-- mock
            logger.debug("socket timeout from recvMsg %s", sys.exc_info())
            buf = b""
        # except socket.error:  # <<- real
        #     logger.debug("socket broken, disconnecting")  # <<- real
        #     self.disconnect()  # <<- real
        #     buf = b""  # <<- real

        return buf
    monkeypatch.setattr(Connection, "recvMsg", mock_connection_recvMsg)
    a_algo_app = AlgoApp(test_cat)
    return a_algo_app

class MockSendRecv:
    def __init__(self):
        self.msg_rcv_q = queue.Queue()

    def send_msg(self, msg):
        # diag_msg('entered', msg)
        recv_msg = b''
        #######################################################################
        # get version and connect time
        #######################################################################
        if msg == b'API\x00\x00\x00\x00\tv100..157':
            current_dt = datetime.now(
                tz=timezone(offset=timedelta(hours=5))).strftime('%Y%m%d %H:%M:%S')

            # recv_msg = b'\x00\x00\x00\x1a157\x0020210301 23:43:23 EST\x00'
            recv_msg = b'\x00\x00\x00\x1a157\x00' \
                       + current_dt.encode('utf-8') + b' EST\x00'

        #######################################################################
        # get next requestID
        #######################################################################
        elif msg == b'\x00\x00\x00\x0871\x002\x000\x00\x00':
            recv_msg = b'\x00\x00\x00\x069\x001\x001\x00'

        self.msg_rcv_q.put(recv_msg, timeout=5)


    def recv_msg(self) -> bytes:
        # diag_msg('entered')
        msg = self.msg_rcv_q.get(timeout=1)  # wait for 1 second if empty
        # diag_msg('exit with msg:', msg)
        return msg

mock_send_recv = MockSendRecv()
