"""conftest.py module for testing."""

from datetime import datetime, timedelta, timezone
import string

import pytest
import pandas as pd  # type: ignore
from typing import Any, cast, List

#from ibapi.client import EClient  # type: ignore
from ibapi.connection import Connection  # type: ignore
from ibapi.message import IN, OUT
from ibapi.connection import *
from ibapi.comm import (make_field, make_msg, read_msg, read_fields)
from scottbrian_algo1.algo_api import AlgoApp
from scottbrian_utils.file_catalog import FileCatalog
from scottbrian_utils.diag_msg import diag_msg
import queue
from pathlib import Path

proj_dir = Path.cwd().resolve().parents[1]  # back two directories

test_cat = \
    FileCatalog({'symbols': Path(proj_dir / 't_datasets/symbols.csv'),
                 'mock_contract_descs':
                     Path(proj_dir / 't_datasets/mock_contract_descs.csv')
                 })


@pytest.fixture(scope='function')
def algo_app(monkeypatch,
             mock_ib) -> "AlgoApp":
    """Instantiate and return an AlgoApp for testing.

    Returns:
        An instance of AlgoApp
    """
    def mock_connection_connect(self):
        logger.debug('entered')
        logger.debug('mock_ib.msg_rcv_q.empty() %s', mock_ib.msg_rcv_q.empty())
        try:
            self.socket = socket.socket()
        # TO DO list the exceptions you want to catch
        except socket.error:
            # diag_msg('socket instantiation failed')
            if self.wrapper:
                self.wrapper.error(NO_VALID_ID, FAIL_CREATE_SOCK.code(), FAIL_CREATE_SOCK.msg())

        try:
            if self.port == mock_ib.PORT_FOR_REQID_TIMEOUT:
                mock_ib.reqId_timeout = True
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
            mock_ib.send_msg(msg)  # <-- mock
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
            buf = mock_ib.recv_msg()  # <-- mock
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


# class MockSendRecv:
#     def __init__(self, mock_ib):
#         self.msg_rcv_q = queue.Queue()
#         self.reqId_timeout = False
#         self.mock_ib = mock_ib
#
#     def send_msg(self, msg):
#         diag_msg('entered', msg)
#         (size, msg2, buf) = read_msg(msg)
#         diag_msg('msg size:', size)
#         diag_msg('msg2:', msg2)
#         diag_msg('buf:', buf)
#
#         fields = read_fields(msg2)
#         diag_msg('fields:', fields)
#
#         recv_msg = b''
#         #######################################################################
#         # get version and connect time (special case - not decode able)
#         #######################################################################
#         if msg == b'API\x00\x00\x00\x00\tv100..157':
#             current_dt = datetime.now(
#                 tz=timezone(offset=timedelta(hours=5))).strftime('%Y%m%d %H:%M:%S')
#
#             # recv_msg = b'\x00\x00\x00\x1a157\x0020210301 23:43:23 EST\x00'
#             recv_msg = b'\x00\x00\x00\x1a157\x00' \
#                        + current_dt.encode('utf-8') + b' EST\x00'
#
#         #######################################################################
#         # reqId (get next valid requestID)
#         # b'\x00\x00\x00\x0871\x002\x000\x00\x00'
#         #######################################################################
#         elif int(fields[0]) == OUT.START_API:
#             diag_msg('startAPI detected')
#             # recv_msg = b'\x00\x00\x00\x069\x001\x001\x00'
#             if self.reqId_timeout:
#                 recv_msg = make_msg('0')
#             else:
#                 msg3 = make_field(IN.NEXT_VALID_ID) \
#                         + make_field('1') \
#                         + make_field('1')
#                 diag_msg('msg3:', msg3)
#                 msg4 = make_msg(msg3)
#                 diag_msg('msg4:', msg4)
#                 recv_msg = make_msg(make_field(IN.NEXT_VALID_ID)
#                                 + make_field('1')
#                                 + make_field('1'))
#             diag_msg('recv_msg:', recv_msg)
#         #######################################################################
#         # reqMatchingSymbols
#         #######################################################################
#         elif int(fields[0]) == OUT.REQ_MATCHING_SYMBOLS:
#             diag_msg('reqMatchingSymbols detected')
#             reqId = int(fields[1])
#             pattern = fields[2].decode(errors='backslashreplace')
#             diag_msg('pattern:', pattern)
#             diag_msg('type(pattern):', type(pattern))
#             build_msg = make_field(IN.SYMBOL_SAMPLES) \
#                 + make_field(reqId)
#
#             # scan each record to see if pattern matches
#             diag_msg('contract descriptions:',
#                      self.mock_ib.contract_descriptions)
#             match_descs = self.mock_ib.contract_descriptions.loc[
#                 self.mock_ib.contract_descriptions['symbol'].str.
#                     startswith(pattern)]
#             diag_msg('match_descs:', match_descs)
#             num_found = min(MAX_CONTRACT_DESCS_RETURNED, match_descs.shape[0])
#             build_msg = build_msg + make_field(num_found)
#             for i in range(num_found):
#                 conId = match_descs.iloc[i].conId
#                 symbol = match_descs.iloc[i].symbol
#                 secType = match_descs.iloc[i].secType
#                 primaryExchange = match_descs.iloc[i].primaryExchange
#                 currency = match_descs.iloc[i].currency
#                 build_msg = build_msg + make_field(conId) \
#                             + make_field(symbol) \
#                             + make_field(secType) \
#                             + make_field(primaryExchange) \
#                             + make_field(currency) \
#                             + make_field(0)  # zero derivativeSecTypes for now
#             recv_msg = make_msg(build_msg)
#             diag_msg('recv_msg:', recv_msg)
#         #######################################################################
#         # queue the message to be received
#         #######################################################################
#         self.msg_rcv_q.put(recv_msg, timeout=5)
#
#
#     def recv_msg(self) -> bytes:
#         # diag_msg('entered')
#         msg = self.msg_rcv_q.get(timeout=1)  # wait for 1 second if empty
#         # diag_msg('exit with msg:', msg)
#         return msg
#
#
# @pytest.fixture(scope='function')
# def mock_send_recv(mock_ib) -> "MockSendRecv":
#     """Provide a list of symbols for testing.
#
#     Returns:
#         An instance of MockSendRecv
#     """
#     return MockSendRecv(mock_ib)



class MockIB:
    """Class provides simulation data and methods for testing with ibapi."""

    def __init__(self, test_cat):
        self.test_cat = test_cat
        self.msg_rcv_q = queue.Queue()
        self.reqId_timeout = False
        self.next_con_id = 7000
        self.MAX_CONTRACT_DESCS_RETURNED = 16
        self.PORT_FOR_REQID_TIMEOUT = 9001
        self.valid_first_chars = ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I',
                                 'J', 'K', 'L', 'M', 'N', 'O')
        self.valid_second_chars = ('C', 'D', 'F')
        self.valid_third_chars = ('D', 'F', 'G')
        self.valid_fourth_chars = ('H', 'J', 'L')
        self.valid_fifth_chars = ('S', 'B', 'T')
        self.valid_sixth_chars = ('X', 'Y', 'Z')
        self.contract_descriptions = pd.DataFrame()

        self.build_contract_descriptions()

    def send_msg(self, msg):
        diag_msg('entered', msg)
        (size, msg2, buf) = read_msg(msg)
        diag_msg('msg size:', size)
        diag_msg('msg2:', msg2)
        diag_msg('buf:', buf)

        fields = read_fields(msg2)
        diag_msg('fields:', fields)

        recv_msg = b''
        #######################################################################
        # get version and connect time (special case - not decode able)
        #######################################################################
        if msg == b'API\x00\x00\x00\x00\tv100..157':
            current_dt = datetime.now(
                tz=timezone(offset=timedelta(hours=5))).strftime('%Y%m%d %H:%M:%S')

            # recv_msg = b'\x00\x00\x00\x1a157\x0020210301 23:43:23 EST\x00'
            recv_msg = b'\x00\x00\x00\x1a157\x00' \
                       + current_dt.encode('utf-8') + b' EST\x00'

        #######################################################################
        # reqId (get next valid requestID)
        # b'\x00\x00\x00\x0871\x002\x000\x00\x00'
        #######################################################################
        elif int(fields[0]) == OUT.START_API:
            diag_msg('startAPI detected')
            # recv_msg = b'\x00\x00\x00\x069\x001\x001\x00'
            if self.reqId_timeout:
                recv_msg = make_msg('0')
            else:
                msg3 = make_field(IN.NEXT_VALID_ID) \
                        + make_field('1') \
                        + make_field('1')
                diag_msg('msg3:', msg3)
                msg4 = make_msg(msg3)
                diag_msg('msg4:', msg4)
                recv_msg = make_msg(make_field(IN.NEXT_VALID_ID)
                                + make_field('1')
                                + make_field('1'))
            diag_msg('recv_msg:', recv_msg)
        #######################################################################
        # reqMatchingSymbols
        #######################################################################
        elif int(fields[0]) == OUT.REQ_MATCHING_SYMBOLS:
            diag_msg('reqMatchingSymbols detected')
            reqId = int(fields[1])
            pattern = fields[2].decode(errors='backslashreplace')
            diag_msg('pattern:', pattern)
            diag_msg('type(pattern):', type(pattern))
            build_msg = make_field(IN.SYMBOL_SAMPLES) \
                + make_field(reqId)

            # scan each record to see if pattern matches
            diag_msg('contract descriptions:',
                     self.contract_descriptions)
            match_descs = self.contract_descriptions.loc[
                self.contract_descriptions['symbol'].str.
                    startswith(pattern)]
            diag_msg('match_descs:', match_descs)
            num_found = min(self.MAX_CONTRACT_DESCS_RETURNED,
                            match_descs.shape[0])
            build_msg = build_msg + make_field(num_found)
            for i in range(num_found):
                conId = match_descs.iloc[i].conId
                symbol = match_descs.iloc[i].symbol
                secType = match_descs.iloc[i].secType
                primaryExchange = match_descs.iloc[i].primaryExchange
                currency = match_descs.iloc[i].currency
                build_msg = build_msg + make_field(conId) \
                            + make_field(symbol) \
                            + make_field(secType) \
                            + make_field(primaryExchange) \
                            + make_field(currency) \
                            + make_field(0)  # zero derivativeSecTypes for now
            recv_msg = make_msg(build_msg)
            diag_msg('recv_msg:', recv_msg)
        #######################################################################
        # queue the message to be received
        #######################################################################
        self.msg_rcv_q.put(recv_msg, timeout=5)


    def recv_msg(self) -> bytes:
        # diag_msg('entered')
        # if the queue is empty, the get will wait up to 1 second for an item
        # to be queued - if no item shows up, an Empty exception is raised
        msg = self.msg_rcv_q.get(timeout=1)  # wait for 1 second if empty
        # diag_msg('exit with msg:', msg)
        return msg
    ###########################################################################
    # since ib will return only 16 symbols per request, we need to
    # create a table with earlier entries being the single character symbols
    # and longer symbols later in the table. We also need:
    #  1) symbols with same name but different primary exchange
    #  2) symbols with USD and non-USD for currency
    #  3) symbols with STK and non-STK
    #  4) symbols starting with one char and going up to 6 chars, and
    #     enough of each to drive the recursion code to 6 char exact names
    ###########################################################################
    def build_desc(self, symbol):
        """Build the mock contract_descriptions.

        Args:
            symbol: symbol to be built
        """
        # combos = self.get_combos(symbol)
        combos = (('STK', 'CBOE', 'USD'),)
        for combo in combos:
            self.next_con_id += 1
            self.contract_descriptions = self.contract_descriptions.append(
                            pd.DataFrame([[self.next_con_id,
                                           symbol,
                                           combo[0],
                                           combo[1],
                                           combo[2]
                                           ]],
                                         columns=['conId',
                                                  'symbol',
                                                  'secType',
                                                  'primaryExchange',
                                                  'currency']))

        # diag_msg('built contract descriptors:', self.contract_descriptions)
    def build_contract_descriptions(self):
        contract_descs_path = self.test_cat.get_path('mock_contract_descs')
        logger.info('mock_contract_descs path: %s', contract_descs_path)

        # if contract_descs_path.exists():
        #     self.contract_descriptions = pd.read_csv(contract_descs_path,
        #                                              header=0,
        #                                              index_col=0)
        # else:
        self.contract_descriptions = pd.DataFrame()

        # for chr1 in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
        #              'L', 'M', 'N', 'O']:
        for chr1 in string.ascii_uppercase[0:3]:
            self.build_desc(chr1)
            for chr2 in string.ascii_uppercase[1:4]:  # ['C', 'D', 'F']:
                self.build_desc(chr1 + chr2)
                for chr3 in string.ascii_uppercase[2:5]:  # ['D', 'F', 'G']:
                    self.build_desc(chr1 + chr2 + chr3)
                    # for chr4 in string.ascii_uppercase[3:6]:  # ['H', 'J', 'L']:
                    #     self.build_desc(chr1 + chr2 + chr3 + chr4)
                    #     for chr5 in string.ascii_uppercase[4:7]:  # ['S', 'B', 'T']:
                    #         self.build_desc(chr1 + chr2 + chr3 + chr4 +
                    #                         chr5)
                    #         for chr6 in string.ascii_uppercase[5:8]:  # ['X', 'Y', 'Z']:
                    #             self.build_desc(chr1 + chr2 + chr3 + chr4
                    #                             + chr5 + chr6)
        logger.info('built mock_con_descs DataFrame with %d entries',
                    len(self.contract_descriptions))
        logger.info('saving mock_contract_descs DataFrame to csv')
        # self.contract_descriptions.to_csv(contract_descs_path)

    # def get_non_existent_symbols(self,
    #                              num_to_get: int,
    #                              secType: str,
    #                              currency: str) -> List[str]:
    #     # symbols that does not exist
    #     for i in range(num_to_get):

        # symbols that exists but not for secType
        # symbols that exists but not for currency

    @staticmethod
    def get_combos(symbol: str) -> List[List[str]]:
        """Get combos.

        Args:
            symbol: get combos for this symbol

        """
        combos = (('CASH', 'ISLAND', 'EUR'),
                  ('STK', 'CBOE', 'USD'),
                  ('IND', 'NYSE', 'GBP'),
                  ('OPT', 'BOX', 'YEN'),
                  ('STK', 'ISLAND', 'USD'),
                  ('OPT', 'NYSE', 'USD'),
                  ('IND', 'BOX', 'YEN'),
                  ('STK', 'BOX', 'GBP')
                  )
        allow_first_char = {'A': {0, 1, 2, 3, 4, 5, 6, 7},
                            'B': {0, 1, 2, 3},
                            'C': {4, 5, 6, 7},
                            'D': {0, 1},
                            'E': {2, 3},
                            'F': {4, 5},
                            'G': {6, 7},
                            'H': {0},
                            'I': {1},
                            'J': {2},
                            'K': {3},
                            'L': {4},
                            'M': {5},
                            'N': {6},
                            'O': {7}}
        allow_mid_char = {'F': {0, 1, 2, 3, 4, 5, 6, 7},
                          'D': {1},
                          'G': {3}
                          }
        allow_last_char = {'Y': {0, 1, 2, 3, 4, 5, 6, 7},
                           'X': {4},
                           'Z': {7}
                           }
        allow_combo = allow_first_char[symbol[0]]
        if len(symbol) == 5 and symbol[2] in allow_mid_char:
            allow_combo = allow_combo | allow_mid_char[symbol[2]]
        if len(symbol) == 6 and symbol[5] in allow_last_char:
            allow_combo = allow_combo | allow_last_char[symbol[5]]
        ret_combo = []
        for i in range(8):
            if i in allow_combo:
                ret_combo.append(list(combos[i]))

        return ret_combo


@pytest.fixture(scope='session')
def mock_ib() -> "MockIB":
    """Provide data and methods for testing with ib.

    Returns:
        An instance of MockIB
    """
    return MockIB(test_cat)


nonexistent_symbol_arg_list = ['AA',
                               'DA'
                               ]

@pytest.fixture(params=nonexistent_symbol_arg_list)  # type: ignore
def nonexistent_symbol_arg(request: Any) -> str:
    """Provide symbol patterns that are not in the mock contract descriptions.

    Args:
        request: pytest fixture that returns the fixture params

    Returns:
        The params values are returned one at a time
    """
    return cast(str, request.param)

symbol_pattern_arg_list = ['A',
                           'B'
                           ]


@pytest.fixture(params=symbol_pattern_arg_list)  # type: ignore
def symbol_pattern_arg(request: Any) -> str:
    """Provide symbol patterns that are in the mock contract descriptions.

    Args:
        request: pytest fixture that returns the fixture params

    Returns:
        The params values are returned one at a time
    """
    return cast(str, request.param)
