"""conftest.py module for testing."""

from datetime import datetime, timedelta, timezone
import string

import pytest
import pandas as pd  # type: ignore
from typing import Any, cast, Tuple
import socket

from ibapi.connection import Connection
from ibapi.message import IN, OUT
from ibapi.comm import (make_field, make_msg, read_msg, read_fields)
from ibapi.common import NO_VALID_ID
from ibapi.errors import FAIL_CREATE_SOCK

from scottbrian_algo1.algo_api import AlgoApp
from scottbrian_utils.file_catalog import FileCatalog
from scottbrian_utils.diag_msg import diag_msg
import queue
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

proj_dir = Path.cwd().resolve().parents[1]  # back two directories

test_cat = \
    FileCatalog({'symbols': Path(proj_dir / 't_datasets/symbols.csv'),
                 'mock_contract_descs':
                     Path(proj_dir / 't_datasets/mock_contract_descs.csv')
                 })


@pytest.fixture(scope='function')
def algo_app(monkeypatch: Any,
             tmp_path: Any,
             mock_ib: "MockIB") -> "AlgoApp":
    """Instantiate and return an AlgoApp for testing.

    Args:
        monkeypatch: pytest fixture used to modify code for testing
        mock_ib: mock of some section of ibapi used for testing

    Returns:
        An instance of AlgoApp
    """
    def mock_connection_connect(self) -> None:
        """Mock connect routine.

        Args:
            self: instance of ib Connection class

        """
        logger.debug('entered')
        try:
            self.socket = socket.socket()
        # TO DO list the exceptions you want to catch
        except socket.error:
            logger.debug('socket.error exception')
            if self.wrapper:
                self.wrapper.error(NO_VALID_ID,
                                   FAIL_CREATE_SOCK.code(),
                                   FAIL_CREATE_SOCK.msg())

        if self.port == mock_ib.PORT_FOR_REQID_TIMEOUT:
            mock_ib.reqId_timeout = True  # simulate timeout
        else:
            mock_ib.reqId_timeout = False
        self.socket.settimeout(1)  # non-blocking

    monkeypatch.setattr(Connection, "connect", mock_connection_connect)

    def mock_connection_disconnect(self) -> None:
        """Mock disconnect routine.

        Args:
            self: instance of ib Connection class

        """
        self.lock.acquire()
        try:
            if self.socket is not None:
                logger.debug("disconnecting")
                self.socket = None
                logger.debug("disconnected")
                if self.wrapper:
                    self.wrapper.connectionClosed()
        finally:
            self.lock.release()

    monkeypatch.setattr(Connection, "disconnect", mock_connection_disconnect)

    def mock_connection_send_msg(self, msg: bytes) -> int:
        """Mock sendMsg routine.

        Args:
            self: instance of ib Connection class
            msg: message to be sent

        Returns:
              number of bytes sent

        Raises:
              queue.Full: message can not be added because queue is at limit

        """
        logger.debug('entered with msg: %s', msg)
        logger.debug("acquiring lock")
        self.lock.acquire()
        logger.debug("acquired lock")
        if not self.isConnected():
            logger.debug("sendMsg attempt while not connected, releasing lock")
            self.lock.release()
            return 0
        try:
            nSent = len(msg)
            mock_ib.send_msg(msg)
        except queue.Full:
            logger.debug("queue full exception on sendMsg attempt")
            raise
        finally:
            logger.debug("releasing lock")
            self.lock.release()
            logger.debug("released lock")

        logger.debug("sendMsg: number bytes sent: %d", nSent)
        return nSent

    monkeypatch.setattr(Connection, "sendMsg", mock_connection_send_msg)

    def mock_connection_recv_msg(self) -> bytes:
        """Mock recvMsg routine.

        Args:
            self: instance of ib Connection class

        Returns:
            message that was received

        """
        if not self.isConnected():
            logger.debug("recvMsg attempted while not connected")
            return b""
        try:
            buf = mock_ib.recv_msg()  # <-- mock
            # receiving 0 bytes outside a timeout means the connection is
            # either closed or broken
            if len(buf) == 0:
                logger.debug("socket either closed or broken, disconnecting")
                self.disconnect()
        except queue.Empty:
            logger.debug("timeout from recvMsg")
            buf = b""
        return buf

    monkeypatch.setattr(Connection, "recvMsg", mock_connection_recv_msg)

    d = tmp_path / "t_files"
    d.mkdir()
    p = d / "symbols.csv"
    catalog = FileCatalog({'symbols': p})

    a_algo_app = AlgoApp(catalog)
    return a_algo_app


class MockIB:
    """Class provides simulation data and methods for testing with ibapi."""

    def __init__(self, test_cat):
        """Initialize the MockIB instance.

        Args:
            test_cat: catalog of data sets used for testing
        """
        self.test_cat = test_cat
        self.msg_rcv_q = queue.Queue()
        self.reqId_timeout = False
        self.next_con_id = 7000
        self.MAX_CONTRACT_DESCS_RETURNED = 16
        self.PORT_FOR_REQID_TIMEOUT = 9001
        self.contract_descriptions = pd.DataFrame()

        self.build_contract_descriptions()

    def send_msg(self, msg):
        """Mock send to ib by interpresting and placing on receive queue.

        Args:
            msg: message to be sent (i.e., interpreted and queued)

        """
        logger.debug('entered with message %s', msg)
        (size, msg2, buf) = read_msg(msg)
        logger.debug('size, msg, buf: %d, %s, %s ', size, msg2, buf)
        fields = read_fields(msg2)
        logger.debug('fields: %s', fields)

        recv_msg = b''
        #######################################################################
        # get version and connect time (special case - not decode able)
        #######################################################################
        if msg == b'API\x00\x00\x00\x00\tv100..157':
            current_dt = datetime.now(
                tz=timezone(offset=timedelta(hours=5))).\
                strftime('%Y%m%d %H:%M:%S')

            # recv_msg = b'\x00\x00\x00\x1a157\x0020210301 23:43:23 EST\x00'
            recv_msg = b'\x00\x00\x00\x1a157\x00' \
                       + current_dt.encode('utf-8') + b' EST\x00'

        #######################################################################
        # reqId (get next valid requestID)
        # b'\x00\x00\x00\x0871\x002\x000\x00\x00'
        #######################################################################
        elif int(fields[0]) == OUT.START_API:
            logger.info('startAPI detected')
            # recv_msg = b'\x00\x00\x00\x069\x001\x001\x00'
            if self.reqId_timeout:  # if testing timeout case
                recv_msg = make_msg('0')  # simulate timeout
            else:  # build the normal next valid id message
                recv_msg = make_msg(make_field(IN.NEXT_VALID_ID)
                                    + make_field('1')
                                    + make_field('1'))
            logger.debug('recv_msg: %s', recv_msg)
        #######################################################################
        # reqMatchingSymbols
        #######################################################################
        elif int(fields[0]) == OUT.REQ_MATCHING_SYMBOLS:
            logger.info('reqMatchingSymbols detected')
            reqId = int(fields[1])
            pattern = fields[2].decode(errors='backslashreplace')
            logger.debug('pattern: %s', pattern)

            # construct start of receive message for wrapper
            build_msg = make_field(IN.SYMBOL_SAMPLES) + make_field(reqId)

            # find pattern matches in mock contract descriptions
            match_descs = self.contract_descriptions.loc[
                self.contract_descriptions['symbol'].str.
                startswith(pattern)]

            # limit the number found as ib does
            num_found = min(self.MAX_CONTRACT_DESCS_RETURNED,
                            match_descs.shape[0])

            # add the number of descriptions to the receive message
            build_msg = build_msg + make_field(num_found)

            for i in range(num_found):
                conId = match_descs.iloc[i].conId
                symbol = match_descs.iloc[i].symbol
                secType = match_descs.iloc[i].secType
                primaryExchange = match_descs.iloc[i].primaryExchange
                currency = match_descs.iloc[i].currency
                # derivative_types = []
                # if match_descs.iloc[i].deriv_0 != '0':
                #     derivative_types.append(match_descs.iloc[i].deriv_0)
                build_msg = build_msg + make_field(conId) \
                    + make_field(symbol) \
                    + make_field(secType) \
                    + make_field(primaryExchange) \
                    + make_field(currency) \
                    + make_field(len(match_descs.iloc[i].derivative_types))

                for dvt in match_descs.iloc[i].derivative_types:
                    build_msg = build_msg + make_field(dvt)

            recv_msg = make_msg(build_msg)
            # diag_msg('recv_msg:', recv_msg)
        #######################################################################
        # queue the message to be received
        #######################################################################
        self.msg_rcv_q.put(recv_msg, timeout=5)

    def recv_msg(self) -> bytes:
        """Mock receive message from ib by getting it from the receive queue.

        Returns:
            Received message from ib for request sent earlier to ib

        """
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
        combos = self.get_combos(symbol)
        for combo in combos:
            self.next_con_id += 1
            self.contract_descriptions = self.contract_descriptions.append(
                            pd.DataFrame([[self.next_con_id,
                                           symbol,
                                           combo[0],
                                           combo[1],
                                           combo[2],
                                           combo[3],
                                           ]],
                                         columns=['conId',
                                                  'symbol',
                                                  'secType',
                                                  'primaryExchange',
                                                  'currency',
                                                  'derivative_types'
                                                  ]))

        # diag_msg('built contract descriptors:', self.contract_descriptions)
    def build_contract_descriptions(self):
        """Build the set of contract descriptions to use for testing."""
        contract_descs_path = self.test_cat.get_path('mock_contract_descs')
        logger.info('mock_contract_descs path: %s', contract_descs_path)

        # if contract_descs_path.exists():
        #     self.contract_descriptions = pd.read_csv(contract_descs_path,
        #                                              header=0,
        #                                              index_col=0)
        # else:
        self.contract_descriptions = pd.DataFrame()

        for chr1 in string.ascii_uppercase[0:12]:  # A-L
            self.build_desc(chr1)
            for chr2 in string.ascii_uppercase[1:4]:  # B-D
                self.build_desc(chr1 + chr2)
                for chr3 in string.ascii_uppercase[2:5]:  # C-E
                    self.build_desc(chr1 + chr2 + chr3)
                    for chr4 in string.ascii_uppercase[3:6]:  # D-F
                        self.build_desc(chr1 + chr2 + chr3 + chr4)
                        for chr5 in string.ascii_uppercase[4:7]:  # E-G
                            self.build_desc(chr1 + chr2 + chr3 + chr4 +
                                            chr5)
                            for chr6 in string.ascii_uppercase[5:8]:  # F-H
                                self.build_desc(chr1 + chr2 + chr3 + chr4
                                                + chr5 + chr6)
        logger.info('built mock_con_descs DataFrame with %d entries',
                    len(self.contract_descriptions))
        # diag_msg('built contract descriptors:', self.contract_descriptions)
        # logger.info('saving mock_contract_descs DataFrame to csv')
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
    def get_combos(symbol: str
                   ) -> Tuple[Tuple[str, str, str, Tuple[str, ...]]]:
        """Get combos.

        Args:
            symbol: get combos for this symbol

        Returns:
            List of lists of combos
        """
        first_char = symbol[0]
        combos = {'A': (('STK', 'CBOE', 'EUR', ()),),
                  'B': (('IND', 'CBOE', 'USD', ('BAG', )),),
                  'C': (('STK', 'CBOE', 'USD', ('WAR',)),),
                  'D': (('STK', 'CBOE', 'USD', ('WAR', 'BAG')),
                        ('IND', 'CBOE', 'USD', ('CFD', ))),
                  'E': (('STK', 'CBOE', 'USD', ('CFD', 'BAG')),
                        ('STK', 'NYSE', 'EUR', ('CFD', 'WAR'))),
                  'F': (('STK', 'CBOE', 'USD', ('CFD', 'WAR', 'BAG')),
                        ('STK', 'NYSE', 'USD', ())),
                  'G': (('STK', 'CBOE', 'EUR', ('OPT', )),),
                  'H': (('IND', 'CBOE', 'USD', ('OPT', 'BAG')),),
                  'I': (('STK', 'CBOE', 'USD', ('OPT', 'WAR')),),
                  'J': (('STK', 'CBOE', 'USD', ('OPT', 'WAR', 'BAG')),
                        ('IND', 'CBOE', 'USD', ('OPT', 'CFD'))),
                  'K': (('STK', 'CBOE', 'USD', ('OPT', 'CFD', 'BAG')),
                        ('STK', 'NYSE', 'EUR', ('OPT', 'CFD', 'WAR'))),
                  'L': (('STK', 'CBOE', 'USD', ('OPT', 'CFD', 'WAR', 'BAG')),
                        ('STK', 'NYSE', 'USD', ('OPT', )))
                  }

        return combos[first_char]


@pytest.fixture(scope='session')
def mock_ib() -> "MockIB":
    """Provide data and methods for testing with ib.

    Returns:
        An instance of MockIB
    """
    return MockIB(test_cat)


nonexistent_symbol_arg_list = ['A',
                               'AB',
                               'ABC',
                               'ABCD',
                               'ABCDE',
                               'ABCDEF',
                               'B',
                               'BB',
                               'BBC',
                               'BBCD',
                               'BBCDE',
                               'BBCDEF',
                               'C',
                               'D',
                               'E',
                               'F',
                               'G',
                               'H'
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


symbol_pattern_match_1_arg_list = ['IBCDEF',
                                   'ICDEFG',
                                   'IDEFGH',
                                   'JBCDEF',
                                   'JDDDGH',
                                   'KCCFFF'
                                   ]


@pytest.fixture(params=symbol_pattern_match_1_arg_list)  # type: ignore
def symbol_pattern_match_1_arg(request: Any) -> str:
    """Provide symbol patterns that are in the mock contract descriptions.

    Args:
        request: pytest fixture that returns the fixture params

    Returns:
        The params values are returned one at a time
    """
    return cast(str, request.param)
