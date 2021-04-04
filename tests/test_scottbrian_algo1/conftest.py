"""conftest.py module for testing."""

from datetime import datetime, timedelta, timezone
import string
import time

import pytest
import pandas as pd  # type: ignore
from typing import Any, Tuple
import socket

from ibapi.connection import Connection
from ibapi.message import IN, OUT
from ibapi.comm import (make_field, make_msg, read_msg, read_fields)
from ibapi.common import NO_VALID_ID
from ibapi.errors import FAIL_CREATE_SOCK

from scottbrian_algo1.algo_api import AlgoApp
from scottbrian_utils.file_catalog import FileCatalog

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
        tmp_path: pytest fixture for providing a temporary path
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

        if self.port == mock_ib.PORT_FOR_SIMULATE_REQUEST_DISCONNECT:
            mock_ib.simulate_request_disconnect = True  # simulate disconnect
        else:
            mock_ib.simulate_request_disconnect = False

        if self.port == mock_ib.PORT_FOR_SIMULATE_REQUEST_TIMEOUT:
            mock_ib.simulate_request_timeout = True  # simulate timeout
        else:
            mock_ib.simulate_request_timeout = False

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
    symbols_path = d / "symbols.csv"
    stock_symbols_path = d / "stock_symbols.csv"
    symbol_status_path = d / "symbol_status.csv"
    contracts_path = d / "contracts.csv"
    contract_details_path = d / "contract_details.csv"
    catalog = FileCatalog({'symbols': symbols_path,
                           'stock_symbols': stock_symbols_path,
                           'symbols_status': symbol_status_path,
                           'contracts': contracts_path,
                           'contract_details': contract_details_path})

    a_algo_app = AlgoApp(catalog)
    return a_algo_app


class MockIB:
    """Class provides simulation data and methods for testing with ibapi."""

    PORT_FOR_REQID_TIMEOUT = 9001
    PORT_FOR_SIMULATE_REQUEST_DISCONNECT = 9002
    PORT_FOR_SIMULATE_REQUEST_TIMEOUT = 9003

    def __init__(self, test_cat):
        """Initialize the MockIB instance.

        Args:
            test_cat: catalog of data sets used for testing
        """
        self.test_cat = test_cat
        self.msg_rcv_q = queue.Queue()
        self.reqId_timeout = False
        self.simulate_request_disconnect = False
        self.simulate_request_timeout = False
        self.next_conId = 7000
        self.MAX_CONTRACT_DESCS_RETURNED = 16
        self.contract_descriptions = pd.DataFrame()

        self.build_contract_descriptions()

    def send_msg(self, msg):
        """Mock send to ib by interpreting and placing on receive queue.

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
        # Handle special test cases for request disconnect and timeout
        #######################################################################
        elif self.simulate_request_disconnect:  # if testing timeout case
            time.sleep(2)  # allow some time for request to get into wait loop
            recv_msg = b''  # simulate disconnect
        elif self.simulate_request_timeout:
            recv_msg = make_msg('0')  # simulate timeout
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
                build_msg = build_msg \
                    + make_field(match_descs.iloc[i].conId) \
                    + make_field(match_descs.iloc[i].symbol) \
                    + make_field(match_descs.iloc[i].secType) \
                    + make_field(match_descs.iloc[i].primaryExchange) \
                    + make_field(match_descs.iloc[i].currency) \
                    + make_field(len(match_descs.iloc[i].derivativeSecTypes))

                for dvt in match_descs.iloc[i].derivativeSecTypes:
                    build_msg = build_msg + make_field(dvt)

            recv_msg = make_msg(build_msg)

        #######################################################################
        # reqContractDetails
        #######################################################################
        elif int(fields[0]) == OUT.REQ_CONTRACT_DATA:
            logger.info('reqContractDetails detected')
            version = int(fields[1])
            reqId = int(fields[2])
            conId = int(fields[3])

            # construct start of receive message for wrapper
            start_msg = make_field(IN.CONTRACT_DATA) \
                + make_field(version) \
                + make_field(reqId)

            # find pattern matches in mock contract descriptions
            # fow now, just conId
            match_descs = self.contract_descriptions.loc[
                self.contract_descriptions['conId'] == conId]

            last_trade_date = '20220101'  # for now
            strike = 0  # for now
            right = 'P'  # for now
            exchange = 'SMART'  # for now
            market_name = 'MarketName' + str(conId)
            trading_class = 'TradingClass' + str(conId)
            min_tick = 0.01
            md_size_multiplier = 1
            multiplier = "2"
            order_types = 'OrderTypes' + str(conId)
            valid_exchanges = 'ValidExchanges' + str(conId)
            price_magnifier = 3
            under_conid = 12345
            long_name = 'LongName' + str(conId)

            contract_month = 'ContractMonth'
            industry = 'Industry'
            category = 'Category'
            subcategory = 'SubCategory'
            time_zone_id = 'TimeZoneId'
            trading_hours = 'TradingHours'
            liquid_hours = 'LiquidHours'
            ev_rule = 'EvRule'
            ev_multiplier = 4
            sec_id_list_count = 5   # will also need to try 0, 1, 2 for test
            sec_id_list = ['tag0', 'value0',
                           'tag1', 'value1',
                           'tag2', 'value2',
                           'tag3', 'value3',
                           'tag4', 'value4']

            agg_group = 6
            under_symbol = 'UnderSymbol'
            under_sec_type = 'UnderSecType'
            market_rule_ids = 'MarketRuleIds'
            real_expiration_date = 'RealExpirationDate'
            stock_type = 'StockType' + str(conId)

            for i in range(len(match_descs)):
                local_symbol = match_descs.iloc[i].symbol
                build_msg = start_msg \
                    + make_field(match_descs.iloc[i].symbol) \
                    + make_field(match_descs.iloc[i].secType) \
                    + make_field(last_trade_date) \
                    + make_field(strike) \
                    + make_field(right) \
                    + make_field(exchange) \
                    + make_field(match_descs.iloc[i].currency) \
                    + make_field(local_symbol) \
                    + make_field(market_name) \
                    + make_field(trading_class) \
                    + make_field(match_descs.iloc[i].conId) \
                    + make_field(min_tick) \
                    + make_field(md_size_multiplier) \
                    + make_field(multiplier) \
                    + make_field(order_types) \
                    + make_field(valid_exchanges) \
                    + make_field(price_magnifier) \
                    + make_field(under_conid) \
                    + make_field(long_name) \
                    + make_field(match_descs.iloc[i].primaryExchange) \
                    + make_field(contract_month) \
                    + make_field(industry) \
                    + make_field(category) \
                    + make_field(subcategory) \
                    + make_field(time_zone_id) \
                    + make_field(trading_hours) \
                    + make_field(liquid_hours) \
                    + make_field(ev_rule) \
                    + make_field(ev_multiplier) \
                    + make_field(sec_id_list_count)

                for j in range(2*sec_id_list_count):
                    build_msg += make_field(sec_id_list[j])

                build_msg += make_field(agg_group) \
                    + make_field(under_symbol) \
                    + make_field(under_sec_type) \
                    + make_field(market_rule_ids) \
                    + make_field(real_expiration_date) \
                    + make_field(stock_type)

                recv_msg = make_msg(build_msg)
                self.msg_rcv_q.put(recv_msg, timeout=5)

            build_msg = make_field(IN.CONTRACT_DATA_END) \
                + make_field(version) \
                + make_field(reqId)
            recv_msg = make_msg(build_msg)

        #######################################################################
        # queue the message to be received
        #######################################################################
        self.msg_rcv_q.put(recv_msg, timeout=5)

    def recv_msg(self) -> bytes:
        """Mock receive message from ib by getting it from the receive queue.

        Returns:
            Received message from ib for request sent earlier to ib

        """
        # if the queue is empty, the get will wait up to 1 second for an item
        # to be queued - if no item shows up, an Empty exception is raised
        msg = self.msg_rcv_q.get(timeout=1)  # wait for 1 second if empty

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
    @staticmethod
    def search_patterns():
        """Generator for search pattern strings.

        Yields:
            search pattern

        """
        for char1 in string.ascii_uppercase[0:17]:  # A-Q
            yield char1
            for char2 in string.ascii_uppercase[1:3] + '.':  # 'BC.'
                yield f'{char1}{char2}'
                for char3 in string.ascii_uppercase[2:5]:  # C-E
                    yield f'{char1}{char2}{char3}'
                    for char4 in string.ascii_uppercase[3:5] + '.':  # D-E
                        yield f'{char1}{char2}{char3}{char4}'

    @staticmethod
    def no_find_search_patterns():
        """Generator for search patterns that will not find symbols.

        Yields:
            search pattern

        """
        # one character
        for char1 in string.ascii_uppercase[17:20]:  # RST
            yield char1

        # two characters
        for char1 in string.ascii_uppercase[0:2]:  # ABC
            for char2 in string.ascii_uppercase[3:5]:  # 'DE'
                yield f'{char1}{char2}'

        # three characters
        for char1 in string.ascii_uppercase[0:2]:  # ABC
            for char2 in string.ascii_uppercase[1:3] + '.':  # 'BC.'
                for char3 in string.ascii_uppercase[5:7]:  # 'FG'
                    yield f'{char1}{char2}{char3}'

        # four characters
        for char1 in string.ascii_uppercase[0:2]:  # 'AB'
            for char2 in string.ascii_uppercase[1:3] + '.':  # 'BC.'
                for char3 in string.ascii_uppercase[2:4]:  # 'CD'
                    for char4 in string.ascii_uppercase[5:7]:  # 'FG'
                        yield f'{char1}{char2}{char3}{char4}'

        # five characters
        for char1 in string.ascii_uppercase[0:2]:  # 'AB'
            for char2 in string.ascii_uppercase[1:3] + '.':  # 'BC.'
                for char3 in string.ascii_uppercase[2:4]:  # 'CD'
                    for char4 in string.ascii_uppercase[3:5] + '.':  # 'DE.'
                        for char5 in string.ascii_uppercase[0:2]:  # 'AB'
                            yield f'{char1}{char2}{char3}{char4}{char5}'

    def build_desc(self, symbol):
        """Build the mock contract_descriptions.

        Args:
            symbol: symbol to be built
        """
        combos = self.get_combos(symbol)
        for combo in combos:
            self.next_conId += 1
            self.contract_descriptions = self.contract_descriptions.append(
                            pd.DataFrame([[self.next_conId,
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
                                                  'derivativeSecTypes'
                                                  ]))

    def build_contract_descriptions(self):
        """Build the set of contract descriptions to use for testing."""
        contract_descs_path = self.test_cat.get_path('mock_contract_descs')
        logger.info('mock_contract_descs path: %s', contract_descs_path)

        self.contract_descriptions = pd.DataFrame()

        for symbol in self.search_patterns():
            self.build_desc(symbol)

        logger.info('built mock_con_descs DataFrame with %d entries',
                    len(self.contract_descriptions))

    @staticmethod
    def get_combos(symbol: str
                   ) -> Tuple[Tuple[str, str, str, Tuple[str, ...]]]:
        """Get combos.

        Args:
            symbol: get combos for this symbol (must be valid)

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
                        ('STK', 'NYSE', 'USD', ('WAR', ))),
                  'M': (('STK', 'CBOE', 'USD', ('OPT', 'CFD', 'WAR', 'BAG')),
                        ('STK', 'NYSE', 'USD', ('OPT',))),
                  'N': (('STK', 'CBOE', 'USD', ('OPT', 'CFD', 'WAR', 'BAG')),
                        ('STK', 'NYSE', 'USD', ('OPT',)),
                        ('STK', 'BATS', 'USD', ('WAR', ))),
                  'O': (('STK', 'CBOE', 'USD', ('OPT', 'CFD', 'WAR', 'BAG')),
                        ('STK', 'NYSE', 'USD', ('OPT',)),
                        ('STK', 'BATS', 'USD', ('OPT',))),
                  'P': (('STK', 'CBOE', 'USD', ('OPT', 'CFD', 'WAR', 'BAG')),
                        ('STK', 'NYSE', 'USD', ('OPT',)),
                        ('STK', 'BATS', 'USD', ('OPT',)),
                        ('STK', 'AMSE', 'USD', ('OPT',))),
                  'Q': (('STK', 'CBOE', 'USD', ('OPT', 'CFD', 'WAR', 'BAG')),
                        ('STK', 'NYSE', 'USD', ('OPT',)),
                        ('STK', 'BATS', 'USD', ('OPT',)),
                        ('STK', 'BOSE', 'USD', ('OPT',)),
                        ('STK', 'AMSE', 'USD', ('OPT',)))
                  }

        return combos[first_char]


@pytest.fixture(scope='session')
def mock_ib() -> "MockIB":
    """Provide data and methods for testing with ib.

    Returns:
        An instance of MockIB
    """
    return MockIB(test_cat)
