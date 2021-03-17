"""scottbrian_algo1 algo_api.

========
algo_api
========

With algo_api you can connect to the IBAPI and request information and make
trades.

"""

import pandas as pd  # type: ignore
from threading import Event, get_ident, get_native_id, Thread, Lock
# from pathlib import Path

import time

from ibapi.wrapper import EWrapper  # type: ignore
# from ibapi import utils
from ibapi.client import EClient  # type: ignore
from ibapi.utils import current_fn_name  # type: ignore

# types
from ibapi.common import ListOfContractDescription  # type: ignore
# from ibapi.order_condition import *  # @UnusedWildImport
# from ibapi.contract import *  # @UnusedWildImport
# from ibapi.order import *  # @UnusedWildImport
# from ibapi.order_state import *  # @UnusedWildImport
# from ibapi.execution import Execution
# from ibapi.execution import ExecutionFilter
# from ibapi.commission_report import CommissionReport
# from ibapi.ticktype import *  # @UnusedWildImport
# from ibapi.tag_value import TagValue
#
# from ibapi.account_summary_tags import *

from typing import Type, TYPE_CHECKING
import string

from scottbrian_utils.file_catalog import FileCatalog
from scottbrian_utils.diag_msg import get_formatted_call_sequence

# from datetime import datetime
import logging

# set logging for debug for now until things ar working
logging.basicConfig(filename='AlgoApp.log',
                    filemode='w',
                    level=logging.DEBUG,
                    format='%(asctime)s '
                           '%(levelname)s '
                           '%(filename)s:'
                           '%(funcName)s:'
                           '%(lineno)d '
                           '%(message)s')

logger = logging.getLogger(__name__)


class AlgoAppError(Exception):
    """Base class for exception in this module."""
    pass


class AlreadyConnected(AlgoAppError):
    """AlgoApp exception for an attempt to connect when already connected."""
    pass


class DisconnectLockHeld(AlgoAppError):
    """Attempted to connect while the disconnect lock is held."""


class ConnectTimeout(AlgoAppError):
    """Connect timeout waiting for nextValid_ID event."""


class AlgoApp(EWrapper, EClient):  # type: ignore
    """AlgoApp class."""

    PORT_FOR_LIVE_TRADING = 7496
    PORT_FOR_PAPER_TRADING = 7497

    ###########################################################################
    # __init__
    ###########################################################################
    def __init__(self, ds_catalog: FileCatalog) -> None:
        """Instantiate the AlgoApp.

        Args:
            ds_catalog: contain the paths for data sets

        :Example: instantiate AlgoApp and print it

        >>> from scottbrian_algo1.algo_api import AlgoApp
        >>> from scottbrian_utils.file_catalog import FileCatalog
        >>> from pathlib import Path
        >>> test_cat = FileCatalog({'symbols': Path('t_datasets/symbols.csv')})
        >>> algo_app = AlgoApp(test_cat)
        >>> print(algo_app)
        AlgoApp(ds_catalog)

        """
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.disconnect_lock = Lock()
        self.ds_catalog = ds_catalog
        self.request_id: int = 0

        # stock symbols
        self.symbols_status = pd.DataFrame()
        self.num_stock_symbols_received = 0
        self.stock_symbols = pd.DataFrame()

        self.response_complete_event = Event()
        self.nextValidId_event = Event()
        self.run_thread = Thread(target=self.run)

    ###########################################################################
    # __repr__
    ###########################################################################
    def __repr__(self) -> str:
        """Return a representation of the class.

        Returns:
            The representation as how the class is instantiated

        :Example: instantiate AlgoApp and print it

        >>> from scottbrian_algo1.algo_api import AlgoApp
        >>> from scottbrian_utils.file_catalog import FileCatalog
        >>> from pathlib import Path
        >>> test_cat = FileCatalog({'symbols': Path('t_datasets/symbols.csv')})
        >>> algo_app = AlgoApp(test_cat)
        >>> print(algo_app)
        AlgoApp(ds_catalog)

        """
        if TYPE_CHECKING:
            __class__: Type[AlgoApp]
        classname = self.__class__.__name__
        parms = 'ds_catalog'

        return f'{classname}({parms})'

    ###########################################################################
    # error
    ###########################################################################
    def error(self, reqId: int, errorCode: int, errorString: str) -> None:
        """Receive error from IB and print it.

        Args:
            reqId: the id of the failing request
            errorCode: the error code
            errorString: text to explain the error

        """
        self.logAnswer(current_fn_name(), vars())
        logger.error("ERROR %s %s %s", reqId, errorCode, errorString)
        print("Error: ", reqId, " ", errorCode, " ", errorString)

    ###########################################################################
    # nextValidId
    ###########################################################################
    def nextValidId(self, request_id: int) -> None:
        """Receive next valid ID from IB and save it.

        Args:
            request_id: next id to use for a request to IB

        """
        logger.info('next valid ID is %i', request_id)
        self.request_id = request_id
        self.nextValidId_event.set()

    ###########################################################################
    # get_req_id
    ###########################################################################
    def get_req_id(self) -> int:
        """Obtain a request id to use for the current request.

        The request id is bumped and then returned

        Returns:
            request id to use on the current request

        """
        self.request_id += 1
        return self.request_id

    ###########################################################################
    # prepare_to_connect
    ###########################################################################
    def prepare_to_connect(self) -> None:
        """Reset the AlgoApp in preparation for connect processing.

        Raises:
            AlreadyConnected: Attempt to connect when already connected
            DisconnectLockHeld: Attempted to connect while the disconnect lock
                                  is held

        """
        if self.isConnected():
            raise AlreadyConnected('Attempted to connect, but already '
                                   'connected')

        if self.disconnect_lock.locked():
            raise DisconnectLockHeld('Attempted to connect while the '
                                     'disconnect lock is held')

        self.request_id = 0
        self.num_stock_symbols_received = 0
        self.stock_symbols = pd.DataFrame()
        self.response_complete_event.clear()
        self.nextValidId_event.clear()

    ###########################################################################
    # connect_to_ib
    ###########################################################################
    def connect_to_ib(self, ip_addr: str, port: int, client_id: int) -> None:
        """Connect to IB on the given addr and port and client id.

        Args:
            ip_addr: addr to connect to
            port: port to connect to
            client_id: client id to use for connection

        Raises:
            ConnectTimeout: timed out waiting for next valid request ID

        """
        self.prepare_to_connect()  # verification and initialization

        self.connect(ip_addr, port, client_id)

        logger.info('starting run thread')
        # the following try will succeed for the first connect only
        try:
            self.run_thread.start()
        except RuntimeError:  # must be a reconnect - we need a new thread
            self.run_thread = Thread(target=self.run)
            self.run_thread.start()

        # we will wait on the first requestID here for 10 seconds
        logger.debug('id of nextValidId_event %d',
                     id(self.nextValidId_event))
        if not self.nextValidId_event.wait(timeout=10):  # if we timed out
            logger.debug("timed out waiting for next valid request ID")
            self.disconnect_from_ib()
            raise ConnectTimeout(
                'connect_to_ib failed to receive nextValid_ID')

        logger.info('connect success')

    ###########################################################################
    # disconnect_from_ib
    ###########################################################################
    def disconnect_from_ib(self) -> None:
        """Disconnect from ib."""
        logger.info('calling EClient disconnect')

        self.disconnect()  # call our disconnect (overrides EClient)

        logger.info('join run_thread to wait for it to come home')
        self.run_thread.join()

        logger.info('disconnect complete')

    ###########################################################################
    # disconnect
    ###########################################################################
    def disconnect(self) -> None:
        """Call this function to terminate the connections with TWS."""
        # We would like to call EClient.disconnect, but it does not wait for
        # the reader thread to come home which leads to problems if a connect
        # is done immediately after the disconnect. The still running reader
        # thread snatches the early handshaking messages and leaves the
        # connect hanging. The following code is from client.py and is
        # modified here to add the thread join to ensure the reader comes
        # home before the disconnect returns.
        # Note also the use of the disconnect lock to serialize the two known
        # cases of disconnect being called from different threads (one from
        # mainline through disconnect_from_ib in AlgoApp, and one from the
        # EClient run method in the run thread.
        call_seq = get_formatted_call_sequence()
        logger.debug("%s entered disconnect", call_seq)
        with self.disconnect_lock:
            logger.debug("%s setting conn state", call_seq)
            self.setConnState(EClient.DISCONNECTED)
            if self.conn is not None:
                logger.info("%s disconnecting", call_seq)
                self.conn.disconnect()
                self.wrapper.connectionClosed()
                reader_id = id(self.reader)
                my_id = get_ident()
                my_native_id = get_native_id()
                logger.debug('about to join reader id %d for self id %d to'
                             ' wait for it to come home on thread %d %d',
                             reader_id, id(self), my_id, my_native_id)
                self.reader.join()
                logger.debug('reader id %d came home for id(self) %d '
                             'thread id %d %d',
                             reader_id,
                             id(self), my_id, my_native_id)
                self.reset()

    ###########################################################################
    # symbolSamples
    ###########################################################################
    def symbolSamples(self, request_id: int,
                      contract_descriptions: ListOfContractDescription
                      ) -> None:
        """Receive IB reply for reqMatchingSymbols request.

        Args:
            request_id: the id used on the request
            contract_descriptions: contains a list of contract descriptions.
                                     Each description includes the symbol,
                                     conId, security type, primary exchange,
                                     currency, and derivative security
                                     types.

        The contracts are filtered for stocks traded in the USA and are
        stored into a data frame as contracts that can be used later to
        request additional information or to make trades.
        """
        logger.info('entered for request_id %d', request_id)
        self.num_stock_symbols_received = len(contract_descriptions)
        logger.info('Number of descriptions received: %d',
                    self.num_stock_symbols_received)

        for desc in contract_descriptions:
            logger.debug('Symbol: {}'.format(desc.contract.symbol))
            if desc.contract.secType == 'STK' and \
                    desc.contract.currency == 'USD' and \
                    'OPT' in desc.derivativeSecTypes:
                self.stock_symbols = self.stock_symbols.append(
                    pd.DataFrame([[desc.contract.symbol,
                                   desc.contract.primaryExchange,
                                   ]],
                                 columns=['symbol',
                                          'primaryExchange'],
                                 index=[desc.contract.conId]))
        self.response_complete_event.set()

    ###########################################################################
    # get_symbols
    ###########################################################################
    def get_symbols(self) -> None:
        """Gets symbols and place them in the stock_symbols list."""
        # get_symbols is the starting point to the reqMatchingSymbols request
        # to ib. Input to reqMatchingSymbols is a pattern which is used to
        # find symbols. The pattern acts similar to a "string*" where the
        # asterisk acts as a wild card. So, a pattern such as "A" will return
        # any symbols that start with "A", such as "A" and "AB". There are
        # symbols that have the same name but are for different securities.
        # IB documentation says that at most 16 symbols will be returned that
        # are found with the pattern. So, in order to get all symbols that
        # start with "A", we need to try asking for symbols that start with
        # "A", "AB", "AC", "ABC", etc.., to ensure we find as many as we can
        # given the 16 symbols limit per request.
        # Another point is that we need to limit each reqMatchingSymbols to
        # no more than one request per second. Given the possibility of over
        # 500,000 existing symbols, and getting those at 16 per second, trying
        # to get everything will take almost 9 hours. So, we only attempt to
        # get all symbols that start with one character at a time which should
        # take about 20 minutes.We will start by loading a csv file that
        # contains each letter of the alphabet and a date of when the symbols
        # that start with that letter have last been obtained. The idea is
        # that we can call this method once per day and it will go after the
        # the symbols that were least recently loaded, thus maintaining a
        # fairly up-to-date list.

        #######################################################################
        # if stock_symbols data set exists, load it and reset the index
        #######################################################################
        stock_symbols_path = self.ds_catalog.get_path('stock_symbols')
        logger.info('path: %s', stock_symbols_path)

        if stock_symbols_path.exists():
            self.stock_symbols = pd.read_csv(stock_symbols_path,
                                             header=0,
                                             index_col=0)

        #######################################################################
        # load or create the symbols_status ds
        #######################################################################
        symbols_status_path = self.ds_catalog.get_path('symbols_status')
        logger.info('symbols_status_path: %s', symbols_status_path)

        if symbols_status_path.exists():
            self.symbols_status = pd.read_csv(symbols_status_path,
                                              header=0,
                                              index_col=0,
                                              parse_dates=True)
        else:
            self.symbols_status = pd.DataFrame(list(string.ascii_uppercase),
                                               columns=['AlphaChar'],
                                               index=pd.date_range("20000101",
                                                                   periods=26,
                                                                   freq="S"))
        #######################################################################
        # Get the next single uppercase letter and do the search.
        # The response from ib is handled by symbolSamples wrapper method
        #######################################################################
        search_char = self.symbols_status.iloc[0].AlphaChar
        self.get_symbols_recursive(search_char)

        #######################################################################
        # Save stock_symbols DataFrame to csv
        #######################################################################
        logger.info('Symbols obtained')
        logger.info('Number of entries before drop dups and sort: %d',
                    len(self.stock_symbols))

        if not self.stock_symbols.empty:
            self.stock_symbols.drop_duplicates(inplace=True)
            self.stock_symbols.sort_index(inplace=True)

        logger.info('Number of entries after drop dups and sort: %d',
                    len(self.stock_symbols))

        logger.info('saving stock_symbols DataFrame to csv')
        self.stock_symbols.to_csv(stock_symbols_path)

        #######################################################################
        # Update and save symbols_status DataFrame to csv. The timestamp
        # is updated to 'now' for the letter we just searched and then the ds
        # is sorted to put that entry last and move the next letter to
        # processed into the first slot for next time we call this method.
        #######################################################################
        self.symbols_status.index = [pd.Timestamp.now()] \
            + self.symbols_status.index.to_list()[1:]
        self.symbols_status.sort_index(inplace=True)
        logger.info('saving symbols_status DataFrame to csv')
        self.symbols_status.to_csv(symbols_status_path)

    ###########################################################################
    # get_symbols_recursive
    ###########################################################################
    def get_symbols_recursive(self, search_string: str) -> None:
        """Gets symbols and place them in the stock_symbols list.

        Args:
            search_string: string to start with

        """
        self.request_symbols(search_string)
        if self.num_stock_symbols_received > 15:  # possibly more to find
            # call recursively to get more symbols for this char sequence
            for add_char in string.ascii_uppercase + '.':
                longer_search_string = search_string + add_char
                self.get_symbols_recursive(longer_search_string)

    ###########################################################################
    # request_symbols
    ###########################################################################
    def request_symbols(self, symbol_to_get: str) -> None:
        """Request contract info from IB for given symbol.

        Args:
            symbol_to_get: one of more chars to match to symbols

        """
        self.response_complete_event.clear()
        logger.info('getting symbols that start with %s', symbol_to_get)

        #######################################################################
        # send request to IB
        #######################################################################
        self.reqMatchingSymbols(self.get_req_id(), symbol_to_get)
        # the following sleep for 1 second is required to avoid
        # overloading IB with requests (they ask for 1 second). Note that we
        # are doing the sleep after the request is made and before we wait
        # on the response to be completed. This allow some of the response
        # processing, which is on a different thread, to make progress while
        # we sleep, thus helping to reduce the entire wait (as opposed to
        # doing the 1 second wait before making the request).
        time.sleep(1)  # throttle to avoid overloading IB
        self.response_complete_event.wait()


# stock_symbols = pd.read_csv('/home/Tiger/Downloads/companylist.csv'
#                             ,usecols = ['Symbol', 'Name', 'MarketCap']
#                             ,index_col=['Symbol']
#                            )

# @time_box
# def main():
#     ds_catalog = FileCatalog()
#
#     try:
#         algo_app = AlgoApp(ds_catalog)
#
#         algo_app.connect_to_ib("127.0.0.1", 7496, client_id=0)
#
#         print("serverVersion:%s connectionTime:%s" %
#         (algo_app.serverVersion(),
#         algo_app.twsConnectionTime()))
#     except:
#         raise

    # print('get_stock_symbols:main about to sleep 2 seconds')
    # time.sleep(2)
    # print('SBT get_stock_symbols:main about to wait on nextValidId_event')
    # algo_app.nextValidId_event.wait()
    # print('SBT get_stock_symbols:main about to call get_symbols')
    # # algo_app.get_symbols(start_char='A', end_char='A')
    # # algo_app.get_symbols(start_char='B', end_char='B')
    #
    # algo_app.request_symbols('SWKS')
    #
    # algo_app.disconnect()
    # print('get_stock_symbols: main About to sleep for 2 seconds before exit')
    # time.sleep(2)
    # print('get_stock_symbols: main exiting')


# if __name__ == "__main__":
#     main()
