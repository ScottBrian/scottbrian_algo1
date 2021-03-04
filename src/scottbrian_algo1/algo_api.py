"""scottbrian_algo1 algo_api.

========
algo_api
========

With algo_api you can connect to the IBAPI and request information and make
trades.

"""

import pandas as pd  # type: ignore
from threading import Thread, Event
# from pathlib import Path
from scottbrian_utils.file_catalog import FileCatalog  # ,diag_msg
import time

from ibapi.wrapper import EWrapper  # type: ignore
# from ibapi import utils
from ibapi.client import EClient  # type: ignore
# from ibapi.utils import iswrapper

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

# from scottbrian_utils.file_catalog import FileCatalog

# from datetime import datetime
import logging

# set looging for debug for now until things ar working
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

class AlgoApp(EWrapper, EClient):  # type: ignore
    """AlgoApp class."""

    def __init__(self, ds_catalog: FileCatalog) -> None:
        """Instantiate the AlgoApp."""
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.ds_catalog = ds_catalog
        self.next_request_id: int = 0
        self.stock_symbols = pd.DataFrame()
        self.response_complete_event = Event()
        self.nextValidId_event = Event()
        self.run_thread = Thread(target=self.run)

    def __repr__(self) -> str:
        """Return a representation of the class.

        Returns:
            The representation as how the class is instantiated

        :Example: instantiate AlgoApp and print it

        >>> from scottbrian_algo1.algo_api import AlgoApp
        >>> algo_app = AlgoApp()
        >>> print(algo_app)
        AlgoApp()

        """
        if TYPE_CHECKING:
            __class__: Type[AlgoApp]
        classname = self.__class__.__name__
        parms = ''

        return f'{classname}({parms})'

    def error(self, reqId: int, errorCode: int, errorString: str) -> None:
        """Receive error from IB and print it.

        Args:
            reqId: the id of the failing request
            errorCode: the error code
            errorString: text to explain the error

        """
        # diag_msg('entered', depth=3)
        print("Error: ", reqId, " ", errorCode, " ", errorString)

    def nextValidId(self, request_id: int) -> None:
        """Receive next valid ID from IB and save it.

        Args:
            request_id: next id to use for a request to IB

        """
        # diag_msg('entered with request_id', request_id)
        self.next_request_id = request_id
        self.nextValidId_event.set()

    def connect_to_ib(self, ip_addr: str, port: int, client_id: int) -> bool:
        """Connect to IB on the given addr and port and client id.

        Args:
            ip_addr: addr to connect to
            port: port to connect to
            client_id: client id to use for connection

        """
        self.connect(ip_addr, port, client_id)

        # diag_msg('about to start run thread')
        self.run_thread.start()

        # we will wait on the first requestID here for 10 seconds
        if not self.nextValidId_event.wait(timeout=5):  # if we timed out
            # diag_msg("timed out waiting for next valid request ID")
            return False

        # diag_msg("back from wait")
        return True

    def disconnect_from_ib(self):
        # diag_msg('calling EClient disconnect', depth=3)
        EClient.disconnect(self)
        # diag_msg('join thread')
        self.run_thread.join()
        # diag_msg('exiting')


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
        print('get_stock_symbols:AlgoApp:symbolSamples entered for '
              'request_id', request_id)
        print('Number of descriptions received: {}'.
              format(len(contract_descriptions)))
        for desc in contract_descriptions:
            print('Symbol: {}'.format(desc.contract.symbol))
            # print('desc.contract:')
            # print(desc.contract)
            # print('    conId              :', desc.contract.conId)
            # print('    secType            :', desc.contract.secType)
            # print('    primaryExchange    :', desc.contract.primaryExchange)
            # print('    currency           :', desc.contract.currency)
            # print('    derivativeSecTypes :', desc.derivativeSecTypes)
            if desc.contract.secType == 'STK' and \
                    desc.contract.currency == 'USD':
                self.stock_symbols = self.stock_symbols.append(
                    pd.DataFrame([[desc.contract.symbol,
                                   desc.contract.conId,
                                   desc.contract.primaryExchange,
                                   ]],
                                 columns=['symbol',
                                          'con_id',
                                          'primary_exchange']))
        self.response_complete_event.set()

    def get_symbols(self, start_char: str, end_char: str) -> None:
        """Gets symbols and place them in the stock_symbols list.

        Args:
            start_char: char to start with
            end_char: char to end with

        """
        # if symbols data set exists, load it and reset the index
        stock_symbols_path = 'unknown'  # self.ds_catalog.catalog.loc[
        #  'stock_symbols'].full_path
        print('path:', stock_symbols_path)

        # if Path(stock_symbols_path).exists():
        #     self.stock_symbols = pd.read_csv(stock_symbols_path,
        #                                      header=0,
        #                                      index_col=False)
        for first_char in string.ascii_uppercase:
            # The reqMatchingSymbols request looks for matching
            # symbols based on the input string which acts as a simple pattern
            # match similar to "string*". So, in order to get single
            # character symbols we need to make the request with the single
            # char, and for two char names we need to request with a two char
            # string. We will pass in a three char string for three and more
            # character names. Unfortunately, IB only returns about 16 symbols
            # per request. So, we can only hope that the one char request
            # will return all of the one char names (there may be duplicate
            # names but for different contracts), and the same for the two
            # and three char names. We will deal with any duplicates later
            # after the collection is completed.
            if first_char < start_char:
                continue  # skip chars until we reach the start char

            if end_char < first_char:
                break  # we are done for the requested range of chars

            self.request_symbols(first_char)
            for second_char in string.ascii_uppercase:
                self.request_symbols(first_char + second_char)
                for third_char in string.ascii_uppercase:
                    self.request_symbols(first_char + second_char + third_char)

        print('AlgoApp:get_symbols Symbols obtained')
        print('Number of entries before drop dups, index and sort:',
              self.stock_symbols.shape)

        self.stock_symbols.drop_duplicates(inplace=True)
        self.stock_symbols = self.stock_symbols.set_index(
            ['symbol']).sort_index()

        print('Number of entries after drop dups, index, and sort:',
              self.stock_symbols.shape)
        print(self.stock_symbols)
        print('AlgoApp: get_symbols saving DataFrame to csv')
        # self.stock_symbols.to_csv(stock_symbols_path)

    def request_symbols(self, symbol_to_get: str) -> None:
        """Request contract info from IB for given symbol.

        Args:
            symbol_to_get: one of more chars to match to symbols

        """
        self.response_complete_event.clear()
        print('AlgoApp: request_symbols getting symbols that start with',
              symbol_to_get)
        self.next_request_id += 1
        self.reqMatchingSymbols(self.next_request_id,
                                symbol_to_get)
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
#
#     print('get_stock_symbols:main about to sleep 2 seconds')
#     time.sleep(2)
#     print('SBT get_stock_symbols:main about to wait on nextValidId_event')
#     algo_app.nextValidId_event.wait()
#     print('SBT get_stock_symbols:main about to call get_symbols')
#     algo_app.get_symbols(start_char='A', end_char='A')
#     algo_app.get_symbols(start_char='B', end_char='B')
#
#     algo_app.disconnect()
#     print('get_stock_symbols: main About to sleep for 2  seconds before
#     exit')
#     time.sleep(2)
#     print('get_stock_symbols: main exiting')
#
#
# if __name__ == "__main__":
#     main()
