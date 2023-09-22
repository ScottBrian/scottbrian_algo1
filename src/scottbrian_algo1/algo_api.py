"""scottbrian_algo1 algo_api.

========
algo_api
========

With algo_api you can connect to the IBAPI to obtain information and
make trades.

The information that can be obtained includes:

    1) stock symbols used by the IBAPI
    2) stock prices
    3) option prices
    4) current positions held
    5) current cash balance
    6) total balance of cash and positions

Much of the information is saved in a MongoDb database and updated
periodically.

There are 3 possible configurations:

    1) AlgoApp is to run under the current thread. AlgoApp init parm
       thread_config is specified as ThreadConfig.CurrentThread.
       AlgoApp will create a SmartThread using the current thread.
    2) AlgoApp is to run under the current SmartThread. AlgoApp init
       parm thread_config is specified as ThreadConfig.CurrentThread.
       AlgoApp will use the SmartThread for the current thread.
    3) AlgoApp is to run under a remote SmartThread. AlgoApp init parm
       thread_config is specified as ThreadConfig.RemoteThread.
       AlgoApp will create a remote SmartThread and start it.

Requests can be made to the AlgoApp from any thread. If on the same thread,
the request is handled synchronously. If from a remote thread,
the request can specify whether it is to run synchrounously or
asynchronously. When synchronous, any results are returned directly.
For asynchronous requests, the requestor must do a SmartThread
smart_recv request to receive the result.

"""
########################################################################
# Standard Library
########################################################################
import ast
from enum import Enum, auto
import logging
from pathlib import Path
import string
import threading
from threading import Event, get_ident, get_native_id, Lock, Thread
import time
from typing import Any, Callable, Optional, Type, TYPE_CHECKING, Union

########################################################################
# Third Party
########################################################################
import ibapi.common as ibcommon
from ibapi.client import EClient  # type: ignore
from ibapi.wrapper import EWrapper  # type: ignore
from ibapi.utils import current_fn_name  # type: ignore

from ibapi.common import ListOfContractDescription  # type: ignore

# from ibapi.order_condition import *  # @UnusedWildImport
from ibapi.contract import Contract, ContractDetails  # type: ignore

# from ibapi.order import *  # @UnusedWildImport
# from ibapi.order_state import *  # @UnusedWildImport
# from ibapi.execution import Execution
# from ibapi.execution import ExecutionFilter
# from ibapi.commission_report import CommissionReport
# from ibapi.ticktype import *  # @UnusedWildImport
# from ibapi.tag_value import TagValue
#
# from ibapi.account_summary_tags import *
import pandas as pd  # type: ignore
from scottbrian_paratools.smart_thread import (
    SmartThread,
    ThreadState,
    SmartThreadRemoteThreadNotAlive,
    SmartThreadRequestTimedOut,
)
from scottbrian_utils.file_catalog import FileCatalog
from scottbrian_utils.diag_msg import get_formatted_call_sequence

########################################################################
# Local
########################################################################
# import numexpr as ne
# import bottleneck as bn
# from scottbrian_algo1.algo_maps import AlgoTagValue, AlgoComboLeg
# from scottbrian_algo1.algo_maps import AlgoDeltaNeutralContract

from scottbrian_algo1.algo_maps import get_contract_dict
from scottbrian_algo1.algo_maps import get_contract_details_dict
from scottbrian_algo1.algo_maps import get_contract_description_dict


########################################################################
# logging
########################################################################
logger = logging.getLogger(__name__)


########################################################################
# pandas options
########################################################################
pd.set_option("mode.chained_assignment", "raise")
pd.set_option("display.max_columns", 10)


########################################################################
# Exceptions
########################################################################
class AlgoAppError(Exception):
    """Base class for exceptions in this module."""

    pass


class AlreadyConnected(AlgoAppError):
    """AlgoApp attempt to connect when already connected."""

    pass


class DisconnectLockHeld(AlgoAppError):
    """Attempted to connect while the disconnect lock is held."""


class ConnectTimeout(AlgoAppError):
    """Connect timeout waiting for nextValid_ID event."""


class DisconnectDuringRequest(AlgoAppError):
    """Request detected disconnect while waiting on event completion."""


class RequestTimeout(AlgoAppError):
    """Request timed out while waiting on event completion."""


class RequestError(AlgoAppError):
    """Request received an error while waiting on event completion."""


########################################################################
# ThreadConfig
########################################################################
class ThreadConfig(Enum):
    CurrentThread = auto()
    RemoteThread = auto()


####################################################################
# handle_thread_switching
####################################################################
def handle_thread_switching(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapped(self, *args, **kwargs) -> Any:
        # if self.algo1_smart_thread.thread is not threading.current_thread():
        logger.debug(f"wrapped entry: {self.thread=}, {threading.current_thread()=}")
        logger.debug(f"wrapped entry: {SmartThread._registry=}")
        for key, item in SmartThread._registry.items():
            logger.debug(
                f"{key=}, {item=}, {item.thread=}, {id(item.thread)=}, "
                f"{item.thread.is_alive()=}"
            )
        if self.thread is not threading.current_thread():
            if (
                caller_smart_thread := SmartThread.get_current_smart_thread()
            ) is not None:
                logger.debug(f"wrapped: {caller_smart_thread=}")
                cmd_tuple = (
                    func,
                    args,
                    kwargs,
                )
                caller_smart_thread.smart_send(msg=cmd_tuple, receivers=self.algo1_name)
                ret_value = caller_smart_thread.smart_recv(senders=self.algo1_name)
                if ret_value == "NONE":
                    ret_value = None
            else:
                raise RequestError(
                    f"{func.__name__} requires caller to have a SmartThread"
                )
        else:
            ret_value = func(self, *args, **kwargs)
        return ret_value

    return wrapped


########################################################################
# AlgoApp
########################################################################
class AlgoApp(EWrapper, EClient, SmartThread, Thread):  # type: ignore
    """AlgoApp class."""

    PORT_FOR_LIVE_TRADING = 7496
    PORT_FOR_PAPER_TRADING = 7497

    REQUEST_TIMEOUT_SECONDS = 60
    REQUEST_THROTTLE_SECONDS = 1.0

    ####################################################################
    # __init__
    ####################################################################
    def __init__(
        self,
        ds_catalog: FileCatalog,
        # thread_config: Optional[ThreadConfig] = ThreadConfig.CurrentThread,
        algo_name: str = "algo_app",
    ) -> None:
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
        Thread.__init__(self)
        # threading.current_thread().name = algo_name
        SmartThread.__init__(
            self,
            name=algo_name,
            thread=self,
            auto_start=False,
        )
        self.algo1_name = algo_name
        self.disconnect_lock = Lock()
        self.ds_catalog = ds_catalog
        self.request_id: int = 0
        self.error_reqId: int = 0
        self.response_complete_event = Event()
        self.nextValidId_event = Event()

        # stock symbols
        self.request_throttle_secs = AlgoApp.REQUEST_THROTTLE_SECONDS
        self.symbols_status = pd.DataFrame()
        self.num_symbols_received = 0
        self.symbols = pd.DataFrame()
        self.stock_symbols = pd.DataFrame()

        # contract details
        self.contracts = pd.DataFrame()
        self.contract_details = pd.DataFrame()

        # fundamental data
        self.fundamental_data = pd.DataFrame()

        self.handle_cmds: bool = False

        self.client_name = "ibapi_client"
        self.ibapi_client_smart_thread = SmartThread(
            name=self.client_name,
            target=EClient.run,
            args=(self,),
            auto_start=False,
        )

        # if thread_config == ThreadConfig.CurrentThread:
        #     if (smart_thread := SmartThread.get_current_smart_thread()) is not None:
        #         self.algo1_name = smart_thread.name
        #         self.algo1_smart_thread = smart_thread
        #     else:
        #         self.algo1_name = algo_name
        #         self.algo1_smart_thread = SmartThread(name=self.algo1_name)
        # elif thread_config == ThreadConfig.RemoteThread:
        #     self.algo1_name = algo_name
        #     self.algo1_smart_thread = SmartThread(
        #         name=self.algo1_name,
        #         target=self.cmd_loop,
        #         thread_parm_name="algo_smart_thread",
        #     )

    ###########################################################################
    # __repr__
    ###########################################################################
    def __repr__(self) -> str:
        """Return a representation of the class.

        Returns:
            The representation as how the class is instantiated

        # :Example: instantiate AlgoApp and print it
        #
        # >>> from scottbrian_algo1.algo_api import AlgoApp
        # >>> from scottbrian_utils.file_catalog import FileCatalog
        # >>> from pathlib import Path
        # >>> test_cat = FileCatalog({'symbols':
        Path('t_datasets/symbols.csv')})
        # >>> algo_app = AlgoApp(test_cat)
        # >>> print(algo_app)
        # AlgoApp(ds_catalog)

        """
        if TYPE_CHECKING:
            __class__: Type[AlgoApp]  # noqa: F842
        classname = self.__class__.__name__
        parms = "ds_catalog"

        return f"{classname}({parms})"

    ####################################################################
    # run
    ####################################################################
    # def run(self, algo_smart_thread: SmartThread) -> None:
    def run(self) -> None:
        """Handle commands for the AlgoApp."""
        logger.debug("run entered")

        exclude_names: set[str] = {self.algo1_name, self.client_name}
        recv_msgs: dict[str, list[Any]] = {}
        self.handle_cmds = True
        while self.handle_cmds:
            senders = SmartThread.get_active_names() - exclude_names
            if senders:
                try:
                    # recv_msgs = algo_smart_thread.smart_recv(
                    #     senders=senders,
                    #     sender_count=1,
                    #     timeout=2,
                    # )
                    recv_msgs = self.smart_recv(
                        senders=senders,
                        sender_count=1,
                        timeout=2,
                    )
                except SmartThreadRemoteThreadNotAlive:
                    pass
                except SmartThreadRequestTimedOut:
                    continue

                for name, cmd_list in recv_msgs.items():
                    for cmd in cmd_list:
                        cmd_result = cmd[0](self, *cmd[1], **cmd[2])
                        if cmd_result is None:
                            cmd_result = "NONE"
                        try:
                            # algo_smart_thread.smart_send(
                            #     msg=cmd_result,
                            #     receivers=name,
                            # )
                            self.smart_send(
                                msg=cmd_result,
                                receivers=name,
                            )
                        except SmartThreadRemoteThreadNotAlive:
                            continue
            else:
                time.sleep(1)

        logger.debug("run exiting")

    ####################################################################
    # error
    ####################################################################
    def error(
        self,
        reqId: ibcommon.TickerId,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson="",
    ) -> None:
        """Receive error from IB and print it.

        Args:
            reqId: the id of the failing request
            errorCode: the error code
            errorString: text to explain the error

        """
        super(EWrapper, self).wrapper.error(
            reqId,
            errorCode,
            errorString,
            advancedOrderRejectJson,
        )

        self.error_reqId = reqId

    ###########################################################################
    # nextValidId
    ###########################################################################
    def nextValidId(self, request_id: int) -> None:
        """Receive next valid ID from IB and save it.

        Args:
            request_id: next id to use for a request to IB

        """
        logger.info(
            f"next valid ID is {request_id}, {threading.current_thread()=}, " f"{self=}"
        )

        self.request_id = request_id
        # self.nextValidId_event.set()
        self.ibapi_client_smart_thread.smart_resume(waiters=self.algo1_name)

    ###########################################################################
    # get_reqId
    ###########################################################################
    def get_reqId(self) -> int:
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
            raise AlreadyConnected("Attempted to connect, but already connected")

        if self.disconnect_lock.locked():
            raise DisconnectLockHeld(
                "Attempted to connect while the disconnect lock is held"
            )

        self.request_id = 0
        self.num_stock_symbols_received = 0
        self.stock_symbols = pd.DataFrame()
        self.symbols = pd.DataFrame()
        self.response_complete_event.clear()
        self.nextValidId_event.clear()

    ###########################################################################
    # connect_to_ib
    ###########################################################################
    @handle_thread_switching
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

        logger.info("starting run thread")
        # the following try will succeed for the first connect only
        # try:
        #     self.ibapi_client_smart_thread.smart_start()
        # except RuntimeError:  # must be a reconnect - we need a new thread
        #     self.ibapi_client_smart_thread = SmartThread(
        #         name="ibapi_client",
        #         auto_start=False,
        #         target=self.run,
        #     )
        #     self.ibapi_client_smart_thread.smart_start()
        if self.ibapi_client_smart_thread.st_state == ThreadState.Registered:
            self.ibapi_client_smart_thread.smart_start()
        else:
            self.ibapi_client_smart_thread = SmartThread(
                name="ibapi_client",
                target=EClient.run,
                args=(self,),
            )
        # we will wait on the first requestID here for 10 seconds
        logger.debug("id of nextValidId_event %d", id(self.nextValidId_event))
        try:
            self.smart_wait(resumers="ibapi_client", timeout=10)
        except SmartThreadRequestTimedOut:
            logger.debug("timed out waiting for next valid request ID")
            self.disconnect_from_ib()
            raise ConnectTimeout("connect_to_ib failed to receive nextValid_ID")

        # if not self.nextValidId_event.wait(timeout=10):  # if we timed out
        #     logger.debug("timed out waiting for next valid request ID")
        #     self.disconnect_from_ib()
        #     raise ConnectTimeout("connect_to_ib failed to receive nextValid_ID")

        logger.info("connect success")

    ###########################################################################
    # disconnect_from_ib
    ###########################################################################
    @handle_thread_switching
    def disconnect_from_ib(self) -> None:
        """Disconnect from ib."""
        logger.info("calling EClient disconnect")

        self.disconnect()  # call our disconnect (overrides EClient)

        logger.info("join ibapi_client_smart_thread to wait for it to come home")
        # self.algo1_smart_thread.smart_join(
        #     targets="ibapi_client",
        #     timeout=60,
        # )
        self.smart_join(
            targets="ibapi_client",
            timeout=60,
        )
        # tell run (if running) to exit
        self.handle_cmds = False

        logger.info("disconnect complete")

    ###########################################################################
    # algo_join
    ###########################################################################
    def algo_join(self, caller_smart_thread: SmartThread) -> None:
        """Join the algo thread.

        Args:
            caller_smart_thread: smart thread instance to do join
        """
        logger.info("algo_join entered")

        caller_smart_thread.smart_join(targets=self.algo1_name)

        logger.info("algo_join exiting")

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
                logger.debug(
                    "about to join reader id %d for self id %d to"
                    " wait for it to come home on thread %d %d",
                    reader_id,
                    id(self),
                    my_id,
                    my_native_id,
                )
                self.reader.join()
                logger.debug(
                    "reader id %d came home for id(self) %d " "thread id %d %d",
                    reader_id,
                    id(self),
                    my_id,
                    my_native_id,
                )
                self.reset()

    ###########################################################################
    # wait_for_request_completion
    ###########################################################################
    def wait_for_request_completion(self, reqId: int) -> None:
        """Wait for the request to complete.

        Args:
            reqId: the request id

        Raises:
            DisconnectDuringRequest: disconnect detected
            RequestTimeout: waited too long
            RequestError: the error callback method was entered

        """
        call_seq = get_formatted_call_sequence()
        logger.info("%s about to wait for request event completion", call_seq)
        for i in range(self.REQUEST_TIMEOUT_SECONDS):  # try for one minute
            if not self.isConnected():
                logger.error("%s detected disconnect while waiting", call_seq)
                raise DisconnectDuringRequest
            if self.error_reqId == reqId:
                raise RequestError
            if self.response_complete_event.wait(timeout=1):
                return  # good, event completed w/o timeout
        # if here, we timed out out while still connected
        logger.error("%s request timed out", call_seq)
        raise RequestTimeout

    ###########################################################################
    # load_contracts
    ###########################################################################
    @staticmethod
    def load_contracts(path: Path) -> Any:
        """Load the contracts DataFrame.

        Args:
            path: where to find the dataframe to load

        Returns:
              a dataframe of contracts
        """
        ret_df = pd.read_csv(
            path,
            header=0,
            index_col=0,
            parse_dates=["lastTradeDateOrContractMonth"],
            converters={
                "symbol": lambda x: x,
                "secType": lambda x: x,
                "right": lambda x: x,
                "multiplier": lambda x: x,
                "exchange": lambda x: x,
                "primaryExchange": lambda x: x,
                "currency": lambda x: x,
                "localSymbol": lambda x: x,
                "tradingClass": lambda x: x,
                "secIdType": lambda x: x,
                "secId": lambda x: x,
                "comboLegsDescrip": lambda x: x,
                "comboLegs": lambda x: None if x == "" else x,
                "deltaNeutralContract": lambda x: None if x == "" else x,
                "originalLastTradeDate": lambda x: x,
            },
        )
        return ret_df

    ###########################################################################
    # load_contracts
    ###########################################################################
    @staticmethod
    def load_fundamental_data(path: Path) -> Any:
        """Load the fundamental_data DataFrame.

        Args:
            path: where to find the dataframe to load

        Returns:
              a dataframe of fundamental data
        """
        ret_df = pd.read_csv(path, header=0, index_col=0)
        return ret_df

    ###########################################################################
    # load_contract_details
    ###########################################################################
    def load_contract_details(self) -> None:
        """Load the contracts DataFrame."""
        #######################################################################
        # if contract_details data set exists, load it and reset the index
        #######################################################################
        contract_details_path = self.ds_catalog.get_path("contract_details")
        logger.info("contract_details path: %s", contract_details_path)

        if contract_details_path.exists():
            self.contract_details = pd.read_csv(
                contract_details_path,
                header=0,
                index_col=0,
                converters={
                    "contract": lambda x: None if x == "" else x,
                    "marketName": lambda x: x,
                    "orderTypes": lambda x: x,
                    "validExchanges": lambda x: x,
                    "longName": lambda x: x,
                    "contractMonth": lambda x: x,
                    "industry": lambda x: x,
                    "category": lambda x: x,
                    "subcategory": lambda x: x,
                    "timeZoneId": lambda x: x,
                    "tradingHours": lambda x: x,
                    "liquidHours": lambda x: x,
                    "evRule": lambda x: x,
                    "underSymbol": lambda x: x,
                    "underSecType": lambda x: x,
                    "marketRuleIds": lambda x: x,
                    "secIdList": lambda x: None if x == "" else x,
                    "realExpirationDate": lambda x: x,
                    "lastTradeTime": lambda x: x,
                    "stockType": lambda x: x,
                    "cusip": lambda x: x,
                    "ratings": lambda x: x,
                    "descAppend": lambda x: x,
                    "bondType": lambda x: x,
                    "couponType": lambda x: x,
                    "maturity": lambda x: x,
                    "issueDate": lambda x: x,
                    "nextOptionDate": lambda x: x,
                    "nextOptionType": lambda x: x,
                    "notes": lambda x: x,
                },
            )

    ###########################################################################
    # save_contracts
    ###########################################################################
    def save_contracts(self) -> None:
        """Save the contracts DataFrame."""
        #######################################################################
        # Get the contracts path
        #######################################################################
        contracts_path = self.ds_catalog.get_path("contracts")
        logger.info("contracts path: %s", contracts_path)

        #######################################################################
        # Save contracts DataFrame to csv
        #######################################################################
        logger.info("Number of contract entries: %d", len(self.contracts))

        if not self.contracts.empty:
            self.contracts.sort_index(inplace=True)

        logger.info("saving contracts DataFrame to csv")
        self.contracts.to_csv(contracts_path)

    ###########################################################################
    # save_contract_details
    ###########################################################################
    def save_contract_details(self) -> None:
        """Save the contract_details DataFrame."""
        #######################################################################
        # get the contract_details path
        #######################################################################
        contract_details_path = self.ds_catalog.get_path("contract_details")
        logger.info("contract_details path: %s", contract_details_path)

        #######################################################################
        # Save contract_details DataFrame to csv
        #######################################################################
        logger.info(
            "Number of contract_details entries: %d", len(self.contract_details)
        )

        if not self.contract_details.empty:
            self.contract_details.sort_index(inplace=True)

        logger.info("saving contract_details DataFrame to csv")
        self.contract_details.to_csv(contract_details_path)

    ###########################################################################
    # save_fundamental_data
    ###########################################################################
    def save_fundamental_data(self) -> None:
        """Save the fundamental_data DataFrame."""
        #######################################################################
        # Get the fundamental_data path
        #######################################################################
        fundamental_data_path = self.ds_catalog.get_path("fundamental_data")
        logger.info("fundamental_data path: %s", fundamental_data_path)

        #######################################################################
        # Save fundamental_data DataFrame to csv
        #######################################################################
        logger.info(
            "Number of fundamental_data entries: %d", len(self.fundamental_data)
        )

        if not self.fundamental_data.empty:
            self.fundamental_data.sort_index(inplace=True)

        logger.info("saving fundamental_data DataFrame to csv")
        self.contracts.to_csv(fundamental_data_path)

    ###########################################################################
    ###########################################################################
    # get_symbols
    ###########################################################################
    ###########################################################################
    def get_symbols(self) -> None:
        """Gets symbols and place them in the symbols list."""
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
        # if symbols data set exists, load it and reset the index
        #######################################################################
        symbols_path = self.ds_catalog.get_path("symbols")
        logger.info("path: %s", symbols_path)

        if symbols_path.exists():
            self.symbols = pd.read_csv(
                symbols_path,
                header=0,
                index_col=0,
                converters={"derivativeSecTypes": lambda x: ast.literal_eval(x)},
            )
        #######################################################################
        # if stock_symbols data set exists, load it and reset the index
        #######################################################################
        stock_symbols_path = self.ds_catalog.get_path("stock_symbols")
        logger.info("path: %s", stock_symbols_path)

        if stock_symbols_path.exists():
            self.stock_symbols = pd.read_csv(
                stock_symbols_path,
                header=0,
                index_col=0,
                converters={"derivativeSecTypes": lambda x: ast.literal_eval(x)},
            )
        #######################################################################
        # load or create the symbols_status
        #######################################################################
        symbols_status_path = self.ds_catalog.get_path("symbols_status")
        logger.info("symbols_status_path: %s", symbols_status_path)

        if symbols_status_path.exists():
            self.symbols_status = pd.read_csv(
                symbols_status_path, header=0, index_col=0, parse_dates=True
            )
        else:
            self.symbols_status = pd.DataFrame(
                list(string.ascii_uppercase),
                columns=["AlphaChar"],
                index=pd.date_range("20000101", periods=26, freq="S"),
            )
        #######################################################################
        # Get the next single uppercase letter and do the search.
        # The response from ib is handled by symbolSamples wrapper method
        #######################################################################
        search_char = self.symbols_status.iloc[0].AlphaChar
        self.get_symbols_recursive(search_char)

        #######################################################################
        # Save symbols DataFrame to csv
        #######################################################################
        logger.info("Symbols obtained")
        logger.info("Number of symbol entries: %d", len(self.symbols))

        if not self.symbols.empty:
            self.symbols.sort_index(inplace=True)

        logger.info("saving symbols DataFrame to csv")
        self.symbols.to_csv(symbols_path)

        #######################################################################
        # Save stock_symbols DataFrame to csv
        #######################################################################
        logger.info("Symbols obtained")
        logger.info("Number of stoc_symbol entries: %d", len(self.stock_symbols))

        if not self.stock_symbols.empty:
            self.stock_symbols.sort_index(inplace=True)

        logger.info("saving stock_symbols DataFrame to csv")
        self.stock_symbols.to_csv(stock_symbols_path)

        #######################################################################
        # Update and save symbols_status DataFrame to csv. The timestamp
        # is updated to 'now' for the letter we just searched and then the ds
        # is sorted to put that entry last and move the next letter to
        # processed into the first slot for next time we call this method.
        #######################################################################
        self.symbols_status.index = [
            pd.Timestamp.now()
        ] + self.symbols_status.index.to_list()[1:]
        self.symbols_status.sort_index(inplace=True)
        logger.info("saving symbols_status DataFrame to csv")
        self.symbols_status.to_csv(symbols_status_path)

    ###########################################################################
    # get_symbols_recursive
    ###########################################################################
    def get_symbols_recursive(self, search_string: str) -> None:
        """Gets symbols and place them in the symbols list.

        Args:
            search_string: string to start with

        """
        self.request_symbols(search_string)
        if self.num_symbols_received > 15:  # possibly more to find
            # call recursively to get more symbols for this char sequence
            for add_char in string.ascii_uppercase + ".":
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
        logger.info("getting symbols that start with %s", symbol_to_get)

        #######################################################################
        # send request to IB
        #######################################################################
        self.response_complete_event.clear()  # reset the wait bit first
        reqId = self.get_reqId()  # bump reqId and return it
        self.reqMatchingSymbols(reqId, symbol_to_get)
        # the following sleep for 1 second is required to avoid
        # overloading IB with requests (they ask for 1 second). Note that we
        # are doing the sleep after the request is made and before we wait
        # on the response to be completed. This allow some of the response
        # processing, which is on a different thread, to make progress while
        # we sleep, thus helping to reduce the entire wait (as opposed to
        # doing the 1 second wait before making the request).
        time.sleep(self.request_throttle_secs)  # avoid overloading IB
        self.wait_for_request_completion(reqId)

    ###########################################################################
    # symbolSamples - callback
    ###########################################################################
    def symbolSamples(
        self, request_id: int, contract_descriptions: ListOfContractDescription
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
        logger.info("entered for request_id %d", request_id)
        self.num_symbols_received = len(contract_descriptions)
        logger.info("Number of descriptions received: %d", self.num_symbols_received)

        for desc in contract_descriptions:
            logger.debug("Symbol: {}".format(desc.contract.symbol))

            conId = desc.contract.conId

            if (
                desc.contract.secType == "STK"
                and desc.contract.currency == "USD"
                and "OPT" in desc.derivativeSecTypes
            ):
                if conId in self.stock_symbols.index:
                    self.stock_symbols.loc[conId] = pd.Series(
                        get_contract_description_dict(desc)
                    )
                else:
                    self.stock_symbols = pd.concat(
                        [
                            self.stock_symbols,
                            pd.DataFrame(
                                get_contract_description_dict(desc, df=True),
                                index=[conId],
                            ),
                        ]
                    )
            else:  # all other symbols
                # update the descriptor if it already exists in the DataFrame
                # as we want the newest information to replace the old
                if conId in self.symbols.index:
                    self.symbols.loc[conId] = pd.Series(
                        get_contract_description_dict(desc)
                    )
                else:
                    self.symbols = pd.concat(
                        [
                            self.symbols,
                            pd.DataFrame(
                                get_contract_description_dict(desc, df=True),
                                index=[conId],
                            ),
                        ]
                    )

        self.response_complete_event.set()

    ###########################################################################
    ###########################################################################
    # get_contract_details
    ###########################################################################
    ###########################################################################
    def get_contract_details(self, contract: Contract) -> None:
        """Get contract details for one or more contracts.

        Args:
            contract: contains the search criteria for details request

        The request for contract details sends the input contract to ib which
        it will use to find all contracts that match the contract fields. This
        could be a specific contract, or many contracts determined by how
        specific the input contract fields are.
        """
        #######################################################################
        # The contract details request will cause zero or more contract_details
        # class instances to be returned to the callback method. The contract
        # itself will be removed from the contract_details and its dictionary
        # will be obtained and used to create the DataFrame entry. The same
        # will be done for the contract_details (minus the contract). Note that
        # this is tricky code since both the contract and contract details
        # may contain other class instances or a list of class instances. In
        # the case of a list of class instances, we must replace each  of
        # those
        # instances with its dictionary, then turn the list into a tuple,
        # and then turn that into a string and placed into the containing
        # class instance. This needs to be done to 1) allow a list of
        # arbitrary
        # count be placed into a DataFrame column as one item, and 2) allow
        # the DataFrame to be stored to a csv file. Note that the list
        # needs to be a tuple to allow certain DataFrame operations to be
        # performed, such as removing duplicates which does not like lists
        # (because they non-hashable). Getting the dictionary
        # of these classes is done with a get_dict method which will take care
        # of the details described above. When the saved DataFrame is retrieved
        # later and we want to restore an entry to its class instance, the
        # DataFrame entry is converted to a dictionary which is then used to
        # set into the class instance dictionary. When the dictionary contains
        # strings of tuples of dictionary of embedded class instances, they
        # need to be converted back to a list of those instances. The details
        # of this are handled by get_obj methods.
        #
        #######################################################################

        ################################################################
        # load the contracts and contract_details DataFrames
        ################################################################
        contracts_path = self.ds_catalog.get_path("contracts")
        logger.info("contracts path: %s", contracts_path)

        if contracts_path.exists():
            self.contracts = self.load_contracts(contracts_path)

        self.load_contract_details()

        ################################################################
        # make the request for details
        ################################################################
        self.response_complete_event.clear()
        reqId = self.get_reqId()  # bump reqId and return it
        self.reqContractDetails(reqId, contract)
        self.wait_for_request_completion(reqId)

        #######################################################################
        # Save contracts and contract_details DataFrames
        #######################################################################
        self.save_contracts()
        self.save_contract_details()

    ###########################################################################
    # contractDetails callback method
    ###########################################################################
    def contractDetails(
        self, request_id: int, contract_details: ContractDetails
    ) -> None:
        """Receive IB reply for reqContractDetails request.

        Args:
            request_id: the id used on the request
            contract_details: contains contract and details

        """
        logger.info("entered for request_id %d", request_id)
        logger.debug("Symbol: %s", contract_details.contract.symbol)
        # print('contract_details:\n', contract_details)
        # print('contract_details.__dict__:\n', contract_details.__dict__)

        # get the conId to use as an index
        conId = contract_details.contract.conId

        # The contracts and contract_details DataFrames are both indexed by
        # the conId. If an entry for the conId already exists, we will replace
        # it with the newest information we just now received. Otherwise, we
        # will add it. The get_contract_dict and get_contract_details_dict
        # methods each return a dictionary that can be used to update or add
        # to the DataFrame. Any class instances or arrays of class
        # instances will be returned as a string so that the DataFrame can be
        # stored and retrieved as csv files.
        contract_dict = get_contract_dict(contract_details.contract)
        if conId in self.contracts.index:
            self.contracts.loc[conId] = pd.Series(contract_dict)
        else:
            self.contracts = pd.concat(
                [self.contracts, pd.DataFrame(contract_dict, index=[conId])]
            )

        # add the contract details to the DataFrame
        contract_details_dict = get_contract_details_dict(contract_details)
        if conId in self.contract_details.index:
            self.contract_details.loc[conId] = pd.Series(contract_details_dict)
        else:
            self.contract_details = pd.concat(
                [
                    self.contract_details,
                    pd.DataFrame(contract_details_dict, index=[conId]),
                ]
            )

    ###########################################################################
    # contractDetailsEnd
    ###########################################################################
    def contractDetailsEnd(self, request_id: int) -> None:
        """Receive IB reply for reqContractDetails request end.

        Args:
            request_id: the id used on the request

        """
        logger.info("entered for request_id %d", request_id)
        self.response_complete_event.set()

    ###########################################################################
    ###########################################################################
    # get_fundamental_data
    ###########################################################################
    ###########################################################################
    def get_fundamental_data(self, contract: Contract, report_type: str) -> None:
        """Get fundamental data for one contract.

        Args:
            contract: contains the conId used to get fundamental data
            report_type: ReportSnapshot, ReportsFinSummary, ReportRatios,
                           ReportFinStatements, RESC

        """
        #######################################################################
        # report_type:
        # ReportSnapshot: Company overview
        # ReportsFinSummary: Financial summary
        # ReportRatios: Financial ratios
        # ReportsFinStatements: Financial statements
        # RESC: Analyst estimates
        #######################################################################

        ################################################################
        # load the fundamental_data DataFrame
        ################################################################
        fundamental_data_path = self.ds_catalog.get_path("fundamental_data")
        logger.info("fundamental_data_path: %s", fundamental_data_path)

        if fundamental_data_path.exists():
            self.fundamental_data = self.load_fundamental_data(fundamental_data_path)

        ################################################################
        # make the request for fundamental data
        ################################################################
        self.response_complete_event.clear()
        reqId = self.get_reqId()  # bump reqId and return it
        self.reqFundamentalData(reqId, contract, report_type, [])
        self.wait_for_request_completion(reqId)

        #######################################################################
        # Save fundamental_data DataFrame
        #######################################################################
        self.save_fundamental_data()

    ###########################################################################
    # fundamentalData callback method
    ###########################################################################
    def fundamentalData(self, request_id: int, data: str) -> None:
        """Receive IB reply for reqFundamentalData request.

        Args:
            request_id: the id used on the request
            data: xml string of fundamental data

        """
        logger.info("entered for request_id %d", request_id)

        print("\nfundamental data:\n", data)
        # print('contract_details.__dict__:\n', contract_details.__dict__)

        # remove the contract if it already exists in the DataFrame
        # as we want the newest information to replace the old
        # conId = contract_details.contract.conId
        # self.contracts.drop(conId,
        #                     inplace=True,
        #                     errors='ignore')
        # get the conId to use as an index

        # Add the contract to the DataFrame using contract dict.
        # Note that if the contract contains an array for one of the
        # fields, the DataFrame create will reject it because if the
        # single item conId for the index. The contract get_dict method
        # returns a dictionary that can be used to instantiate the
        # DataFrame item, and any class instances or arrays of class
        # instances will be returned as a string so that the DataFrame
        # item will work
        # contract_dict = get_contract_dict(contract_details.contract)
        # self.contracts = self.contracts.append(
        #             pd.DataFrame(contract_dict,
        #                          index=[conId]))

        # remove the contract_details if it already exists in the DataFrame
        # as we want the newest information to replace the old
        # self.contract_details.drop(conId,
        #                            inplace=True,
        #                            errors='ignore')

        # remove the contract from the contract_details
        # contract_details.contract = None

        # add the contract details to the DataFrame
        # contract_details_dict = get_contract_details_dict(contract_details)
        # self.contract_details = self.contract_details.append(
        #             pd.DataFrame(contract_details_dict,
        #                          index=[conId]))

        # print('self.contract_details:\n', contract_details)
        # print('self.contract_details.__dict__:\n',
        #       self.contract_details.__dict__)


# @time_box
# def main() -> None:
#     """Main routine for quick discovery tests."""
#     from pathlib import Path
#
#     proj_dir = Path.cwd().resolve().parents[1]  # back two directories
#     ds_catalog = FileCatalog(
#         {
#             "symbols": Path(proj_dir / "t_datasets/symbols.csv"),
#             "mock_contract_descs": Path(
#                 proj_dir / "t_datasets/mock_contract_descs.csv"
#             ),
#             "contracts": Path(proj_dir / "t_datasets/contracts.csv"),
#             "contract_details": Path(proj_dir / "t_datasets/contract_details.csv"),
#             "fundamental_data": Path(proj_dir / "t_datasets/fundamental_data.csv"),
#         }
#     )
#
#     algo_app = AlgoApp(ds_catalog)
#
#     # algo_app.connect_to_ib("127.0.0.1", 7496, client_id=0)
#
#     print(
#         "serverVersion:%s connectionTime:%s"
#         % (algo_app.serverVersion(), algo_app.twsConnectionTime())
#     )
#
#     # print('SBT get_stock_symbols:main about to call get_symbols')
#     # algo_app.get_symbols(start_char='A', end_char='A')
#     # algo_app.get_symbols(start_char='B', end_char='B')
#
#     # algo_app.request_symbols('SWKS')
#     #
#     # print('algo_app.stock_symbols\n', algo_app.stock_symbols)
#     try:
#         contract = Contract()
#         contract.conId = 208813719  # 3691937 4726021
#         contract.symbol = "GOOGL"
#         contract.secType = "STK"
#         contract.currency = "USD"
#         contract.exchange = "SMART"
#         contract.primaryExchange = "NASDAQ"
#
#         # algo_app.get_contract_details(contract)
#         # algo_app.get_fundamental_data(contract, 'ReportSnapshot')
#         # ReportSnapshot: Company overview
#         # ReportsFinSummary: Financial summary
#         # ReportRatios: Financial ratios
#         # ReportsFinStatements: Financial statements
#         # RESC: Analyst estimates
#         # print('algo_app.contracts\n', algo_app.contracts)
#         # print('algo_app.contract_details\n', algo_app.contract_details)
#         # print('algo_app.contracts.primaryExchange.loc[contract.conId]\n',
#         #       algo_app.contracts.primaryExchange.loc[contract.conId])
#         # print('algo_app.contracts.symbol.loc[contract.conId]\n',
#         #       algo_app.contracts.symbol.loc[contract.conId])
#         # my_contract_details = algo_app.contract_details.loc[
#         # contract.conId][0]
#         # print('my_contract_details\n', my_contract_details)
#     finally:
#         print("about to disconnect")
#         algo_app.disconnect()
#
#
# if __name__ == "__main__":
#     main()
