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
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum, auto
import functools
import inspect
import logging
from pathlib import Path
import string
import threading
from threading import Event, Lock, Thread
import time
from typing import (
    Any,
    cast,
    Callable,
    ClassVar,
    Final,
    NamedTuple,
    NewType,
    NoReturn,
    Optional,
    Type,
    TypeAlias,
    TYPE_CHECKING,
    Union,
)

########################################################################
# Third Party
########################################################################
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
from scottbrian_locking import se_lock as sel
from scottbrian_paratools.smart_thread import (
    SmartThread,
    ThreadState,
    SmartThreadNotFound,
    SmartThreadRemoteThreadNotAlive,
    SmartThreadRequestTimedOut,
)
from scottbrian_utils.file_catalog import FileCatalog
from scottbrian_utils.diag_msg import get_formatted_call_sequence
from scottbrian_utils.timer import Timer
from scottbrian_utils.unique_ts import UniqueTS, UniqueTStamp

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

from scottbrian_algo1.algo_client import AlgoClient, ClientRequestBlock, ReqID
from scottbrian_algo1.algo_data import MarketData


########################################################################
# TypeAlias
########################################################################
IntFloat: TypeAlias = Union[int, float]
OptIntFloat: TypeAlias = Optional[IntFloat]

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


class ConnectTimeout(AlgoAppError):
    """Connect timeout waiting for nextValid_ID event."""

    pass


class DisconnectTimeout(AlgoAppError):
    """Connect timeout waiting join during disconnect processing."""

    pass


class DisconnectDuringRequest(AlgoAppError):
    """Request detected disconnect while waiting on event completion."""

    pass


class RequestTimeout(AlgoAppError):
    """Request timed out while waiting on event completion."""

    pass


class RequestError(AlgoAppError):
    """Request received an error while waiting on event completion."""

    pass


class AlgoApiNotReady(AlgoAppError):
    """Request attempted with no connection established."""

    pass


# class RequestAfterShutdown(AlgoAppError):
#     """Request attempted after disconnect started."""
#
#     pass


AlgoExceptions: TypeAlias = Union[
    Type[AlreadyConnected],
    Type[ConnectTimeout],
    Type[DisconnectDuringRequest],
    Type[RequestTimeout],
    Type[RequestError],
]


########################################################################
# SetupArgs
########################################################################
@dataclass
class SetupArgs:
    smart_thread: SmartThread
    req_num: UniqueTStamp


dummy_setup_args = SetupArgs(
    smart_thread=SmartThread(group_name="1", name="1"), req_num=0.0
)


########################################################################
# ThreadConfig
########################################################################
class ThreadConfig(Enum):
    CurrentThread = auto()
    RemoteThread = auto()


########################################################################
# AsyncThreadBlock
########################################################################
@dataclass
class ThreadBlock:
    smart_thread: SmartThread
    in_use_count: int = 0
    create_time: float = 0.0
    start_time: float = 0.0
    time_freed: float = 0.0


####################################################################
# make_sync_async
####################################################################
def setup_sync(func: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(func)
    def wrapped(self, *args, **kwargs) -> Any:
        # logger.debug(f"{args=}, {kwargs=}")
        smart_thread = SmartThread.get_current_smart_thread(group_name=self.group_name)
        ref_num: UniqueTStamp = UniqueTS.get_unique_ts()

        ret_value = func(self, *args, **kwargs)

        return ret_value

    return wrapped


####################################################################
# setup_teardown
####################################################################
def setup_teardown(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to get and pass SmartThread and req_num to request.

    Args:
        func: function to be decorated

    Returns:
        decorated function

    Notes:
        1) this decorator adds the args to the kwargs for the function
           being decorated

    """

    @functools.wraps(func)
    def decorator_setup_teardown(self, *args, **kwargs) -> Any:
        # get unique timestamp to use for part of the smart thread name
        req_num = UniqueTS.get_unique_ts()

        with self.request_threads_lock:
            if (
                not self.ready_for_work
                and func.__name__ != "connect_to_ib"
                and func.__name__ != "disconnect_from_ib"
            ):
                error_msg = f"setup_teardown raising AlgoApiNotReady"
                logging.error(error_msg)
                raise AlgoApiNotReady(error_msg)
            # if self.shut_down_in_progress:
            #     error_msg = f"setup_teardown raising RequestAfterShutdown"
            #     logging.error(error_msg)
            #     raise RequestAfterShutdown(error_msg)
            try:
                # case 0: we are running under the thread that
                #         instantiated the AlgoApi and should certainly
                #         get back the already existing SmartThread
                #         (which is self)
                # case 1: we are running under a different thread than
                #         the one that instantiated the AlgoApi and this
                #         is the first time we have made a request on
                #         this thread which means we do not yet have a
                #         SmartThread. So we will get the
                #         SmartThreadNotFound exception and create a new
                #         SmartThread and place it in the
                #         request_threads array.
                # case 2: we are running under a different thread than
                #         the one that instantiated the AlgoApi and this
                #         is *not* the first time we have made a request
                #         on this thread which means we already have a
                #         a SmartThread and should get it returned on
                #         this call. We will mark it in use and set the
                #         start_time using the req_num time stamp.
                req_smart_thread = SmartThread.get_current_smart_thread(
                    group_name=self.group_name
                )
                if req_smart_thread is not self:
                    self.request_threads[req_smart_thread.name].in_use_count += 1
                    self.request_threads[req_smart_thread.name].start_time = req_num

            except SmartThreadNotFound:
                req_smart_thread_name = f"algo_requestor_{req_num}"
                req_smart_thread = SmartThread(
                    group_name=self.group_name, name=req_smart_thread_name
                )
                self.request_threads[req_smart_thread_name] = ThreadBlock(
                    smart_thread=req_smart_thread,
                    in_use_count=1,
                    create_time=req_num,
                    start_time=req_num,
                )

        kwargs["setup_args"] = SetupArgs(smart_thread=req_smart_thread, req_num=req_num)

        try:
            ret_value = func(self, *args, **kwargs)
        finally:
            if req_smart_thread is not self:
                with self.request_threads_lock:
                    # if we just did a disconnect
                    self.request_threads[req_smart_thread.name].in_use_count -= 1
                    self.request_threads[req_smart_thread.name].time_freed = time.time()
                    if (
                        not self.ready_for_work
                        and self.request_threads[req_smart_thread.name].in_use_count
                        == 0
                    ):
                        del self.request_threads[req_smart_thread.name]
                        req_smart_thread.smart_unreg()

        return ret_value

    # return SetupArgs(smart_thread=req_smart_thread, req_num=req_num)
    return decorator_setup_teardown


########################################################################
# AlgoApp
########################################################################
# class AlgoApp(EWrapper, EClient, SmartThread, Thread):  # type: ignore
class AlgoApp(SmartThread, Thread):  # type: ignore
    """AlgoApp class."""

    PORT_FOR_LIVE_TRADING: Final[int] = 7496
    PORT_FOR_PAPER_TRADING: Final[int] = 7497

    REQUEST_TIMEOUT_SECONDS: Final[int] = 60
    REQUEST_THROTTLE_SECONDS: Final[int] = 1.0

    K_LOOP_IDLE_TIME: Final[IntFloat] = 1.0
    K_DEFAULT_TIMEOUT: Final[IntFloat] = 30

    _config_lock: ClassVar[sel.SELock] = sel.SELock()

    ####################################################################
    # __init__
    ####################################################################
    def __init__(
        self,
        ds_catalog: FileCatalog,
        group_name: str = "algo_app_group",
        algo_name: str = "algo_app",
        default_timeout: OptIntFloat = K_DEFAULT_TIMEOUT,
    ) -> None:
        """Instantiate the AlgoApp.

        Args:
            ds_catalog: contain the paths for data sets
            group_name: name of SmartThread group
            algo_name: name of SmartThread instance for AlgoApp
            default_timeout: number of seconds to use as a timeout
                value if timeout is not specifies on a method that
                provides a timeout parameter. If this default_timeout is
                not specifies, the default_tineout is set to 30 seconds.

        :Example: instantiate AlgoApp and print it

        >>> from scottbrian_algo1.algo_api import AlgoApp
        >>> from scottbrian_utils.file_catalog import FileCatalog
        >>> from pathlib import Path
        >>> test_cat = FileCatalog({'symbols': Path('t_datasets/symbols.csv')})
        >>> algo_app = AlgoApp(test_cat)
        >>> print(algo_app)
        AlgoApp(ds_catalog)

        """
        Thread.__init__(self)
        SmartThread.__init__(
            self,
            group_name=group_name,
            name=algo_name,
        )
        self.group_name = group_name
        self.algo_name = algo_name
        self.ds_catalog = ds_catalog

        self.default_timeout = default_timeout

        self.request_threads: dict[str, ThreadBlock] = {}
        self.request_threads_lock: threading.Lock = threading.Lock()

        # self.shut_down_in_progress: bool = False
        self.ready_for_work: bool = False

        self.response_complete_event = Event()

        self.request_throttle_secs = AlgoApp.REQUEST_THROTTLE_SECONDS
        self.market_data = MarketData()

        # fundamental data
        # self.fundamental_data = pd.DataFrame()

        self.loop_idle_event: threading.Event = threading.Event()
        self.stop_monitor: bool = False

        self.client_name = "ibapi_client"

        # self.monitor_smart_thread = SmartThread(
        #     group_name=self.group_name,
        #     name="algo_monitor",
        #     target_rtn=self.monitor,
        #     thread_parm_name="smart_thread",
        # )

        self.algo_client = AlgoClient(
            group_name=group_name,
            algo_name=self.algo_name,
            client_name=self.client_name,
            response_complete_event=self.response_complete_event,
            market_data=self.market_data,
        )

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
    # monitor
    ####################################################################
    # def monitor(self, smart_thread: SmartThread) -> None:
    #     """Loop to do various tasks."""
    #     while True:
    #         self.loop_idle_event.wait(timeout=AlgoApp.K_LOOP_IDLE_TIME)

    ####################################################################
    # shut_down
    ####################################################################
    def shut_down(self) -> None:
        """Shut down the AlgoApp."""
        # self.stop_monitor = True
        self.smart_unreg()

    ####################################################################
    # setup_client_request
    ####################################################################
    def setup_client_request(self, requestor_name: str, req_num: UniqueTStamp) -> ReqID:
        """Send request to async handler."""

        req_id = self.algo_client.get_req_id()  # bump reqId and return it
        client_req_block = ClientRequestBlock(
            requestor_name=requestor_name, req_num=req_num
        )
        self.algo_client.add_active_request(req_id=req_id, req_block=client_req_block)

        return req_id

    ###########################################################################
    # connect_to_ib
    ###########################################################################
    @setup_teardown
    def connect_to_ib(
        self,
        *,
        ip_addr: str,
        port: int,
        client_id: int,
        timeout: Optional[IntFloat] = None,
        setup_args: SetupArgs = dummy_setup_args,
    ) -> None:
        """Connect to IB on the given addr and port and client id.

        Args:
            ip_addr: addr to connect to
            port: port to connect to
            client_id: client id to use for connection
            timeout: specifies the amount of time allowed for the
                request. If exceeded, a ConnectTimeout error will be
                raised.
            setup_args: the async args

        Raises:
            ConnectTimeout: timed out waiting for next valid request ID

        """
        logger.debug(
            f"connect_to_ib entry: {ip_addr=}, {port=}, {client_id=}, {timeout=} "
            f"{setup_args=}"
        )

        # setup_args = self.get_setup_args()

        error_msg = ""
        with sel.SELockExcl(AlgoApp._config_lock):
            if self.algo_client.isConnected():
                error_msg = "connect_to_ib already connected"
                logger.debug(error_msg)
                raise AlreadyConnected(error_msg)

            if self.algo_client.st_state == ThreadState.Unregistered:
                logger.info("instantiating AlgoClient")
                self.algo_client = AlgoClient(
                    group_name=self.group_name,
                    algo_name=self.algo_name,
                    client_name=self.client_name,
                    response_complete_event=self.response_complete_event,
                    market_data=self.market_data,
                )
            req_id = self.setup_client_request(
                requestor_name=setup_args.smart_thread.name,
                req_num=setup_args.req_num,
            )
            self.algo_client.connect(ip_addr, port, client_id)
            logger.info("starting AlgoClient thread 1")
            self.algo_client.smart_start()

            # we will wait on the first requestID here
            try:
                setup_args.smart_thread.smart_wait(
                    resumers=self.client_name,
                    timeout=timeout,
                    log_msg="connect_to_ib wait for nextValid_ID",
                )
                self.algo_client.pop_active_request(req_id=req_id)
                self.ready_for_work = True
                # self.shut_down_in_progress = False
            except SmartThreadRequestTimedOut:
                error_msg = "connect_to_ib timed out waiting to receive nextValid_ID"
                self.disconnect_from_ib()
                # call our disconnect (overrides EClient)
                # self.algo_client.disconnect()
                #
                # logger.info("join algo_client to wait for it to come home")
                #
                # try:
                #     setup_args.smart_thread.smart_join(
                #         targets=self.client_name,
                #         timeout=timeout,
                #     )
                # except SmartThreadRequestTimedOut:
                #     error_msg = "disconnect_from_ib timed out waiting for smart_join"
                #     logger.debug(error_msg)
                #     raise DisconnectTimeout(error_msg)

        if error_msg:
            logger.debug(error_msg)
            # we need to have dropped the lock since disconnect_from_ib
            # will need to acquire it
            # self.disconnect_from_ib()

            raise ConnectTimeout(error_msg)

        logger.info("connect success")

        logger.debug(
            f"connect_to_ib exit: {ip_addr=}, {port=}, {client_id=}, {timeout=}"
        )

    ####################################################################
    # disconnect_from_ib
    ####################################################################
    @setup_teardown
    def disconnect_from_ib(
        self,
        timeout: Optional[IntFloat] = None,
        setup_args: SetupArgs = dummy_setup_args,
    ) -> None:
        """Disconnect from ib.

        Args:
            timeout: specifies the amount of time allowed for the
                request. If exceeded, a DisconnectTimeout error will be
                raised.

        Raises:
            DisconnectTimeout: timed out waiting for join
        """
        logger.debug(f"disconnect_from_ib entry: {timeout=}")
        timer = Timer(timeout=timeout, default_timeout=self.default_timeout)

        # setup_args = self.get_setup_args()

        # self.shut_down_in_progress = True
        self.ready_for_work = False

        while True:
            names_to_remove: set[str] = set()
            pending_threads: int = 0
            with self.request_threads_lock:
                # the algo_api smartThread is not in self.request_threads
                for key, item in self.request_threads.items():
                    if item.smart_thread is not setup_args.smart_thread:
                        if item.in_use_count > 0:
                            pending_threads += 1
                        else:
                            names_to_remove |= {item.smart_thread.name}
                for name in names_to_remove:
                    del self.request_threads[name]

            if names_to_remove:
                setup_args.smart_thread.smart_unreg(targets=names_to_remove)

            if pending_threads == 0:
                break

            if timer.is_expired():
                error_msg = (
                    f"disconnect_from_ib raising DisconnectTimeout waiting for"
                    f"pending requests to complete"
                )
                logging.error(error_msg)
                raise DisconnectTimeout(error_msg)
            time.sleep(0.2)

        with sel.SELockExcl(AlgoApp._config_lock, allow_recursive_obtain=True):
            logger.info("calling EClient disconnect")

            # call our disconnect (overrides EClient)
            self.algo_client.disconnect()

            logger.info("join algo_client to wait for it to come home")

            try:
                setup_args.smart_thread.smart_join(
                    targets=self.client_name,
                    timeout=timer.remaining_time(),
                )
            except SmartThreadRequestTimedOut:
                error_msg = "disconnect_from_ib timed out waiting for smart_join"
                logger.debug(error_msg)
                raise DisconnectTimeout(error_msg)

        logger.info("disconnect complete")
        logger.debug(f"disconnect_from_ib exit: {timeout=}")

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
            # if not self.isConnected():
            #     logger.error("%s detected disconnect while waiting", call_seq)
            #     raise DisconnectDuringRequest
            # if self.algo_wrapper.error_reqId == reqId:
            #     raise RequestError
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
            self.market_data.contract_details = pd.read_csv(
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
        logger.info("Number of contract entries: %d", len(self.market_data.contracts))

        if not self.market_data.contracts.empty:
            self.market_data.contracts.sort_index(inplace=True)

        logger.info("saving contracts DataFrame to csv")
        self.market_data.contracts.to_csv(contracts_path)

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
            "Number of contract_details entries: %d",
            len(self.market_data.contract_details),
        )

        if not self.market_data.contract_details.empty:
            self.market_data.contract_details.sort_index(inplace=True)

        logger.info("saving contract_details DataFrame to csv")
        self.market_data.contract_details.to_csv(contract_details_path)

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
            "Number of fundamental_data entries: %d",
            len(self.market_data.fundamental_data),
        )

        if not self.market_data.fundamental_data.empty:
            self.market_data.fundamental_data.sort_index(inplace=True)

        logger.info("saving fundamental_data DataFrame to csv")
        self.market_data.contracts.to_csv(fundamental_data_path)

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
            self.market_data.symbols = pd.read_csv(
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
            self.market_data.stock_symbols = pd.read_csv(
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
            self.market_data.symbols_status = pd.read_csv(
                symbols_status_path, header=0, index_col=0, parse_dates=True
            )
        else:
            self.market_data.symbols_status = pd.DataFrame(
                list(string.ascii_uppercase),
                columns=["AlphaChar"],
                index=pd.date_range("20000101", periods=26, freq="S"),
            )
        #######################################################################
        # Get the next single uppercase letter and do the search.
        # The response from ib is handled by symbolSamples wrapper method
        #######################################################################
        search_char = self.market_data.symbols_status.iloc[0].AlphaChar
        self.get_symbols_recursive(search_char)

        #######################################################################
        # Save symbols DataFrame to csv
        #######################################################################
        logger.info("Symbols obtained")
        logger.info("Number of symbol entries: %d", len(self.market_data.symbols))

        if not self.market_data.symbols.empty:
            self.market_data.symbols.sort_index(inplace=True)

        logger.info("saving symbols DataFrame to csv")
        self.market_data.symbols.to_csv(symbols_path)

        #######################################################################
        # Save stock_symbols DataFrame to csv
        #######################################################################
        logger.info("Symbols obtained")
        logger.info(
            "Number of stoc_symbol entries: %d", len(self.market_data.stock_symbols)
        )

        if not self.market_data.stock_symbols.empty:
            self.market_data.stock_symbols.sort_index(inplace=True)

        logger.info("saving stock_symbols DataFrame to csv")
        self.market_data.stock_symbols.to_csv(stock_symbols_path)

        #######################################################################
        # Update and save symbols_status DataFrame to csv. The timestamp
        # is updated to 'now' for the letter we just searched and then the ds
        # is sorted to put that entry last and move the next letter to
        # processed into the first slot for next time we call this method.
        #######################################################################
        self.market_data.symbols_status.index = [
            pd.Timestamp.now()
        ] + self.market_data.symbols_status.index.to_list()[1:]
        self.market_data.symbols_status.sort_index(inplace=True)
        logger.info("saving symbols_status DataFrame to csv")
        self.market_data.symbols_status.to_csv(symbols_status_path)

    ###########################################################################
    # get_symbols_recursive
    ###########################################################################
    def get_symbols_recursive(self, search_string: str) -> None:
        """Gets symbols and place them in the symbols list.

        Args:
            search_string: string to start with

        """
        self.request_symbols(search_string)
        # if self.num_symbols_received > 15:  # possibly more to find
        if self.market_data.num_symbols_received > 15:  # possibly more to find
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
        # reqId = self.get_req_id()  # bump reqId and return it
        req_id = self.algo_client.get_req_id()  # bump reqId and return it
        self.algo_client.reqMatchingSymbols(req_id, symbol_to_get)
        # the following sleep for 1 second is required to avoid
        # overloading IB with requests (they ask for 1 second). Note that we
        # are doing the sleep after the request is made and before we wait
        # on the response to be completed. This allow some of the response
        # processing, which is on a different thread, to make progress while
        # we sleep, thus helping to reduce the entire wait (as opposed to
        # doing the 1 second wait before making the request).
        time.sleep(self.request_throttle_secs)  # avoid overloading IB
        self.wait_for_request_completion(req_id)

    # ###########################################################################
    # # symbolSamples - callback
    # ###########################################################################
    # def symbolSamples(
    #     self, request_id: int, contract_descriptions: ListOfContractDescription
    # ) -> None:
    #     """Receive IB reply for reqMatchingSymbols request.
    #
    #     Args:
    #         request_id: the id used on the request
    #         contract_descriptions: contains a list of contract descriptions.
    #                                  Each description includes the symbol,
    #                                  conId, security type, primary exchange,
    #                                  currency, and derivative security
    #                                  types.
    #
    #     The contracts are filtered for stocks traded in the USA and are
    #     stored into a data frame as contracts that can be used later to
    #     request additional information or to make trades.
    #     """
    #     logger.info("entered for request_id %d", request_id)
    #     self.num_symbols_received = len(contract_descriptions)
    #     logger.info("Number of descriptions received: %d", self.num_symbols_received)
    #
    #     for desc in contract_descriptions:
    #         logger.debug("Symbol: {}".format(desc.contract.symbol))
    #
    #         conId = desc.contract.conId
    #
    #         if (
    #             desc.contract.secType == "STK"
    #             and desc.contract.currency == "USD"
    #             and "OPT" in desc.derivativeSecTypes
    #         ):
    #             if conId in self.stock_symbols.index:
    #                 self.stock_symbols.loc[conId] = pd.Series(
    #                     get_contract_description_dict(desc)
    #                 )
    #             else:
    #                 self.stock_symbols = pd.concat(
    #                     [
    #                         self.stock_symbols,
    #                         pd.DataFrame(
    #                             get_contract_description_dict(desc, df=True),
    #                             index=[conId],
    #                         ),
    #                     ]
    #                 )
    #         else:  # all other symbols
    #             # update the descriptor if it already exists in the DataFrame
    #             # as we want the newest information to replace the old
    #             if conId in self.symbols.index:
    #                 self.symbols.loc[conId] = pd.Series(
    #                     get_contract_description_dict(desc)
    #                 )
    #             else:
    #                 self.symbols = pd.concat(
    #                     [
    #                         self.symbols,
    #                         pd.DataFrame(
    #                             get_contract_description_dict(desc, df=True),
    #                             index=[conId],
    #                         ),
    #                     ]
    #                 )
    #
    #     self.response_complete_event.set()

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
            self.market_data.contracts = self.load_contracts(contracts_path)

        self.load_contract_details()

        ################################################################
        # make the request for details
        ################################################################
        self.response_complete_event.clear()
        req_id = self.algo_client.get_req_id()  # bump reqId and return it
        self.algo_client.reqContractDetails(req_id, contract)
        self.wait_for_request_completion(req_id)

        #######################################################################
        # Save contracts and contract_details DataFrames
        #######################################################################
        self.save_contracts()
        self.save_contract_details()

    # ###########################################################################
    # # contractDetails callback method
    # ###########################################################################
    # def contractDetails(
    #     self, request_id: int, contract_details: ContractDetails
    # ) -> None:
    #     """Receive IB reply for reqContractDetails request.
    #
    #     Args:
    #         request_id: the id used on the request
    #         contract_details: contains contract and details
    #
    #     """
    #     logger.info("entered for request_id %d", request_id)
    #     logger.debug("Symbol: %s", contract_details.contract.symbol)
    #     # print('contract_details:\n', contract_details)
    #     # print('contract_details.__dict__:\n', contract_details.__dict__)
    #
    #     # get the conId to use as an index
    #     conId = contract_details.contract.conId
    #
    #     # The contracts and contract_details DataFrames are both indexed by
    #     # the conId. If an entry for the conId already exists, we will replace
    #     # it with the newest information we just now received. Otherwise, we
    #     # will add it. The get_contract_dict and get_contract_details_dict
    #     # methods each return a dictionary that can be used to update or add
    #     # to the DataFrame. Any class instances or arrays of class
    #     # instances will be returned as a string so that the DataFrame can be
    #     # stored and retrieved as csv files.
    #     contract_dict = get_contract_dict(contract_details.contract)
    #     if conId in self.market_data.contracts.index:
    #         self.market_data.contracts.loc[conId] = pd.Series(contract_dict)
    #     else:
    #         self.market_data.contracts = pd.concat(
    #             [self.market_data.contracts, pd.DataFrame(contract_dict, index=[conId])]
    #         )
    #
    #     # add the contract details to the DataFrame
    #     contract_details_dict = get_contract_details_dict(contract_details)
    #     if conId in self.market_data.contract_details.index:
    #         self.market_data.contract_details.loc[conId] = pd.Series(contract_details_dict)
    #     else:
    #         self.market_data.contract_details = pd.concat(
    #             [
    #                 self.market_data.contract_details,
    #                 pd.DataFrame(contract_details_dict, index=[conId]),
    #             ]
    #         )
    #
    # ###########################################################################
    # # contractDetailsEnd
    # ###########################################################################
    # def contractDetailsEnd(self, request_id: int) -> None:
    #     """Receive IB reply for reqContractDetails request end.
    #
    #     Args:
    #         request_id: the id used on the request
    #
    #     """
    #     logger.info("entered for request_id %d", request_id)
    #     self.response_complete_event.set()

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
            self.market_data.fundamental_data = self.load_fundamental_data(
                fundamental_data_path
            )

        ################################################################
        # make the request for fundamental data
        ################################################################
        self.response_complete_event.clear()
        req_id = self.algo_client.get_req_id()  # bump reqId and return it
        self.algo_client.reqFundamentalData(req_id, contract, report_type, [])
        self.wait_for_request_completion(req_id)

        #######################################################################
        # Save fundamental_data DataFrame
        #######################################################################
        self.save_fundamental_data()

    # ###########################################################################
    # # fundamentalData callback method
    # ###########################################################################
    # def fundamentalData(self, request_id: int, data: str) -> None:
    #     """Receive IB reply for reqFundamentalData request.
    #
    #     Args:
    #         request_id: the id used on the request
    #         data: xml string of fundamental data
    #
    #     """
    #     logger.info("entered for request_id %d", request_id)
    #
    #     print("\nfundamental data:\n", data)
    #     # print('contract_details.__dict__:\n', contract_details.__dict__)
    #
    #     # remove the contract if it already exists in the DataFrame
    #     # as we want the newest information to replace the old
    #     # conId = contract_details.contract.conId
    #     # self.market_data.contracts.drop(conId,
    #     #                     inplace=True,
    #     #                     errors='ignore')
    #     # get the conId to use as an index
    #
    #     # Add the contract to the DataFrame using contract dict.
    #     # Note that if the contract contains an array for one of the
    #     # fields, the DataFrame create will reject it because if the
    #     # single item conId for the index. The contract get_dict method
    #     # returns a dictionary that can be used to instantiate the
    #     # DataFrame item, and any class instances or arrays of class
    #     # instances will be returned as a string so that the DataFrame
    #     # item will work
    #     # contract_dict = get_contract_dict(contract_details.contract)
    #     # self.market_data.contracts = self.market_data.contracts.append(
    #     #             pd.DataFrame(contract_dict,
    #     #                          index=[conId]))
    #
    #     # remove the contract_details if it already exists in the DataFrame
    #     # as we want the newest information to replace the old
    #     # self.market_data.contract_details.drop(conId,
    #     #                            inplace=True,
    #     #                            errors='ignore')
    #
    #     # remove the contract from the contract_details
    #     # contract_details.contract = None
    #
    #     # add the contract details to the DataFrame
    #     # contract_details_dict = get_contract_details_dict(contract_details)
    #     # self.market_data.contract_details = self.market_data.contract_details.append(
    #     #             pd.DataFrame(contract_details_dict,
    #     #                          index=[conId]))
    #
    #     # print('self.market_data.contract_details:\n', contract_details)
    #     # print('self.market_data.contract_details.__dict__:\n',
    #     #       self.market_data.contract_details.__dict__)


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
