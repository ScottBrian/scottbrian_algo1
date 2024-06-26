"""scottbrian_algo1 algo_client.

===========
algo_client
===========

The algo_client contains AlgoClient, a subclass of the ibapi EClient
class.
"""

########################################################################
# Standard Library
########################################################################
from collections.abc import Iterable
from dataclasses import dataclass
import functools
import logging
import threading
from threading import Event, get_ident, get_native_id, Lock, Thread
from typing import Any, Callable, NewType, Optional, Type, TYPE_CHECKING, Union

########################################################################
# Third Party
########################################################################
from ibapi.client import EClient  # type: ignore
import ibapi.common as ibcommon
from ibapi.contract import Contract, ContractDetails  # type: ignore
from ibapi.wrapper import EWrapper  # type: ignore
import pandas as pd  # type: ignore

from scottbrian_locking import se_lock as sel
from scottbrian_paratools.smart_thread import (
    SmartThread,
    ThreadState,
    SmartThreadRemoteThreadNotAlive,
    SmartThreadRequestTimedOut,
)

from scottbrian_utils.entry_trace import etrace
from scottbrian_utils.file_catalog import FileCatalog
from scottbrian_utils.diag_msg import get_formatted_call_sequence
from scottbrian_utils.unique_ts import UniqueTS, UniqueTStamp

########################################################################
# Local
########################################################################
# from scottbrian_algo1.algo_wrapper import AlgoWrapper
from scottbrian_algo1.algo_data import MarketData

########################################################################
# Types
########################################################################
ReqID = NewType("ReqID", int)

########################################################################
# logging
########################################################################
logger = logging.getLogger(__name__)

algo_client_etrace: bool = True


########################################################################
# Exceptions
########################################################################
class AlgoClientError(Exception):
    """Base class for exceptions in this module."""

    pass


class RequestIdInUse(AlgoClientError):
    """Attempt to add active request with in use req_id."""

    pass


class RequestIdNotFound(AlgoClientError):
    """Attempt to update active request but req_id not found."""

    pass


########################################################################
# ClientRequestBlock
########################################################################
@dataclass
class ClientRequestBlock:
    requestor_name: str
    req_num: UniqueTStamp
    request_complete: bool = False
    request_resumed: bool = False


########################################################################
# AlgoApp
########################################################################
class AlgoClient(EClient, SmartThread, Thread):  # type: ignore
    """AlgoClient class."""

    PORT_FOR_LIVE_TRADING = 7496
    PORT_FOR_PAPER_TRADING = 7497

    REQUEST_TIMEOUT_SECONDS = 60
    REQUEST_THROTTLE_SECONDS = 1.0

    ####################################################################
    # __init__
    ####################################################################
    def __init__(
        self,
        group_name: str,
        algo_name: str,
        client_name: str,
        response_complete_event: Event,
        market_data: MarketData,
    ) -> None:
        """Instantiate the AlgoClient.

        Args:
            group_name: group_name for SmartThread
            algo_name: name of main algo
            client_name: name of client for EClient class instance
            market_data: data frames for contacts
        """
        self.specified_args = locals()  # used for __repr__, see below
        # EWrapper.__init__(self)
        # AlgoWrapper.__init__(self)
        from scottbrian_algo1.algo_wrapper import AlgoWrapper

        self.algo_wrapper = AlgoWrapper(
            group_name=group_name,
            algo_name=algo_name,
            client_name=client_name,
            algo_client=self,
            response_complete_event=response_complete_event,
            market_data=market_data,
            # symbols=symbols,
            # stock_symbols=stock_symbols,
            # contracts=contracts,
            # contract_details=contract_details,
        )
        EClient.__init__(self, wrapper=self.algo_wrapper)
        Thread.__init__(self)
        # threading.current_thread().name = algo_name
        self.group_name = group_name
        self.algo_name = algo_name
        self.client_name = client_name

        self.msg_prefix = f"AlgoClient {algo_name} ({group_name})"

        SmartThread.__init__(
            self,
            group_name=group_name,
            name=self.client_name,
            thread=self,
            auto_start=False,
        )

        self.disconnect_lock = sel.SELock()

        self.active_requests: dict[int, ClientRequestBlock] = {}

        self.request_id: ReqID = 0
        # self.ds_catalog = ds_catalog

        # self.error_reqId: int = 0
        # self.nextValidId_event = Event()
        #
        # # stock symbols
        # self.request_throttle_secs = AlgoApp.REQUEST_THROTTLE_SECONDS
        # self.symbols_status = pd.DataFrame()
        # self.num_symbols_received = 0
        # self.symbols = symbols
        # self.stock_symbols = stock_symbols
        # #
        # # # contract details
        # self.contracts = contracts
        # self.contract_details = contract_details
        #
        # # fundamental data
        # self.fundamental_data = pd.DataFrame()
        #
        # self.handle_cmds: bool = False
        #
        # self.client_name = "ibapi_client"
        # self.ibapi_client_smart_thread = SmartThread(
        #     name=self.client_name,
        #     target=EClient.run,
        #     args=(self,),
        #     auto_start=False,
        # )

        # if thread_config == ThreadConfig.CurrentThread:
        #     if (smart_thread := SmartThread.get_current_smart_thread()) is not None:
        #         self.algo_name = smart_thread.name
        #         self.algo1_smart_thread = smart_thread
        #     else:
        #         self.algo_name = algo_name
        #         self.algo1_smart_thread = SmartThread(name=self.algo_name)
        # elif thread_config == ThreadConfig.RemoteThread:
        #     self.algo_name = algo_name
        #     self.algo1_smart_thread = SmartThread(
        #         name=self.algo_name,
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
            __class__: Type[AlgoClient]  # noqa: F842
        classname = self.__class__.__name__
        parms = f"algo_name='{self.algo_name}'"

        return f"{classname}({parms})"

    ####################################################################
    # add_active_request
    ####################################################################
    def add_active_request(self, req_id: int, req_block: ClientRequestBlock) -> None:
        """Add an active request to the active request array.

        Args:
            req_id: request ID for the new request
            req_block: request block for the new request

        Raises:
            RequestIdInUse: RequestID req_id already in use.

        """
        if req_id in self.active_requests:
            raise RequestIdInUse(f"RequestID {req_id} already in use.")

        self.active_requests[req_id] = req_block

    ####################################################################
    # update_active_request
    ####################################################################
    def update_active_request(self, req_id: int) -> None:
        """Update an active request on the active request array.

        Args:
            req_id: request ID for the new request

        Raises:
            RequestIdNotFound: RequestID req_id not found.

        """
        if req_id not in self.active_requests:
            raise RequestIdNotFound(f"RequestID {req_id} not found.")

        self.active_requests[req_id].request_complete = True

    ####################################################################
    # pop_active_request
    ####################################################################
    def pop_active_request(self, req_id: int) -> ClientRequestBlock:
        """Remove and return a request on the active request array.

        Args:
            req_id: request ID for the request to remove and return

        Raises:
            RequestIdNotFound: RequestID req_id not found.

        """
        if req_id not in self.active_requests:
            raise RequestIdNotFound(f"RequestID {req_id} not found.")

        ret_block = self.active_requests.pop(req_id)
        return ret_block

    ####################################################################
    # msgLoopRec
    ####################################################################
    def msgLoopRec(self):
        for req_id, act_req in self.active_requests.items():
            if act_req.request_complete:
                act_req.request_resumed = True
                self.smart_resume(waiters=act_req.requestor_name)

    ###########################################################################
    # get_req_id
    ###########################################################################
    def get_req_id(self) -> ReqID:
        """Obtain a request id to use for the current request.

        The request id is bumped and then returned

        Returns:
            request id to use on the current request

        """
        self.request_id += 1
        return self.request_id

    ###########################################################################
    # disconnect
    ###########################################################################
    @etrace(enable_trace=algo_client_etrace)
    def disconnect(self) -> None:
        """Call this function to terminate the connections with TWS."""
        # This method overloads the EClient.disconnect to address a
        # problem with EClient.disconnect. EClient.disconnect fails to
        # join the reader thread to ensure the reader thread has ended.
        # This leads to potential problems if a connect is done
        # immediately after the disconnect. The still running reader
        # thread snatches the early handshaking messages and leaves the
        # connect hanging.
        # The following code is from client.py and is modified here to
        # add the thread join to ensure the reader ends before the
        # disconnect returns.
        # Note also the use of the disconnect lock to serialize the two
        # known cases of disconnect being called from different threads:
        # 1) from mainline through disconnect_from_ib in AlgoApp
        # 2) from the EClient run method in the run thread.

        with sel.SELockExcl(self.disconnect_lock):
            logger.debug(
                f"{self.msg_prefix} setting conn state to EClient.DISCONNECTED"
            )
            self.setConnState(EClient.DISCONNECTED)
            if self.conn is not None:
                logger.debug(f"{self.msg_prefix} disconnecting")
                self.conn.disconnect()
                self.wrapper.connectionClosed()
                reader_id = id(self.reader)
                my_id = get_ident()
                logger.debug(
                    f"{self.msg_prefix} about to join reader {reader_id=} for {my_id=}"
                )
                self.reader.join()
                logger.debug(
                    f"{self.msg_prefix} back from join {reader_id=} for {my_id=}"
                )
                self.reset()

    # ###########################################################################
    # # symbolSamples - callback
    # ###########################################################################
    # def symbolSamples(
    #     self,
    #     request_id: int,
    #     contract_descriptions: ibcommon.ListOfContractDescription,
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
    #
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
    #     if conId in self.contracts.index:
    #         self.contracts.loc[conId] = pd.Series(contract_dict)
    #     else:
    #         self.contracts = pd.concat(
    #             [self.contracts, pd.DataFrame(contract_dict, index=[conId])]
    #         )
    #
    #     # add the contract details to the DataFrame
    #     contract_details_dict = get_contract_details_dict(contract_details)
    #     if conId in self.contract_details.index:
    #         self.contract_details.loc[conId] = pd.Series(contract_details_dict)
    #     else:
    #         self.contract_details = pd.concat(
    #             [
    #                 self.contract_details,
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
