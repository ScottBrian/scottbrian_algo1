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
import logging
import threading
from threading import Event, get_ident, get_native_id, Lock, Thread
from typing import Any, Callable, Optional, Type, TYPE_CHECKING, Union

########################################################################
# Third Party
########################################################################
from ibapi.client import EClient  # type: ignore
import ibapi.common as ibcommon
from ibapi.contract import Contract, ContractDetails  # type: ignore
from ibapi.wrapper import EWrapper  # type: ignore
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
from scottbrian_algo1.algo_client import AlgoClient
from scottbrian_algo1.algo_data import MarketData

########################################################################
# logging
########################################################################
logger = logging.getLogger(__name__)


########################################################################
# AlgoApp
########################################################################
class AlgoWrapper(EWrapper):  # type: ignore
    """AlgoWrapper class."""

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
        algo_client: AlgoClient,
        response_complete_event: Event,
        market_data: MarketData,
        # symbols: pd.DataFrame,
        # stock_symbols: pd.DataFrame,
        # contracts: pd.DataFrame,
        # contract_details: pd.DataFrame,
    ) -> None:
        """Instantiate the AlgoClient.

        Args:
            algo_name: name of main algo
            client_name: name of client for EClient class instance

        """
        self.specified_args = locals()  # used for __repr__, see below
        EWrapper.__init__(self)
        self.group_name = group_name
        self.algo_name = algo_name
        self.client_name = client_name
        self.algo_client = algo_client

        self.msg_prefix = f"AlgoWrapper {self.algo_name} ({self.group_name})"

        # self.ds_catalog = ds_catalog
        self.request_id: int = 0
        self.error_reqId: int = 0
        self.response_complete_event = response_complete_event
        self.market_data = market_data
        # self.nextValidId_event = Event()
        #
        # # stock symbols
        # self.request_throttle_secs = AlgoApp.REQUEST_THROTTLE_SECONDS
        # self.symbols_status = pd.DataFrame()
        # self.num_symbols_received = 0
        # self.symbols = symbols
        # self.stock_symbols = stock_symbols
        #
        # # contract details
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
            __class__: Type[AlgoWrapper]  # noqa: F842
        classname = self.__class__.__name__
        parms = ""

        for key, item in self.specified_args.items():
            if item:  # if not None
                if key in ("group_name", "algo_name", "client_name"):
                    parms += ", " + f"{key}={item}"

        return f"{classname}({parms})"

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
        logger.info(f"{self.msg_prefix} next valid ID is {request_id}")

        self.algo_client.request_id = request_id
        self.algo_client.update_active_request(req_id=request_id)

    ####################################################################
    # symbolSamples - callback
    ####################################################################
    def symbolSamples(
        self,
        request_id: int,
        contract_descriptions: ibcommon.ListOfContractDescription,
    ) -> None:
        """Receive IB reply for reqMatchingSymbols request.

        Args:
            request_id: the id used on the request
            contract_descriptions: contains a list of contract
                descriptions. Each description includes the symbol,
                conId, security type, primary exchange, currency, and
                derivative security types.

        The contracts are filtered for stocks traded in the USA and are
        stored into a data frame as contracts that can be used later to
        request additional information or to make trades.
        """
        logger.info("entered for request_id %d", request_id)
        self.market_data.num_symbols_received = len(contract_descriptions)
        logger.info(
            "Number of descriptions received: %d", self.market_data.num_symbols_received
        )

        for desc in contract_descriptions:
            logger.debug("Symbol: {}".format(desc.contract.symbol))

            conId = desc.contract.conId

            if (
                desc.contract.secType == "STK"
                and desc.contract.currency == "USD"
                and "OPT" in desc.derivativeSecTypes
            ):
                if conId in self.market_data.stock_symbols.index:
                    self.market_data.stock_symbols.loc[conId] = pd.Series(
                        get_contract_description_dict(desc)
                    )
                else:
                    self.market_data.stock_symbols = pd.concat(
                        [
                            self.market_data.stock_symbols,
                            pd.DataFrame(
                                get_contract_description_dict(desc, df=True),
                                index=[conId],
                            ),
                        ]
                    )
            else:  # all other symbols
                # update the descriptor if it already exists in the DataFrame
                # as we want the newest information to replace the old
                if conId in self.market_data.symbols.index:
                    self.market_data.symbols.loc[conId] = pd.Series(
                        get_contract_description_dict(desc)
                    )
                else:
                    self.market_data.symbols = pd.concat(
                        [
                            self.market_data.symbols,
                            pd.DataFrame(
                                get_contract_description_dict(desc, df=True),
                                index=[conId],
                            ),
                        ]
                    )

        self.response_complete_event.set()

    ####################################################################
    # contractDetails callback method
    ####################################################################
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
        if conId in self.market_data.contracts.index:
            self.market_data.contracts.loc[conId] = pd.Series(contract_dict)
        else:
            self.market_data.contracts = pd.concat(
                [self.market_data.contracts, pd.DataFrame(contract_dict, index=[conId])]
            )

        # add the contract details to the DataFrame
        contract_details_dict = get_contract_details_dict(contract_details)
        if conId in self.market_data.contract_details.index:
            self.market_data.contract_details.loc[conId] = pd.Series(
                contract_details_dict
            )
        else:
            self.market_data.contract_details = pd.concat(
                [
                    self.market_data.contract_details,
                    pd.DataFrame(contract_details_dict, index=[conId]),
                ]
            )

    ####################################################################
    # contractDetailsEnd
    ####################################################################
    def contractDetailsEnd(self, request_id: int) -> None:
        """Receive IB reply for reqContractDetails request end.

        Args:
            request_id: the id used on the request

        """
        logger.info("entered for request_id %d", request_id)
        self.response_complete_event.set()

    ####################################################################
    # fundamentalData callback method
    ####################################################################
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
        # self.market_data.contracts.drop(conId,
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
        # self.market_data.contracts = self.market_data.contracts.append(
        #             pd.DataFrame(contract_dict,
        #                          index=[conId]))

        # remove the contract_details if it already exists in the DataFrame
        # as we want the newest information to replace the old
        # self.market_data.contract_details.drop(conId,
        #                            inplace=True,
        #                            errors='ignore')

        # remove the contract from the contract_details
        # contract_details.contract = None

        # add the contract details to the DataFrame
        # contract_details_dict = get_contract_details_dict(contract_details)
        # self.market_data.contract_details = self.market_data.contract_details.append(
        #             pd.DataFrame(contract_details_dict,
        #                          index=[conId]))

        # print('self.market_data.contract_details:\n', contract_details)
        # print('self.market_data.contract_details.__dict__:\n',
        #       self.market_data.contract_details.__dict__)
