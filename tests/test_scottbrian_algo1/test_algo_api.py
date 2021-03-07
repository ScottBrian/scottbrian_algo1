"""test_algo_api.py module."""

# from datetime import datetime, timedelta
# import pytest
# import sys

import pandas as pd
from typing import Any  # Callable, cast, Tuple, Union
# from typing_extensions import Final

from ibapi.contract import ContractDescription

from scottbrian_algo1.algo_api import AlgoApp
from scottbrian_utils.diag_msg import diag_msg
from scottbrian_utils.file_catalog import FileCatalog
# from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# class ErrorTstTimeHdr(Exception):
#     """Base class for exception in this module."""
#     pass
#
#
# class InvalidRouteNum(ErrorTstTimeHdr):
#     """InvalidRouteNum exception class."""
#     pass


# enabled_arg_list = ['0',
#                     'static_true',
#                     'static_false',
#                     'dynamic_true',
#                     'dynamic_false'
#                     ]
#
#
# @pytest.fixture(params=enabled_arg_list)  # type: ignore
# def enabled_arg(request: Any) -> str:
#     """Determines how to specify time_box_enabled.
#
#     Args:
#         request: special fixture that returns the fixture params
#
#     Returns:
#         The params values are returned one at a time
#     """
#     return cast(str, request.param)


class TestAlgoApp:
    """TestAlgoApp class."""

    # @pytest.fixture(scope='class')  # type: ignore
    # def hdr(self) -> "StartStopHeader":
    #     """Method hdr.
    #
    #     Returns:
    #         StartStopHeader instance
    #     """
    #     return StartStopHeader('TestName')

    def test_mock_connect_to_IB(self,
                                algo_app: "AlgoApp",
                                monkeypatch: Any) -> None:
        """Test connecting to IB.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            monkeypatch: pytest fixture

        """
        # def mock_client_connect(ip_addr: str,
        #                         port: int,
        #                         client_id: int) -> None:
        #     diag_msg('ip_addr:', ip_addr,
        #              'port:', port,
        #              'client_id:', client_id)
        #     # algo_app.nextValidId(1)
        #     return None
        #
        # # 'connect' is a method in the IB EClient class. We use monkeypatch
        # # here to have mock_client_connect patched in to take its place so that
        # # we don't really connect to IB. This allows us to test without using
        # # the real connection.
        # monkeypatch.setattr(algo_app, "connect", mock_client_connect)
        assert len(algo_app.ds_catalog) > 0
        assert algo_app.next_request_id == 0
        assert algo_app.stock_symbols.empty
        assert algo_app.response_complete_event.is_set() is False
        assert algo_app.nextValidId_event.is_set() is False
        assert algo_app.run_thread.is_alive() is False

        # we are testing connect_to_ib and the subsequent code that gets
        # control as a result, such as getting the first requestID and then
        # starting a separate thread for the run loop.
        logger.debug("about to connect")
        connect_ans = algo_app.connect_to_ib("127.0.0.1", 7496, client_id=0)

        assert connect_ans
        # diag_msg("about to wait")

        # we will wait on the first requestID here
        # algo_app.nextValidId_event.wait()
        # diag_msg("back from wait")

        # make sure our next_request_id was updated from the initial value
        # of zero to the new value of 1
        assert algo_app.next_request_id == 1
        algo_app.disconnect_from_ib()

    # def test_real_connect_to_IB(self,
    #                             #algo_app: "AlgoApp",
    #                             monkeypatch: Any) -> None:
    #     """Test connecting to IB.
    #
    #     Args:
    #         algo_app: instance of AlgoApp from conftest pytest fixture
    #         monkeypatch: pytest fixture
    #
    #     """
    #     algo_app = AlgoApp()
    #     # def mock_client_connect(ip_addr: str,
    #     #                         port: int,
    #     #                         client_id: int) -> None:
    #     #     diag_msg('ip_addr:', ip_addr,
    #     #              'port:', port,
    #     #              'client_id:', client_id)
    #     #     # algo_app.nextValidId(1)
    #     #     return None
    #     #
    #     # # 'connect' is a method in the IB EClient class. We use monkeypatch
    #     # # here to have mock_client_connect patched in to take its place so that
    #     # # we don't really connect to IB. This allows us to test without using
    #     # # the real connection.
    #     # monkeypatch.setattr(algo_app, "connect", mock_client_connect)
    #
    #     assert algo_app.next_request_id == 0
    #
    #     print()
    #     # diag_msg("about to connect")
    #
    #     # we are testing connect_to_ib and the subsequent code that gets
    #     # control as a result, such as getting the first requestID and then
    #     # starting a separate thread for the run loop.
    #     connect_ans = algo_app.connect_to_ib("127.0.0.1", 7496, client_id=0)
    #
    #     assert connect_ans
    #     # diag_msg("about to wait")
    #
    #     # we will wait on the first requestID here
    #     # algo_app.nextValidId_event.wait()
    #     # diag_msg("back from wait")
    #
    #     # make sure our next_request_id was updated from the initial value
    #     # of zero to the new value of 1
    #     assert algo_app.next_request_id == 1
    #
    #     algo_app.disconnect_from_ib()

    def test_request_symbols(self,
                             algo_app: "AlgoApp",
                             monkeypatch: Any) -> None:
        """Test getting symbols and placing into data base.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            monkeypatch: pytest fixture

        The steps are:
        
            * create mock symbols for test that mock ib will return
            * mock connect to ib
            * request symbols that start with a
            * verify that table contains expected symbols and info for a
            * request symbols that start with c
            * verify that table contains expected symbols and info for a and c
            

        
        """
        assert len(algo_app.ds_catalog) > 0
        assert algo_app.next_request_id == 0
        assert algo_app.stock_symbols.empty
        assert algo_app.response_complete_event.is_set() is False
        assert algo_app.nextValidId_event.is_set() is False
        assert algo_app.run_thread.is_alive() is False

        logger.debug("about to connect")
        connect_ans = algo_app.connect_to_ib("127.0.0.1", 7496, client_id=0)
        assert connect_ans

        # delete the stock_symbol csv file if it exists
        stock_symbols_path = algo_app.ds_catalog.get_path('symbols')
        logger.info('path: %s', stock_symbols_path)
        stock_symbols_path.unlink(missing_ok=True)

        # create mock data base of symbols to be returned by mock ib
        # call reqMatchingSymbols for
        # case 1: unknown symbol
        # case 2: 1 known exact case
        # case 3: 1 known case with short search symbol
        # case 4: 2 known cases with short search symbol

        contract_description = ContractDescription()
        contract_description.contract.conId = 12345678
        contract_description.contract.symbol = 'ABCD'
        contract_description.contract.secType = 'STK'
        contract_description.contract.primaryExchange = 'NYSE'
        contract_description.contract.currency = 'USD'

        contract_descriptions = pd.DataFrame()
        contract_descriptions = contract_descriptions.append(
            pd.DataFrame([[contract_description.contract.conId,
                           contract_description.contract.symbol,
                           contract_description.contract.secType,
                           contract_description.contract.primaryExchange,
                           contract_description.contract.currency
                           ]],
                         columns=['conId',
                                  'symbol',
                                  'secType',
                                  'primaryExchange',
                                  'currency']))
        diag_msg('contract_descriptions:', contract_descriptions)
        con_descs_path = \
            algo_app.ds_catalog.get_path('mock_con_descs')
        contract_descriptions.to_csv(con_descs_path)

        # make request to get symbol that is not in the mock data set
        symbol_to_get = 'B'
        algo_app.request_symbols(symbol_to_get)
        # start_char = 'B'
        # end_char = 'B'
        # algo_app.get_symbols(start_char, end_char)

        # verify that the symbol table is empty
        assert algo_app.stock_symbols.empty

        # mock_data_frame = pd.read_csv(stock_symbols_path)
        #
        # assert mock_data_frame.empty

        # make request for symbol that will be returned
        symbol_to_get = 'A'
        algo_app.request_symbols(symbol_to_get)

        # verify symbol table has one entry for the symbol
        assert len(algo_app.stock_symbols) == 1
        assert algo_app.stock_symbols.iloc[0].symbol == \
               contract_description.contract.symbol
        assert algo_app.stock_symbols.iloc[0].conId == \
               contract_description.contract.conId
        assert algo_app.stock_symbols.iloc[0].primaryExchange == \
               contract_description.contract.primaryExchange

        algo_app.disconnect_from_ib()





