"""test_algo_api.py module."""

# from datetime import datetime, timedelta
# import pytest
# import sys
from pathlib import Path

import pandas as pd
from typing import Any  # Callable, cast, Tuple, Union
# from typing_extensions import Final

from ibapi.contract import ContractDescription

from scottbrian_algo1.algo_api import AlgoApp
from scottbrian_utils.diag_msg import diag_msg
from scottbrian_utils.file_catalog import FileCatalog
# from datetime import datetime
import logging

from test_scottbrian_algo1.const import MAX_CONTRACT_DESCS_RETURNED, \
    PORT_FOR_REQID_TIMEOUT

logger = logging.getLogger(__name__)

# class ErrorTstTimeHdr(Exception):
#     """Base class for exception in this module."""
#     pass
#
#
# class InvalidRouteNum(ErrorTstTimeHdr):
#     """InvalidRouteNum exception class."""
#     pass


def verify_algo_app_initialized(algo_app):
    assert len(algo_app.ds_catalog) > 0
    assert algo_app.next_request_id == 0
    assert algo_app.stock_symbols.empty
    assert algo_app.response_complete_event.is_set() is False
    assert algo_app.nextValidId_event.is_set() is False
    assert algo_app.run_thread.is_alive() is False

def verify_algo_app_connected(algo_app):
    assert algo_app.run_thread.is_alive()
    assert algo_app.isConnected()
    assert algo_app.next_request_id == 1

def verify_algo_app_disconnected(algo_app):
    assert not algo_app.run_thread.is_alive()
    assert not algo_app.isConnected()

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
                                algo_app: "AlgoApp"
                                ) -> None:
        """Test connecting to IB.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)

        """
        verify_algo_app_initialized(algo_app)

        # we are testing connect_to_ib and the subsequent code that gets
        # control as a result, such as getting the first requestID and then
        # starting a separate thread for the run loop.
        logger.debug("about to connect")
        assert algo_app.connect_to_ib("127.0.0.1", 7496, client_id=0)

        # verify that algo_app is connected and alive with a valid reqId
        verify_algo_app_connected(algo_app)

        algo_app.disconnect_from_ib()
        verify_algo_app_disconnected(algo_app)

    def test_mock_connect_to_IB_with_timeout(self,
                                             algo_app: "AlgoApp",
                                             ) -> None:
        """Test connecting to IB.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)

        """
        verify_algo_app_initialized(algo_app)

        # we are testing connect_to_ib with a simulated timeout
        logger.debug("about to connect")
        assert not algo_app.connect_to_ib("127.0.0.1",
                                          PORT_FOR_REQID_TIMEOUT,
                                          client_id=0)
        # verify that algo_app is not connected
        verify_algo_app_disconnected(algo_app)
        assert algo_app.next_request_id == 0


    # def test_real_connect_to_IB(self) -> None:
    #     """Test connecting to IB.
    #
    #     Args:
    #         algo_app: instance of AlgoApp from conftest pytest fixture
    #         monkeypatch: pytest fixture
    #
    #     """
    #     proj_dir = Path.cwd().resolve().parents[1]  # back two directories
    #     test_cat = \
    #         FileCatalog({'symbols': Path(proj_dir / 't_datasets/symbols.csv')
    #                      })
    #     algo_app = AlgoApp(test_cat)
    #     verify_algo_app_initialized(algo_app)
    #
    #     # we are testing connect_to_ib and the subsequent code that gets
    #     # control as a result, such as getting the first requestID and then
    #     # starting a separate thread for the run loop.
    #     logger.debug("about to connect")
    #     connect_ans = algo_app.connect_to_ib("127.0.0.1", 7496, client_id=0)
    #
    #     # verify that algo_app is connected and alive with a valid reqId
    #     assert connect_ans
    #     assert algo_app.run_thread.is_alive()
    #     assert algo_app.isConnected()
    #     assert algo_app.next_request_id == 1
    #
    #     algo_app.disconnect_from_ib()
    #     assert not algo_app.run_thread.is_alive()
    #     assert not algo_app.isConnected()


    def test_request_symbols_null_result(self,
                                         algo_app: "AlgoApp",
                                         nonexistent_symbol_arg) -> None:
        """Test request_symbols with unmatched pattern.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_contract_descriptions: pytest fixture of contract_descriptions

        The steps are:
            * mock connect to ib
            * request symbols that will not be found
            * verify that stock symbols table is empty

        """
        verify_algo_app_initialized(algo_app)

        logger.debug("about to connect")
        assert algo_app.connect_to_ib("127.0.0.1", 7496, client_id=0)
        verify_algo_app_connected(algo_app)

        # delete the stock_symbol csv file if it exists
        stock_symbols_path = algo_app.ds_catalog.get_path('symbols')
        logger.info('path: %s', stock_symbols_path)
        stock_symbols_path.unlink(missing_ok=True)

        # make request to get symbol that is not in the mock data set
        algo_app.request_symbols(nonexistent_symbol_arg)

        # verify that the symbol table is empty
        assert algo_app.stock_symbols.empty

        algo_app.disconnect_from_ib()
        verify_algo_app_disconnected(algo_app)

    def test_request_symbols_one_result(self,
                                        algo_app: "AlgoApp",
                                        mock_contract_descriptions) -> None:
        """Test request_symbols with pattern that finds exactly one symbol.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_contract_descriptions: pytest fixture of contract_descriptions

        The steps are:
            * mock connect to ib
            * request symbols with pattern for one match
            * verify that stock symbols table has the expected entry

        """
        verify_algo_app_initialized(algo_app)

        logger.debug("about to connect")
        assert algo_app.connect_to_ib("127.0.0.1", 7496, client_id=0)
        verify_algo_app_connected(algo_app)

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

        # verify that the symbol table is empty
        diag_msg('mock_contract_descriptions', mock_contract_descriptions)

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
        verify_algo_app_disconnected(algo_app)



