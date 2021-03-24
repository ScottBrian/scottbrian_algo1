"""test_algo_api.py module."""

# from datetime import datetime, timedelta
import pytest
# import sys
# from pathlib import Path
import numpy as np
import pandas as pd  # type: ignore
import string
import math

from typing import Any, List, Tuple  # Callable, cast, Tuple, Union
# from typing_extensions import Final

from ibapi.contract import Contract, ContractDetails

from scottbrian_algo1.algo_api import AlgoApp, AlreadyConnected, \
    DisconnectLockHeld, ConnectTimeout

from scottbrian_utils.diag_msg import diag_msg
# from scottbrian_utils.file_catalog import FileCatalog
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


###############################################################################
# connect and disconnect
###############################################################################
class TestAlgoAppConnect:
    """TestAlgoAppConnect class."""

    def test_mock_connect_to_ib(self,
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
        algo_app.connect_to_ib("127.0.0.1",
                               algo_app.PORT_FOR_LIVE_TRADING,
                               client_id=0)

        # verify that algo_app is connected and alive with a valid reqId
        verify_algo_app_connected(algo_app)

        algo_app.disconnect_from_ib()
        verify_algo_app_disconnected(algo_app)

    def test_mock_connect_to_ib_with_timeout(self,
                                             algo_app: "AlgoApp",
                                             mock_ib: Any
                                             ) -> None:
        """Test connecting to IB.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        # we are testing connect_to_ib with a simulated timeout
        logger.debug("about to connect")
        with pytest.raises(ConnectTimeout):
            algo_app.connect_to_ib("127.0.0.1",
                                   mock_ib.PORT_FOR_REQID_TIMEOUT,
                                   client_id=0)

        # verify that algo_app is not connected
        verify_algo_app_disconnected(algo_app)
        assert algo_app.request_id == 0

    def test_connect_to_ib_already_connected(self,
                                             algo_app: "AlgoApp",
                                             mock_ib: Any
                                             ) -> None:
        """Test connecting to IB.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        # first, connect normally to mock_ib
        logger.debug("about to connect")
        algo_app.connect_to_ib("127.0.0.1",
                               algo_app.PORT_FOR_PAPER_TRADING,
                               client_id=0)
        # verify that algo_app is connected
        verify_algo_app_connected(algo_app)

        # try to connect again - should get error
        with pytest.raises(AlreadyConnected):
            algo_app.connect_to_ib("127.0.0.1",
                                   algo_app.PORT_FOR_PAPER_TRADING,
                                   client_id=0)

        # verify that algo_app is still connected and alive with a valid reqId
        verify_algo_app_connected(algo_app)

        algo_app.disconnect_from_ib()
        verify_algo_app_disconnected(algo_app)

    def test_connect_to_ib_with_lock_held(self,
                                          algo_app: "AlgoApp",
                                          mock_ib: Any
                                          ) -> None:
        """Test connecting to IB with disconnect lock held.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        # obtain the disconnect lock
        logger.debug("about to obtain disconnect lock")
        algo_app.disconnect_lock.acquire()

        # try to connect - should get error
        with pytest.raises(DisconnectLockHeld):
            algo_app.connect_to_ib("127.0.0.1",
                                   algo_app.PORT_FOR_LIVE_TRADING,
                                   client_id=0)

        # verify that algo_app is still simply initialized
        verify_algo_app_initialized(algo_app)

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
    #     assert algo_app.request_id == 1
    #
    #     algo_app.disconnect_from_ib()
    #     assert not algo_app.run_thread.is_alive()
    #     assert not algo_app.isConnected()


###############################################################################
# connect disconnect verification
###############################################################################
def verify_algo_app_initialized(algo_app: "AlgoApp") -> None:
    """Helper function to verify the also_app instance is initialized.

    Args:
        algo_app: instance of AlgoApp that is to be checked

    """
    assert len(algo_app.ds_catalog) > 0
    assert algo_app.request_id == 0
    assert algo_app.stock_symbols.empty
    assert algo_app.response_complete_event.is_set() is False
    assert algo_app.nextValidId_event.is_set() is False
    assert algo_app.__repr__() == 'AlgoApp(ds_catalog)'
    # assert algo_app.run_thread is None


def verify_algo_app_connected(algo_app: "AlgoApp") -> None:
    """Helper function to verify we are connected to ib.

    Args:
        algo_app: instance of AlgoApp that is to be checked

    """
    assert algo_app.run_thread.is_alive()
    assert algo_app.isConnected()
    assert algo_app.request_id == 1


def verify_algo_app_disconnected(algo_app: "AlgoApp") -> None:
    """Helper function to verify we are disconnected from ib.

    Args:
        algo_app: instance of AlgoApp that is to be checked

    """
    assert not algo_app.run_thread.is_alive()
    assert not algo_app.isConnected()


###############################################################################
# matching symbols
###############################################################################
class TestAlgoAppMatchingSymbols:
    """TestAlgoAppMatchingSymbols class."""
    def test_request_symbols_null_result(self,
                                         algo_app: "AlgoApp",
                                         mock_ib: Any,
                                         nonexistent_symbol_arg: str) -> None:
        """Test request_symbols with unmatched pattern.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions
            nonexistent_symbol_arg: pytest fixture of symbols

        The steps are:
            * mock connect to ib
            * request symbols that will not be found
            * verify that stock symbols table is empty

        """
        verify_algo_app_initialized(algo_app)
        num_exp_recursive = 0
        num_exp_non_recursive = 0
        # verify symbol table has zero entries for the symbol
        verify_match_symbols(algo_app,
                             mock_ib,
                             nonexistent_symbol_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=1)

    def test_request_symbols_zero_result(self,
                                         algo_app: "AlgoApp",
                                         mock_ib: Any,
                                         symbol_pattern_match_0_arg: str
                                         ) -> None:
        """Test request_symbols with pattern that finds exactly 1 symbol.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions
            symbol_pattern_match_0_arg: symbols to use for searching

        The steps are:
            * mock connect to ib
            * request symbols with pattern for one match
            * verify that stock symbols table has the expected entry

        """
        verify_algo_app_initialized(algo_app)
        num_exp_recursive = 0
        num_exp_non_recursive = 0
        # verify symbol table has zero entries for the symbol
        logger.debug("calling verify_match_symbols req_type 1")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_0_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=1)

        logger.debug("calling verify_match_symbols req_type 2")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_0_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=2)

    def test_request_symbols_one_result(self,
                                        algo_app: "AlgoApp",
                                        mock_ib: Any,
                                        symbol_pattern_match_1_arg: str
                                        ) -> None:
        """Test request_symbols with pattern that finds exactly 1 symbol.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions
            symbol_pattern_match_1_arg: symbols to use for searching

        The steps are:
            * mock connect to ib
            * request symbols with pattern for one match
            * verify that stock symbols table has the expected entry

        """
        verify_algo_app_initialized(algo_app)
        num_exp_recursive = 1
        num_exp_non_recursive = 1
        # verify symbol table has one entry for the symbol
        logger.debug("calling verify_match_symbols req_type 1")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_1_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=1)

        logger.debug("calling verify_match_symbols req_type 2")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_1_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=2)

    def test_request_symbols_two_result(self,
                                        algo_app: "AlgoApp",
                                        mock_ib: Any,
                                        symbol_pattern_match_2_arg: str
                                        ) -> None:
        """Test request_symbols with pattern that finds exactly 2 symbols.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions
            symbol_pattern_match_2_arg: symbols to use for searching

        """
        verify_algo_app_initialized(algo_app)
        num_exp_recursive = 2
        num_exp_non_recursive = 2
        # verify symbol table has 2 entries for the symbol
        logger.debug("calling verify_match_symbols req_type 1")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_2_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=1)

        logger.debug("calling verify_match_symbols req_type 2")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_2_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=2)

    def test_request_symbols_three_result(self,
                                          algo_app: "AlgoApp",
                                          mock_ib: Any,
                                          symbol_pattern_match_3_arg: str
                                          ) -> None:
        """Test request_symbols with pattern that finds exactly 3 symbols.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions
            symbol_pattern_match_3_arg: symbols to use for searching

        """
        verify_algo_app_initialized(algo_app)
        num_exp_recursive = 3
        num_exp_non_recursive = 3
        # verify symbol table has 3 entries for the symbol
        logger.debug("calling verify_match_symbols req_type 1")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_3_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=1)

        logger.debug("calling verify_match_symbols req_type 2")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_3_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=2)

    def test_request_symbols_four_result(self,
                                         algo_app: "AlgoApp",
                                         mock_ib: Any,
                                         symbol_pattern_match_4_arg: str
                                         ) -> None:
        """Test request_symbols with pattern that finds exactly 4 symbols.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions
            symbol_pattern_match_4_arg: symbols to use for searching

        """
        verify_algo_app_initialized(algo_app)
        num_exp_recursive = 4
        num_exp_non_recursive = 4
        # verify symbol table has 4 entries for the symbol
        logger.debug("calling verify_match_symbols req_type 1")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_4_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=1)

        logger.debug("calling verify_match_symbols req_type 2")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_4_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=2)

    def test_request_symbols_eight_result(self,
                                          algo_app: "AlgoApp",
                                          mock_ib: Any,
                                          symbol_pattern_match_8_arg: str
                                          ) -> None:
        """Test request_symbols with pattern that finds exactly 8 symbols.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions
            symbol_pattern_match_8_arg: symbols to use for searching

        """
        verify_algo_app_initialized(algo_app)
        num_exp_recursive = 8
        num_exp_non_recursive = 8
        # verify symbol table has 8 entries for the symbol
        logger.debug("calling verify_match_symbols req_type 1")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_8_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=1)

        logger.debug("calling verify_match_symbols req_type 2")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_8_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=2)

    def test_request_symbols_twelve_result(self,
                                           algo_app: "AlgoApp",
                                           mock_ib: Any,
                                           symbol_pattern_match_12_arg: str
                                           ) -> None:
        """Test request_symbols with pattern that finds exactly 12 symbols.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions
            symbol_pattern_match_12_arg: symbols to use for searching

        """
        verify_algo_app_initialized(algo_app)
        num_exp_recursive = 12
        num_exp_non_recursive = 12
        # verify symbol table has 12 entries for the symbol
        logger.debug("calling verify_match_symbols req_type 1")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_12_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=1)

        logger.debug("calling verify_match_symbols req_type 2")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_12_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=2)

    def test_request_symbols_sixteen_result(self,
                                            algo_app: "AlgoApp",
                                            mock_ib: Any,
                                            symbol_pattern_match_16_arg: str
                                            ) -> None:
        """Test request_symbols with pattern that finds exactly 16 symbols.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions
            symbol_pattern_match_16_arg: symbols to use for searching

        """
        verify_algo_app_initialized(algo_app)
        num_exp_recursive = 16
        num_exp_non_recursive = 16
        # verify symbol table has 16 entries for the symbol
        logger.debug("calling verify_match_symbols req_type 1")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_16_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=1)

        logger.debug("calling verify_match_symbols req_type 2")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_16_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=2)

    def test_request_symbols_twenty_result(self,
                                           algo_app: "AlgoApp",
                                           mock_ib: Any,
                                           symbol_pattern_match_20_arg: str
                                           ) -> None:
        """Test request_symbols with pattern that finds exactly 20 symbols.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions
            symbol_pattern_match_20_arg: symbols to use for searching

        """
        verify_algo_app_initialized(algo_app)
        num_exp_recursive = 20
        num_exp_non_recursive = 16
        # verify symbol table has 16/20 entries for the symbol
        logger.debug("calling verify_match_symbols req_type 1")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_20_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=1)

        logger.debug("calling verify_match_symbols req_type 2")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_20_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=2)

    def test_get_symbols_recursive(self,
                                   algo_app: "AlgoApp",
                                   mock_ib: Any,
                                   get_symbols_search_char_arg: str) -> None:
        """Test get_symbols with single letter patterns.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions
            get_symbols_search_char_arg: single character to use for searching

        """
        verify_algo_app_initialized(algo_app)
        num_exp_recursive, num_exp_non_recursive = \
            get_exp_number(get_symbols_search_char_arg, mock_ib)

        # verify symbol table has correct entries for the symbol
        logger.debug("calling verify_match_symbols req_type 1")
        verify_match_symbols(algo_app,
                             mock_ib,
                             get_symbols_search_char_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=1)

        logger.debug("calling verify_match_symbols req_type 2")
        verify_match_symbols(algo_app,
                             mock_ib,
                             get_symbols_search_char_arg,
                             exp_recursive_matches=num_exp_recursive,
                             exp_non_recursive_matches=num_exp_non_recursive,
                             req_type=2)

    def test_get_symbols(self,
                         algo_app: "AlgoApp",
                         mock_ib: Any) -> None:
        """Test get_symbols with pattern that finds no symbols.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)
        try:
            logger.debug("about to connect")
            algo_app.connect_to_ib("127.0.0.1",
                                   algo_app.PORT_FOR_LIVE_TRADING,
                                   client_id=0)
            verify_algo_app_connected(algo_app)
            full_match_descs = pd.DataFrame()
            stock_symbols_ds = pd.DataFrame()
            # we need to loop from A to Z
            for letter in string.ascii_uppercase:
                full_match_descs, stock_symbols_ds = \
                    verify_get_symbols(letter,
                                       algo_app,
                                       mock_ib,
                                       full_match_descs,
                                       stock_symbols_ds)

        finally:
            logger.debug('disconnecting')
            algo_app.disconnect_from_ib()
            logger.debug('verifying disconnected')
            verify_algo_app_disconnected(algo_app)
            logger.debug('disconnected - test case returning')

    def test_get_symbols_with_connect_disconnect(self,
                                                 algo_app: "AlgoApp",
                                                 mock_ib: Any) -> None:
        """Test get_symbols with pattern that finds no symbols.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        full_match_descs = pd.DataFrame()
        stock_symbols_ds = pd.DataFrame()
        # we need to loop from A to Z
        for letter in string.ascii_uppercase:
            try:
                logger.debug("about to connect")
                algo_app.connect_to_ib("127.0.0.1",
                                       algo_app.PORT_FOR_LIVE_TRADING,
                                       client_id=0)
                verify_algo_app_connected(algo_app)
                logger.debug("about to verify_get_symbols for letter %s",
                             letter)
                full_match_descs, stock_symbols_ds = \
                    verify_get_symbols(letter,
                                       algo_app,
                                       mock_ib,
                                       full_match_descs,
                                       stock_symbols_ds)

            finally:
                logger.debug('disconnecting')
                algo_app.disconnect_from_ib()
                logger.debug('verifying disconnected')
                verify_algo_app_disconnected(algo_app)


###############################################################################
# matching symbols verification
###############################################################################
def verify_match_symbols(algo_app: "AlgoApp",
                         mock_ib: Any,
                         pattern: str,
                         exp_recursive_matches: int,
                         exp_non_recursive_matches: int,
                         req_type: int = 1) -> None:
    """Verify that we find symbols correctly.

    Args:
        algo_app: instance of AlgoApp from conftest pytest fixture
        mock_ib: pytest fixture of contract_descriptions
        pattern: symbols to use for searching
        exp_recursive_matches: number of matches expected
        exp_non_recursive_matches: num expected for req_type 1
        req_type: indicates which request to do

    """
    try:
        logger.debug("about to connect")
        algo_app.connect_to_ib("127.0.0.1",
                               algo_app.PORT_FOR_LIVE_TRADING,
                               client_id=0)
        verify_algo_app_connected(algo_app)

        # make request for symbol that will be returned
        assert algo_app.request_id == 1
        assert req_type == 1 or req_type == 2
        if req_type == 1:
            logger.debug("about to request_symbols for %s", pattern)
            algo_app.request_symbols(pattern)
            assert algo_app.request_id == 2
        else:  # req_type == 2:
            logger.debug("about to get_symbols_recursive for %s", pattern)
            algo_app.get_symbols_recursive(pattern)
            assert algo_app.request_id >= 2
            algo_app.stock_symbols.drop_duplicates(inplace=True)

        logger.debug("getting match_descs")
        match_descs = mock_ib.contract_descriptions.loc[
            (mock_ib.contract_descriptions['symbol'].str.
             startswith(pattern))
            & (mock_ib.contract_descriptions['secType'] == 'STK')
            & (mock_ib.contract_descriptions['currency'] == 'USD')
            & (if_opt_in_derivative_types(mock_ib.contract_descriptions))
            ]

        logger.debug("verifying results counts")

        # diag_msg('len(algo_app.stock_symbols):', len(algo_app.stock_symbols))
        # diag_msg(algo_app.stock_symbols)
        # diag_msg('len(match_descs):', len(match_descs))
        # diag_msg(match_descs)
        if req_type == 1:
            assert len(algo_app.stock_symbols) == exp_non_recursive_matches
            assert len(match_descs) == exp_recursive_matches
        else:
            assert len(algo_app.stock_symbols) == exp_recursive_matches
            assert len(match_descs) == exp_recursive_matches

        logger.debug("verifying results match DataFrame")
        if exp_recursive_matches > 0:
            match_descs = match_descs.drop(columns=['secType',
                                                    'currency',
                                                    'derivative_types'])

            if req_type == 1:
                match_descs = match_descs.iloc[0:exp_non_recursive_matches]
            match_descs = match_descs.set_index(
                ['conId']).sort_index()
            # diag_msg('len(match_descs) 2:', len(match_descs))
            # diag_msg(match_descs)

            algo_app.stock_symbols.sort_index(inplace=True)
            comp_df = algo_app.stock_symbols.compare(match_descs)
            assert comp_df.empty

        logger.debug("all results verified for req_type %d", req_type)

    finally:
        logger.debug('disconnecting')
        algo_app.disconnect_from_ib()
        logger.debug('verifying disconnected')
        verify_algo_app_disconnected(algo_app)
        logger.debug('disconnected - test case returning')


def if_opt_in_derivative_types(df: Any) -> Any:
    """Find the symbols that have options.

    Args:
        df: pandas DataFrame of symbols

    Returns:
          array of boolean values used in pandas loc function

    """
    ret_array = np.full(len(df), False)
    for i in range(len(df)):
        if 'OPT' in df.iloc[i].derivative_types:
            ret_array[i] = True
    return ret_array


def get_exp_number(search_char: str, mock_ib: Any) -> Tuple[int, int]:
    """Helper function to get number of expected symbols.

    Args:
        search_char: single char that will be searched
        mock_ib: mock of ib

    Returns:
        number of expected matches for recursive and non-recursive requests
    """
    if search_char not in string.ascii_uppercase[8:17]:  # I to Q, inclusive
        return 0, 0
    count = 0
    combo = mock_ib.get_combos(search_char)

    for item in combo:
        if item[0] == 'STK' and item[2] == 'USD' and 'OPT' in item[3]:
            count += 1
    num_exp_recursive = count * (1 + 3 + 3**2 + 3**3)
    num_exp_non_recursive = \
        math.ceil(min(16, num_exp_recursive) * (count/len(combo)))

    return num_exp_recursive, num_exp_non_recursive


def verify_get_symbols(letter: str,
                       algo_app: "AlgoApp",
                       mock_ib: Any,
                       full_match_descs: Any,
                       stock_symbols_ds: Any) -> Tuple[Any, Any]:
    """Verify get_symbols.

    Args:
        letter: the single letter we are collecting symbols for
        algo_app: instance of AlgoApp from conftest pytest fixture
        mock_ib: pytest fixture of contract_descriptions
        full_match_descs: DataFrame of cumulative symbols
        stock_symbols_ds: DataFrame of last copy of stock_symbols

    Returns:
        updated full_match_descs and stock_symbols_ds DataFrames

    """
    if letter != 'A':
        # verify the symbol_status ds
        symbols_status_path = \
            algo_app.ds_catalog.get_path('symbols_status')
        logger.info('symbols_status_path: %s', symbols_status_path)

        assert symbols_status_path.exists()
        symbols_status = pd.read_csv(symbols_status_path,
                                     header=0,
                                     index_col=0)
        test_letter = symbols_status.iloc[0, 0]
        assert test_letter == letter

    num_exp_recursive, num_exp_non_recursive = \
        get_exp_number(letter, mock_ib)
    logger.debug("about to get_symbols for %s", letter)
    algo_app.get_symbols()
    assert algo_app.request_id >= 2

    logger.debug("getting match_descs for %s", letter)
    match_descs = mock_ib.contract_descriptions.loc[
        (mock_ib.contract_descriptions['symbol'].str.
         startswith(letter))
        & (mock_ib.contract_descriptions['secType'] == 'STK')
        & (mock_ib.contract_descriptions['currency'] == 'USD')
        & (if_opt_in_derivative_types(
            mock_ib.contract_descriptions))
        ]
    # we expect the stock_symbols to accumulate and grow, so the
    # number should now be what was there from the previous
    # iteration of this loop and what we just now added
    assert len(match_descs) == num_exp_recursive
    assert len(algo_app.stock_symbols) == num_exp_recursive \
           + len(stock_symbols_ds)

    if num_exp_recursive > 0:
        match_descs = \
            match_descs.drop(columns=['secType',
                                      'currency',
                                      'derivative_types'])
        match_descs = match_descs.set_index(
            ['conId']).sort_index()
        full_match_descs = full_match_descs.append(match_descs)
        full_match_descs.sort_index(inplace=True)

        # check the data set
        stock_symbols_path = \
            algo_app.ds_catalog.get_path('stock_symbols')
        logger.info('stock_symbols_path: %s', stock_symbols_path)

        stock_symbols_ds = pd.read_csv(stock_symbols_path,
                                       header=0,
                                       index_col=0)
        comp_df = algo_app.stock_symbols.compare(stock_symbols_ds)
        assert comp_df.empty

    return full_match_descs, stock_symbols_ds


###############################################################################
# error path
###############################################################################
class TestErrorPath:
    def test_error_path_by_request_when_not_connected(self,
                                                      algo_app: "AlgoApp",
                                                      capsys: Any) -> None:
        """Test the error callback by any request while not connected.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            capsys: pytest fixture to capture print output

        """
        verify_algo_app_initialized(algo_app)
        logger.debug('verifying disconnected')
        verify_algo_app_disconnected(algo_app)

        logger.debug("about to request time")
        algo_app.reqCurrentTime()
        captured = capsys.readouterr().out
        assert captured == 'Error:  -1   504   Not connected' + '\n'


###############################################################################
# contract details
###############################################################################
class TestAlgoAppContractDetails:
    """TestAlgoAppContractDetails class."""

    def test_get_contract_details_0_entries(self,
                                            algo_app: "AlgoApp",
                                            mock_ib: Any
                                            ) -> None:
        """Test contract details for non-existent conId.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        logger.debug("about to connect")
        algo_app.connect_to_ib("127.0.0.1",
                               algo_app.PORT_FOR_LIVE_TRADING,
                               client_id=0)

        # verify that algo_app is connected and alive with a valid reqId
        verify_algo_app_connected(algo_app)

        contract = Contract()  # create an empty contract with conId of 0
        algo_app.get_contract_details(contract)

        verify_contract_details(contract, algo_app, mock_ib, [])

        algo_app.disconnect_from_ib()
        verify_algo_app_disconnected(algo_app)

    def test_get_contract_details_1_entry(self,
                                          algo_app: "AlgoApp",
                                          mock_ib: Any
                                          ) -> None:
        """Test contract details for 1 entry.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        logger.debug("about to connect")
        algo_app.connect_to_ib("127.0.0.1",
                               algo_app.PORT_FOR_LIVE_TRADING,
                               client_id=0)

        # verify that algo_app is connected and alive with a valid reqId
        verify_algo_app_connected(algo_app)

        contract = Contract()  # create an empty contract with conId of 0
        contract.conId = 7001
        algo_app.get_contract_details(contract)

        verify_contract_details(contract, algo_app, mock_ib, [7001])

        algo_app.disconnect_from_ib()
        verify_algo_app_disconnected(algo_app)

    def test_get_contract_details_2_entries(self,
                                            algo_app: "AlgoApp",
                                            mock_ib: Any
                                            ) -> None:
        """Test contract details for 2 entries.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        logger.debug("about to connect")
        algo_app.connect_to_ib("127.0.0.1",
                               algo_app.PORT_FOR_LIVE_TRADING,
                               client_id=0)

        # verify that algo_app is connected and alive with a valid reqId
        verify_algo_app_connected(algo_app)

        contract = Contract()  # create an empty contract with conId of 0
        contract.conId = 7001
        algo_app.get_contract_details(contract)

        verify_contract_details(contract, algo_app, mock_ib, [7001])

        contract.conId = 7002
        algo_app.get_contract_details(contract)

        verify_contract_details(contract, algo_app, mock_ib, [7001, 7002])

        algo_app.disconnect_from_ib()
        verify_algo_app_disconnected(algo_app)


###############################################################################
# contract details verification
###############################################################################
def verify_contract_details(contract: "Contract",
                            algo_app: "AlgoApp",
                            mock_ib: Any,
                            conId_list: List[int]) -> None:
    """Verify contract details.

    Args:
        contract: the contract used to get details
        algo_app: instance of AlgoApp from conftest pytest fixture
        mock_ib: pytest fixture of contract_descriptions
        num_expected: number of contracts expected to have been returned

    """

    # diag_msg('mock_ib.contract_descriptions.info:\n',
    #          mock_ib.contract_descriptions.info())
    #
    # diag_msg('match_descs.info:\n',
    #          match_descs.info())
    #
    # diag_msg('algo_app.contract_details.info:\n',
    #          algo_app.contract_details.info())

    # assert len(match_descs) == num_expected
    assert len(algo_app.contract_details) == len(conId_list)

    if len(conId_list) > 0:

        # check the data set
        contract_details_path = \
            algo_app.ds_catalog.get_path('contract_details')
        logger.info('stock_symbols_path: %s', contract_details_path)

        contract_details_ds = pd.read_csv(contract_details_path,
                                          header=0,
                                          index_col=0)
        for conId in conId_list:
            match_desc = mock_ib.contract_descriptions.loc[
                mock_ib.contract_descriptions['conId'] == conId]
            diag_msg('match_desc\n', match_desc)
            diag_msg('match_desc.conId[0]\n', match_desc.conId[0])
            diag_msg('match_desc.symbol[0]\n', match_desc.symbol[0])

            test_contract_details = \
                algo_app.contract_details.loc[conId]['contractDetails']

            diag_msg('test_contract_details\n', test_contract_details)
            diag_msg('test_contract_details.contract.conId:\n',
                     test_contract_details.contract.conId)
            diag_msg('test_contract_details.contract.symbol:\n',
                     test_contract_details.contract.symbol)
            assert test_contract_details.contract.conId == conId
            assert (test_contract_details.contract.symbol
                    == match_desc.symbol[0])
            assert (test_contract_details.contract.secType
                    == match_desc.secType[0])
            assert (test_contract_details.contract.lastTradeDateOrContractMonth
                    == '01012022')
            assert (test_contract_details.contract.strike == 0.0)
            assert (test_contract_details.contract.right == "P")
            assert (test_contract_details.contract.multiplier == "2")
            assert (test_contract_details.contract.exchange == 'SMART')
            assert (test_contract_details.contract.primaryExchange
                    == match_desc.primaryExchange[0])
            assert (test_contract_details.contract.currency
                    == match_desc.currency[0])
            assert (test_contract_details.contract.localSymbol
                    == match_desc.symbol[0])
            assert (test_contract_details.contract.tradingClass
                    == 'TradingClass' + str(conId))
            assert (test_contract_details.contract.includeExpired is False)
            assert (test_contract_details.contract.secIdType == "")
            assert (test_contract_details.contract.secId == "")

            # combos
            assert (test_contract_details.contract.comboLegsDescrip == "")
            assert (test_contract_details.contract.comboLegs is None)
            assert (test_contract_details.contract.deltaNeutralContract
                    is None)





###############################################################################
# fundamental data
###############################################################################
class TestAlgoAppFundamentalData:
    """TestAlgoAppContractDetails class."""

    def test_get_contract_details_0_entries(self,
                                            algo_app: "AlgoApp",
                                            mock_ib: Any
                                            ) -> None:
        """Test contract details for non-existent conId.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        logger.debug("about to connect")
        algo_app.connect_to_ib("127.0.0.1",
                               algo_app.PORT_FOR_LIVE_TRADING,
                               client_id=0)

        # verify that algo_app is connected and alive with a valid reqId
        verify_algo_app_connected(algo_app)

        contract = Contract()  # create an empty contract with conId of 0
        algo_app.get_contract_details(contract)

        verify_contract_details(contract, algo_app, mock_ib, 0)

        algo_app.disconnect_from_ib()
        verify_algo_app_disconnected(algo_app)