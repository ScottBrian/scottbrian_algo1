"""test_algo_api.py module."""

# from datetime import datetime, timedelta
import pytest
# import sys
# from pathlib import Path
import numpy as np
import pandas as pd  # type: ignore
import string
import math
import pickle


from typing import Any, List, NamedTuple, Tuple  # Callable, cast, Tuple, Union
# from typing_extensions import Final

from ibapi.contract import Contract, ContractDetails

from scottbrian_algo1.algo_api import AlgoApp, AlreadyConnected, \
    DisconnectLockHeld, ConnectTimeout, RequestTimeout, DisconnectDuringRequest

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
    assert algo_app.symbols.empty
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
###############################################################################
# matching symbols
###############################################################################
###############################################################################
class ExpCounts(NamedTuple):
    """NamedTuple for the expected counts."""
    stock_non_recursive: int
    stock_recursive: int
    other_non_recursive: int
    other_recursive: int


class TestAlgoAppMatchingSymbols:
    """TestAlgoAppMatchingSymbols class."""
    def test_request_symbols_all_combos(self,
                                        algo_app: "AlgoApp",
                                        mock_ib: Any) -> None:
        """Test request_symbols with all patterns.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        logger.debug("about to connect")
        algo_app.connect_to_ib("127.0.0.1",
                               algo_app.PORT_FOR_LIVE_TRADING,
                               client_id=0)
        verify_algo_app_connected(algo_app)
        algo_app.request_throttle_secs = 0.01

        try:
            for idx, search_pattern in enumerate(
                    mock_ib.search_patterns()):
                exp_counts = get_exp_number(search_pattern, mock_ib)
                # verify symbol table has zero entries for the symbol
                logger.info("calling verify_match_symbols req_type 1 "
                            "sym %s num %d", search_pattern, idx)
                algo_app.symbols = pd.DataFrame()
                algo_app.stock_symbols = pd.DataFrame()
                verify_match_symbols(algo_app,
                                     mock_ib,
                                     search_pattern,
                                     exp_counts=exp_counts,
                                     req_type=1)

                logger.info("calling verify_match_symbols req_type 2 "
                            "sym %s num %d", search_pattern, idx)
                algo_app.symbols = pd.DataFrame()
                algo_app.stock_symbols = pd.DataFrame()
                verify_match_symbols(algo_app,
                                     mock_ib,
                                     search_pattern,
                                     exp_counts=exp_counts,
                                     req_type=2)
        finally:
            logger.debug('disconnecting')
            algo_app.disconnect_from_ib()
            logger.debug('verifying disconnected')
            verify_algo_app_disconnected(algo_app)
            logger.debug('disconnected - test case returning')

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
        exp_counts = ExpCounts(0, 0, 0, 0)

        # verify symbol table has zero entries for the symbol
        verify_match_symbols(algo_app,
                             mock_ib,
                             nonexistent_symbol_arg,
                             exp_counts=exp_counts,
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
        exp_counts = ExpCounts(0, 0, 0, 0)
        # verify symbol table has zero entries for the symbol
        logger.debug("calling verify_match_symbols req_type 1")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_0_arg,
                             exp_counts=exp_counts,
                             req_type=1)

        logger.debug("calling verify_match_symbols req_type 2")
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_0_arg,
                             exp_counts=exp_counts,
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
        exp_counts = get_exp_number(get_symbols_search_char_arg, mock_ib)

        # verify symbol table has correct entries for the symbol
        logger.debug("calling verify_match_symbols req_type 1")
        verify_match_symbols(algo_app,
                             mock_ib,
                             get_symbols_search_char_arg,
                             exp_counts=exp_counts,
                             req_type=1)

        logger.debug("calling verify_match_symbols req_type 2")
        verify_match_symbols(algo_app,
                             mock_ib,
                             get_symbols_search_char_arg,
                             exp_counts=exp_counts,
                             req_type=2)

    def test_get_symbols_timeout(self,
                                 algo_app: "AlgoApp",
                                 mock_ib: Any) -> None:
        """Test get_symbols gets timeout.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)
        try:
            logger.debug("about to connect")
            algo_app.connect_to_ib("127.0.0.1",
                                   mock_ib.PORT_FOR_SIMULATE_REQUEST_TIMEOUT,
                                   client_id=0)
            verify_algo_app_connected(algo_app)

            with pytest.raises(RequestTimeout):
                algo_app.request_symbols('A')

        finally:
            logger.debug('disconnecting')
            algo_app.disconnect_from_ib()
            logger.debug('verifying disconnected')
            verify_algo_app_disconnected(algo_app)
            logger.debug('disconnected - test case returning')

    def test_get_symbols_disconnect(self,
                                 algo_app: "AlgoApp",
                                 mock_ib: Any) -> None:
        """Test get_symbols gets disconnected while waiting.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)
        try:
            logger.debug("about to connect")
            algo_app.connect_to_ib("127.0.0.1",
                                   mock_ib.
                                   PORT_FOR_SIMULATE_REQUEST_DISCONNECT,
                                   client_id=0)
            verify_algo_app_connected(algo_app)

            with pytest.raises(DisconnectDuringRequest):
                algo_app.request_symbols('A')

        finally:
            logger.debug('disconnecting')
            algo_app.disconnect_from_ib()
            logger.debug('verifying disconnected')
            verify_algo_app_disconnected(algo_app)
            logger.debug('disconnected - test case returning')

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
                         exp_counts: ExpCounts,
                         req_type: int = 1) -> None:
    """Verify that we find symbols correctly.

    Args:
        algo_app: instance of AlgoApp from conftest pytest fixture
        mock_ib: pytest fixture of contract_descriptions
        pattern: symbols to use for searching
        exp_counts: recursive and non-recursive matches expected
        req_type: indicates which request to do

    """
    assert req_type == 1 or req_type == 2
    if req_type == 1:
        logger.debug("about to request_symbols for %s", pattern)
        algo_app.request_symbols(pattern)
        # assert algo_app.request_id == 2
    else:  # req_type == 2:
        logger.debug("about to get_symbols_recursive for %s", pattern)
        algo_app.get_symbols_recursive(pattern)
        assert algo_app.request_id >= 2
        # algo_app.stock_symbols.drop_duplicates(inplace=True)

    logger.debug("getting match_descs")
    match_descs = mock_ib.contract_descriptions.loc[
        (mock_ib.contract_descriptions['symbol'].str.
         startswith(pattern))
        & (mock_ib.contract_descriptions['secType'] == 'STK')
        & (mock_ib.contract_descriptions['currency'] == 'USD')
        & (if_opt_in_derivativeSecTypes(mock_ib.contract_descriptions))
        ]

    other_match_descs = mock_ib.contract_descriptions.loc[
        (mock_ib.contract_descriptions['symbol'].str.
         startswith(pattern))
        & ((mock_ib.contract_descriptions['secType'] != 'STK')
            | (mock_ib.contract_descriptions['currency'] != 'USD')
            | (if_opt_in_derivativeSecTypes(mock_ib.contract_descriptions)
               is False))
        ]

    logger.debug("verifying results counts")

    diag_msg('len(algo_app.symbols):', len(algo_app.symbols))
    diag_msg(algo_app.symbols)
    diag_msg('len(other_match_descs):', len(other_match_descs))
    diag_msg(other_match_descs)
    if req_type == 1:
        assert len(algo_app.stock_symbols) == exp_counts.stock_non_recursive
        assert len(algo_app.symbols) == exp_counts.other_non_recursive
        assert len(match_descs) == exp_counts.stock_recursive
        assert len(other_match_descs) == exp_counts.other_recursive
    else:
        assert len(algo_app.stock_symbols) == exp_counts.stock_recursive
        assert len(algo_app.symbols) == exp_counts.other_recursive
        assert len(match_descs) == exp_counts.stock_recursive
        assert len(other_match_descs) == exp_counts.other_recursive

    logger.debug("verifying results match DataFrame")
    if exp_counts.stock_recursive > 0:
        # match_descs = match_descs.drop(columns=['derivativeSecTypes'])

        if req_type == 1:
            match_descs = match_descs.iloc[0:exp_counts.stock_non_recursive]
        match_descs = match_descs.set_index(
            ['conId']).sort_index()
        # diag_msg('len(match_descs) 2:', len(match_descs))
        # diag_msg(match_descs)

        algo_app.stock_symbols.sort_index(inplace=True)
        comp_df = algo_app.stock_symbols.compare(match_descs)
        assert comp_df.empty

    if exp_counts.other_recursive > 0:
        # match_descs = match_descs.drop(columns=['derivativeSecTypes'])

        if req_type == 1:
            other_match_descs = other_match_descs.iloc[
                                0:exp_counts.other_non_recursive]
        other_match_descs = other_match_descs.set_index(
            ['conId']).sort_index()
        # diag_msg('len(match_descs) 2:', len(match_descs))
        # diag_msg(match_descs)

        algo_app.symbols.sort_index(inplace=True)
        comp_df = algo_app.symbols.compare(other_match_descs)
        assert comp_df.empty
    logger.debug("all results verified for req_type %d", req_type)


def if_opt_in_derivativeSecTypes(df: Any) -> Any:
    """Find the symbols that have options.

    Args:
        df: pandas DataFrame of symbols

    Returns:
          array of boolean values used in pandas loc function

    """
    ret_array = np.full(len(df), False)
    for i in range(len(df)):
        if 'OPT' in df.iloc[i].derivativeSecTypes:
            ret_array[i] = True
    return ret_array


def get_exp_number(search_pattern: str, mock_ib: Any) -> ExpCounts:
    """Helper function to get number of expected symbols.

    Args:
        search_pattern: search arg as string of one or more chars
        mock_ib: mock of ib

    Returns:
        number of expected matches for recursive and non-recursive requests
    """
    combo_factor = (1 + 3 + 3**2 + 3**3)
    if len(search_pattern) > 4:
        # 5 or more chars will never match (for our mock setup)
        return ExpCounts(0, 0, 0, 0)
    if search_pattern[0] not in string.ascii_uppercase[0:17]:
        return ExpCounts(0, 0, 0, 0)  # not in A-Q, inclusive
    if len(search_pattern) >= 2:
        if search_pattern[1] not in string.ascii_uppercase[1:3] + '.':
            return ExpCounts(0, 0, 0, 0)  # not in 'BC.'
        combo_factor = (1 + 3 + 3**2)
    if len(search_pattern) >= 3:
        if search_pattern[2] not in string.ascii_uppercase[2:5]:
            return ExpCounts(0, 0, 0, 0)  # not in 'CDE'
        combo_factor = (1 + 3)
    if len(search_pattern) == 4:
        if search_pattern[3] not in string.ascii_uppercase[3:5] + '.':
            return ExpCounts(0, 0, 0, 0)  # not in 'DE.'
        combo_factor = 1

    count = 0
    other_count = 0
    combo = mock_ib.get_combos(search_pattern[0])

    raw_count = len(combo)

    for item in combo:
        if item[0] == 'STK' and item[2] == 'USD' and 'OPT' in item[3]:
            count += 1
        else:
            other_count += 1
    num_exp_recursive = count * combo_factor
    num_other_exp_recursive = other_count * combo_factor
    num_exp_non_recursive = \
        math.ceil(min(16, len(combo) * combo_factor) * (count / len(combo)))
    num_other_exp_non_recursive = \
        math.ceil(min(16, len(combo) * combo_factor) * (other_count
                                                        / len(combo)))

    return ExpCounts(num_exp_non_recursive,
                     num_exp_recursive,
                     num_other_exp_non_recursive,
                     num_other_exp_recursive)


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

    exp_counts = get_exp_number(letter, mock_ib)
    logger.debug("about to get_symbols for %s", letter)
    algo_app.get_symbols()
    assert algo_app.request_id >= 2

    logger.debug("getting match_descs for %s", letter)
    match_descs = mock_ib.contract_descriptions.loc[
        (mock_ib.contract_descriptions['symbol'].str.
         startswith(letter))
        & (mock_ib.contract_descriptions['secType'] == 'STK')
        & (mock_ib.contract_descriptions['currency'] == 'USD')
        & (if_opt_in_derivativeSecTypes(
            mock_ib.contract_descriptions))
        ]
    # we expect the stock_symbols to accumulate and grow, so the
    # number should now be what was there from the previous
    # iteration of this loop and what we just now added
    assert len(match_descs) == exp_counts.stock_recursive
    assert len(algo_app.stock_symbols) == exp_counts.stock_recursive \
           + len(stock_symbols_ds)

    if exp_counts.stock_recursive > 0:
        # match_descs = \
        #     match_descs.drop(columns=['secType',
        #                               'currency',
        #                               'derivativeSecTypes'])
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

    def test_get_contract_details_duplicates(self,
                                             algo_app: "AlgoApp",
                                             mock_ib: Any
                                             ) -> None:
        """Test contract details for 3 entries plus a duplicate.

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

        contract.conId = 7001  # try to add 7001 again
        algo_app.get_contract_details(contract)

        verify_contract_details(contract, algo_app, mock_ib, [7001, 7002])

        contract.conId = 7003
        algo_app.get_contract_details(contract)

        verify_contract_details(contract, algo_app, mock_ib, [7001, 7002, 7003])

        contract.conId = 7002  # another duplicate
        algo_app.get_contract_details(contract)

        verify_contract_details(contract, algo_app, mock_ib, [7001, 7002, 7003])

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

    # diag_msg('algo_app.contract_details:\n',
    #          algo_app.contract_details)
    #
    # diag_msg('algo_app.contract_details.iloc[0]:\n',
    #          algo_app.contract_details.iloc[0])
    #
    # diag_msg('algo_app.contract_details.iloc[1]:\n',
    #          algo_app.contract_details.iloc[1])
    # assert len(match_descs) == num_expected
    assert len(algo_app.contract_details) == len(conId_list)

    if len(conId_list) > 0:

        # check the data set
        contract_details_path = \
            algo_app.ds_catalog.get_path('contract_details')
        logger.info('stock_symbols_path: %s', contract_details_path)

        # contract_details_ds = pd.read_csv(contract_details_path,
        #                                   header=0,
        #                                   index_col=0)
        with open(contract_details_path, 'rb') as f:
            contract_details_ds = pickle.load(f)

        print('contract_details_ds:\n', contract_details_ds)
        print('contract_details_ds.__dict__:\n',
              contract_details_ds.__dict__)
        for conId in conId_list:
            match_desc = mock_ib.contract_descriptions.loc[
                mock_ib.contract_descriptions['conId'] == conId]
            # diag_msg('match_desc\n', match_desc)
            # diag_msg('match_desc.conId[0]\n', match_desc.conId[0])
            # diag_msg('match_desc.symbol[0]\n', match_desc.symbol[0])

            test_contract_details = \
                algo_app.contract_details.loc[conId]['contractDetails']

            test_contract_details2 = \
                contract_details_ds.loc[conId]['contractDetails']
            print('test_contract_details2:\n', test_contract_details2)
            print('test_contract_details2.__dict__:\n',
                  test_contract_details2.__dict__)
            # diag_msg('test_contract_details\n', test_contract_details)
            # diag_msg('test_contract_details.contract.conId:\n',
            #          test_contract_details.contract.conId)
            # diag_msg('test_contract_details.contract.symbol:\n',
            #          test_contract_details.contract.symbol)
            #
            # diag_msg('test_contract_details2\n', test_contract_details2)
            # diag_msg('test_contract_details2.contract.conId:\n',
            #          test_contract_details2.contract.conId)
            # diag_msg('test_contract_details2.contract.symbol:\n',
            #          test_contract_details2.contract.symbol)

            assert (test_contract_details.contract.conId
                    == test_contract_details2.contract.conId
                    == conId)

            assert (test_contract_details.contract.symbol
                    == test_contract_details2.contract.symbol
                    == match_desc.symbol[0])

            assert (test_contract_details.contract.secType
                    == test_contract_details2.contract.secType
                    == match_desc.secType[0])

            assert (test_contract_details.contract.lastTradeDateOrContractMonth
                    == test_contract_details2.contract
                        .lastTradeDateOrContractMonth
                    == '01012022')

            assert (test_contract_details.contract.strike
                    == test_contract_details2.contract.strike
                    == 0.0)

            assert (test_contract_details.contract.right
                    == test_contract_details2.contract.right
                    == "P")

            assert (test_contract_details.contract.multiplier
                    == test_contract_details2.contract.multiplier
                    == "2")

            assert (test_contract_details.contract.exchange
                    == test_contract_details2.contract.exchange
                    == 'SMART')

            assert (test_contract_details.contract.primaryExchange
                    == test_contract_details2.contract.primaryExchange
                    == match_desc.primaryExchange[0])

            assert (test_contract_details.contract.currency
                    == test_contract_details2.contract.currency
                    == match_desc.currency[0])

            assert (test_contract_details.contract.localSymbol
                    == test_contract_details2.contract.localSymbol
                    == match_desc.symbol[0])

            assert (test_contract_details.contract.tradingClass
                    == test_contract_details2.contract.tradingClass
                    == 'TradingClass' + str(conId))

            assert (test_contract_details.contract.includeExpired
                    == test_contract_details2.contract.includeExpired
                    is False)

            assert (test_contract_details.contract.secIdType
                    == test_contract_details2.contract.secIdType
                    == "")

            assert (test_contract_details.contract.secId
                    == test_contract_details2.contract.secId
                    == "")

            # combos
            assert (test_contract_details.contract.comboLegsDescrip
                    == test_contract_details2.contract.comboLegsDescrip
                    == "")

            assert (test_contract_details.contract.comboLegs
                    == test_contract_details2.contract.comboLegs
                    is None)

            assert (test_contract_details.contract.deltaNeutralContract
                    == test_contract_details2.contract.deltaNeutralContract
                    is None)

            ############################################################
            # details
            ############################################################
            assert (test_contract_details.marketName
                    == test_contract_details2.marketName
                    == 'MarketName' + str(conId))

            assert (test_contract_details.minTick
                    == test_contract_details2.minTick
                    == 0.01)

            assert (test_contract_details.orderTypes
                    == test_contract_details2.orderTypes
                    == 'OrderTypes' + str(conId))

            assert (len(test_contract_details.secIdList)
                    == len(test_contract_details2.secIdList)
                    == 5)
            for i in range(len(test_contract_details.secIdList)):
                assert (test_contract_details.secIdList[i].tag
                        == test_contract_details2.secIdList[i].tag
                        == 'tag' + str(i))

                assert (test_contract_details.secIdList[i].value
                        == test_contract_details2.secIdList[i].value
                        == 'value' + str(i))


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