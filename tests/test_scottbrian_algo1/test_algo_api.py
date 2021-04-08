"""test_algo_api.py module."""

# from datetime import datetime, timedelta
import pytest
# import sys
# from pathlib import Path
import numpy as np
import pandas as pd  # type: ignore
import string
import math

from typing import Any, List, NamedTuple
# from typing_extensions import Final

from ibapi.tag_value import TagValue  # type: ignore
from ibapi.contract import ComboLeg  # type: ignore
from ibapi.contract import DeltaNeutralContract
from ibapi.contract import Contract, ContractDetails

from scottbrian_algo1.algo_api import AlgoApp, AlreadyConnected, \
    DisconnectLockHeld, ConnectTimeout, RequestTimeout, DisconnectDuringRequest

from scottbrian_algo1.algo_maps import get_contract_dict, get_contract_obj
from scottbrian_algo1.algo_maps import get_contract_details_obj

# from scottbrian_utils.diag_msg import diag_msg
# from scottbrian_utils.file_catalog import FileCatalog

import logging

logger = logging.getLogger(__name__)


###############################################################################
# TestAlgoAppConnect class
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
    sym_non_recursive: int
    sym_recursive: int
    stock_sym_non_recursive: int
    stock_sym_recursive: int


class SymDfs:
    """Saved sym dfs."""
    def __init__(self,
                 mock_sym_df: Any,
                 sym_df: Any,
                 mock_stock_sym_df: Any,
                 stock_sym_df: Any) -> None:
        """Initialize the SymDfs.

        Args:
            mock_sym_df: mock sym DataFrame
            sym_df: symbol DataFrame
            mock_stock_sym_df: mock stock symbol DataFrame
            stock_sym_df: stock symbols dataFrame

        """
        self.mock_sym_df = mock_sym_df
        self.sym_df = sym_df
        self.mock_stock_sym_df = mock_stock_sym_df
        self.stock_sym_df = stock_sym_df


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

    def test_request_symbols_zero_result(self,
                                         algo_app: "AlgoApp",
                                         mock_ib: Any
                                         ) -> None:
        """Test request_symbols with pattern that finds exactly 1 symbol.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
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
            exp_counts = ExpCounts(0, 0, 0, 0)

            # verify symbol table has zero entries for the symbols
            for idx, search_pattern in enumerate(
                    mock_ib.no_find_search_patterns()):
                logger.info("calling verify_match_symbols req_type 1 "
                            "sym %s num %d", search_pattern, idx)
                verify_match_symbols(algo_app,
                                     mock_ib,
                                     search_pattern,
                                     exp_counts=exp_counts,
                                     req_type=1)

                logger.info("calling verify_match_symbols req_type 2 "
                            "sym %s num %d", search_pattern, idx)
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
            algo_app.request_throttle_secs = 0.01

            sym_dfs = SymDfs(pd.DataFrame(),
                             pd.DataFrame(),
                             pd.DataFrame(),
                             pd.DataFrame())
            # full_stock_sym_match_descs = pd.DataFrame()
            # stock_symbols_ds = pd.DataFrame()
            # full_sym_match_descs = pd.DataFrame()
            # symbols_ds = pd.DataFrame()
            # we need to loop from A to Z
            for letter in string.ascii_uppercase:
                logger.debug("about to verify_get_symbols for letter %s",
                             letter)
                # full_stock_sym_match_descs, stock_symbols_ds,\
                #     full_sym_match_descs, symbols_ds = \
                sym_dfs = verify_get_symbols(letter,
                                             algo_app,
                                             mock_ib,
                                             sym_dfs)

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

        sym_dfs = SymDfs(pd.DataFrame(),
                         pd.DataFrame(),
                         pd.DataFrame(),
                         pd.DataFrame())
        # full_stock_sym_match_descs = pd.DataFrame()
        # full_sym_match_descs = pd.DataFrame()
        # stock_symbols_ds = pd.DataFrame()
        # symbols_ds = pd.DataFrame()
        # we need to loop from A to Z
        for letter in string.ascii_uppercase:
            try:
                logger.debug("about to connect")
                algo_app.connect_to_ib("127.0.0.1",
                                       algo_app.PORT_FOR_LIVE_TRADING,
                                       client_id=0)
                verify_algo_app_connected(algo_app)
                algo_app.request_throttle_secs = 0.01

                logger.debug("about to verify_get_symbols for letter %s",
                             letter)
                # full_stock_sym_match_descs, stock_symbols_ds, \
                #     full_sym_match_descs, symbols_ds = \
                sym_dfs = verify_get_symbols(letter,
                                             algo_app,
                                             mock_ib,
                                             sym_dfs)

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

    logger.debug("getting stock_sym_match_descs")
    stock_sym_match_descs = mock_ib.contract_descriptions.loc[
        (mock_ib.contract_descriptions['symbol'].str.
         startswith(pattern))
        & (mock_ib.contract_descriptions['secType'] == 'STK')
        & (mock_ib.contract_descriptions['currency'] == 'USD')
        & (if_opt_in_derivativeSecTypes(mock_ib.contract_descriptions)),
        ['conId', 'symbol', 'secType', 'primaryExchange', 'currency',
         'derivativeSecTypes']
        ]

    sym_match_descs = mock_ib.contract_descriptions.loc[
        (mock_ib.contract_descriptions['symbol'].str.
         startswith(pattern))
        & ((mock_ib.contract_descriptions['secType'] != 'STK')
            | (mock_ib.contract_descriptions['currency'] != 'USD')
            | if_opt_not_in_derivativeSecTypes(mock_ib.contract_descriptions)
           ),
        ['conId', 'symbol', 'secType', 'primaryExchange', 'currency',
         'derivativeSecTypes']
        ]

    logger.debug("verifying results counts")

    if req_type == 1:
        assert len(algo_app.stock_symbols) \
               == exp_counts.stock_sym_non_recursive
        assert len(algo_app.symbols) == exp_counts.sym_non_recursive
        assert len(stock_sym_match_descs) == exp_counts.stock_sym_recursive
        assert len(sym_match_descs) == exp_counts.sym_recursive
    else:
        assert len(algo_app.stock_symbols) == exp_counts.stock_sym_recursive
        assert len(algo_app.symbols) == exp_counts.sym_recursive
        assert len(stock_sym_match_descs) == exp_counts.stock_sym_recursive
        assert len(sym_match_descs) == exp_counts.sym_recursive

    logger.debug("verifying results match DataFrame")
    if exp_counts.stock_sym_recursive > 0:
        if req_type == 1:
            stock_sym_match_descs = stock_sym_match_descs.iloc[
                          0:exp_counts.stock_sym_non_recursive]
        stock_sym_match_descs = stock_sym_match_descs.set_index(
            ['conId']).sort_index()

        algo_app.stock_symbols.sort_index(inplace=True)
        comp_df = algo_app.stock_symbols.compare(stock_sym_match_descs)
        assert comp_df.empty

    if exp_counts.sym_recursive > 0:
        if req_type == 1:
            sym_match_descs = sym_match_descs.iloc[
                                0:exp_counts.sym_non_recursive]
        sym_match_descs = sym_match_descs.set_index(
            ['conId']).sort_index()

        algo_app.symbols.sort_index(inplace=True)
        comp_df = algo_app.symbols.compare(sym_match_descs)
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


def if_opt_not_in_derivativeSecTypes(df: Any) -> Any:
    """Find the symbols that do not have options.

    Args:
        df: pandas DataFrame of symbols

    Returns:
          array of boolean values used in pandas loc function

    """
    ret_array = np.full(len(df), True)
    for i in range(len(df)):
        if 'OPT' in df.iloc[i].derivativeSecTypes:
            ret_array[i] = False
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

    num_stock_sym_combos = 0
    num_sym_combos = 0
    combo = mock_ib.get_combos(search_pattern[0])

    for item in combo:
        if item[0] == 'STK' and item[2] == 'USD' and 'OPT' in item[3]:
            num_stock_sym_combos += 1
        else:
            num_sym_combos += 1
    exp_stock_sym_recursive = num_stock_sym_combos * combo_factor
    exp_sym_recursive = num_sym_combos * combo_factor
    exp_stock_sym_non_recursive = \
        math.ceil(min(16, len(combo) * combo_factor)
                  * (num_stock_sym_combos / len(combo)))
    exp_sym_non_recursive = \
        math.floor(min(16, len(combo) * combo_factor)
                   * (num_sym_combos / len(combo)))

    return ExpCounts(exp_sym_non_recursive,
                     exp_sym_recursive,
                     exp_stock_sym_non_recursive,
                     exp_stock_sym_recursive
                     )


def verify_get_symbols(letter: str,
                       algo_app: "AlgoApp",
                       mock_ib: Any,
                       sym_dfs: SymDfs) -> SymDfs:
    """Verify get_symbols.

    Args:
        letter: the single letter we are collecting symbols for
        algo_app: instance of AlgoApp from conftest pytest fixture
        mock_ib: pytest fixture of contract_descriptions
        sym_dfs: saved DataFrames between calls

    Returns:
        updated sym_dfs

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

    logger.debug("getting stock_sym_match_descs for %s", letter)
    stock_sym_match_descs = mock_ib.contract_descriptions.loc[
        (mock_ib.contract_descriptions['symbol'].str.startswith(letter))
        & (mock_ib.contract_descriptions['secType'] == 'STK')
        & (mock_ib.contract_descriptions['currency'] == 'USD')
        & (if_opt_in_derivativeSecTypes(
            mock_ib.contract_descriptions)),
        ['conId', 'symbol', 'secType', 'primaryExchange', 'currency',
         'derivativeSecTypes']
        ]

    sym_match_descs = mock_ib.contract_descriptions.loc[
        (mock_ib.contract_descriptions['symbol'].str.startswith(letter))
        & ((mock_ib.contract_descriptions['secType'] != 'STK')
           | (mock_ib.contract_descriptions['currency'] != 'USD')
           | if_opt_not_in_derivativeSecTypes(mock_ib.contract_descriptions)
           ),
        ['conId', 'symbol', 'secType', 'primaryExchange', 'currency',
         'derivativeSecTypes']
        ]
    # we expect the stock_symbols to accumulate and grow, so the
    # number should now be what was there from the previous
    # iteration of this loop plus what we just now added
    assert len(stock_sym_match_descs) == exp_counts.stock_sym_recursive
    assert len(algo_app.stock_symbols) == (
            exp_counts.stock_sym_recursive + len(sym_dfs.stock_sym_df))

    assert len(sym_match_descs) == exp_counts.sym_recursive
    assert len(algo_app.symbols) == (
            exp_counts.sym_recursive + len(sym_dfs.sym_df))

    if exp_counts.stock_sym_recursive > 0:
        stock_sym_match_descs = stock_sym_match_descs.set_index(
            ['conId']).sort_index()
        sym_dfs.mock_stock_sym_df \
            = sym_dfs.mock_stock_sym_df.append(stock_sym_match_descs)
        sym_dfs.mock_stock_sym_df.sort_index(inplace=True)

        # check the data set
        stock_symbols_path = algo_app.ds_catalog.get_path('stock_symbols')
        logger.info('stock_symbols_path: %s', stock_symbols_path)

        sym_dfs.stock_sym_df = pd.read_csv(stock_symbols_path,
                                           header=0,
                                           index_col=0,
                                           converters={
                                               'derivativeSecTypes':
                                                   lambda x: eval(x)})
        comp_df = algo_app.stock_symbols.compare(sym_dfs.stock_sym_df)
        assert comp_df.empty

        comp_df = algo_app.stock_symbols.compare(sym_dfs.mock_stock_sym_df)
        assert comp_df.empty

    if exp_counts.sym_recursive > 0:
        sym_match_descs = sym_match_descs.set_index(
            ['conId']).sort_index()
        sym_dfs.mock_sym_df = \
            sym_dfs.mock_sym_df.append(sym_match_descs)
        sym_dfs.mock_sym_df.sort_index(inplace=True)

        # check the data set
        symbols_path = \
            algo_app.ds_catalog.get_path('symbols')
        logger.info('symbols_path: %s', symbols_path)

        sym_dfs.sym_df = pd.read_csv(symbols_path,
                                     header=0,
                                     index_col=0,
                                     converters={
                                         'derivativeSecTypes':
                                             lambda x: eval(x)})

        comp_df = algo_app.symbols.compare(sym_dfs.sym_df)
        assert comp_df.empty

        comp_df = algo_app.symbols.compare(sym_dfs.mock_sym_df)
        assert comp_df.empty

    return sym_dfs


###############################################################################
###############################################################################
# error path
###############################################################################
###############################################################################
class TestErrorPath:
    """Class to test error path."""
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
###############################################################################
# contract details
###############################################################################
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

        verify_contract_details(contract, algo_app, mock_ib,
                                [7001, 7002, 7003])

        contract.conId = 7002  # another duplicate
        algo_app.get_contract_details(contract)

        verify_contract_details(contract, algo_app, mock_ib,
                                [7001, 7002, 7003])

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
        conId_list: list of con ids

    """
    assert len(algo_app.contract_details) == len(conId_list)

    if len(conId_list) > 0:
        # first, save the algo_app contracts and contract_details
        contracts_ds = algo_app.contracts
        contract_details_ds = algo_app.contract_details

        # next, reload algo_app contracts and contract_details from csv
        # so we can test that they were saved and restored
        # correctly (i.e., we will compare them against
        # what we just loaded)
        contracts_path = algo_app.ds_catalog.get_path('contracts')
        logger.info('contracts_path: %s', contracts_path)
        algo_app.contracts = algo_app.load_contracts(contracts_path)
        algo_app.load_contract_details()

        # print('contract_details_ds:\n', contract_details_ds)
        # print('contract_details_ds.__dict__:\n',
        #       contract_details_ds.__dict__)

        for conId in conId_list:
            # match_desc = mock_ib.contract_descriptions.loc[
            #     mock_ib.contract_descriptions['conId'] == conId]

            # match_desc = match_desc.iloc[0]

            contract1 = get_contract_obj(
                algo_app.contracts.loc[conId].to_dict())

            contract2 = get_contract_obj(contracts_ds.loc[conId].to_dict())

            compare_contracts(contract1,
                              contract2)

            idx = conId - 7001  # $$$ bypass for now
            contract3 = get_contract_from_mock_desc(idx, mock_ib)

            assert compare_contracts(contract1,
                                     contract3)

            contract_details1 = get_contract_details_obj(
                algo_app.contract_details.loc[conId].to_dict())

            contract_details2 = get_contract_details_obj(
                contract_details_ds.loc[conId].to_dict())

            compare_contract_details(contract_details1,
                                     contract_details2)

            contract_details3 = \
                get_contract_details_from_mock_desc(idx, mock_ib)

            compare_contract_details(contract_details1,
                                     contract_details3)


###############################################################################
###############################################################################
# TestExtraContractFields
###############################################################################
###############################################################################
class TestExtraContractFields:
    """TestExtraContractFields class."""

    ###########################################################################
    # test_contract_combo_legs
    ###########################################################################
    def test_contract_combo_legs(self,
                                 algo_app: "AlgoApp",
                                 mock_ib: Any
                                 ) -> None:
        """Test combo legs in contract.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        num_contracts = 5
        contract_list = []
        contract_df = pd.DataFrame()
        # get the path for saving/loading the combo legs contract df
        extra_contract_path = \
            algo_app.ds_catalog.get_path('extra_contract')
        logger.info('extra_contract_path: %s', extra_contract_path)

        for i in range(num_contracts):
            contract = get_contract_from_mock_desc(i, mock_ib)

            # add combo legs
            combo_leg_list = build_combo_legs(i, algo_app, mock_ib)
            if combo_leg_list:
                contract.comboLegs = combo_leg_list
            elif i % 2 == 1:  # empty list
                # empty list for odd, None for even
                contract.comboLegs = []

            contract_list.append(contract)
            contract_dict = get_contract_dict(contract)
            contract_df = \
                contract_df.append(pd.DataFrame(contract_dict,
                                                index=[contract.conId]))
        # Save dataframe to csv
        contract_df.to_csv(extra_contract_path)

        # read dataframe from csv
        contract_df2 = algo_app.load_contracts(extra_contract_path)

        for i in range(num_contracts):
            contract1 = contract_list[i]
            contract_dict2 = contract_df2.iloc[i].to_dict()
            contract2 = get_contract_obj(contract_dict2)

            compare_contracts(contract1, contract2)


###############################################################################
# build_combo_legs
###############################################################################
def build_combo_legs(idx: int,
                     algo_app: "AlgoApp",
                     mock_ib: Any) -> List[ComboLeg]:
    """Build the combo leg list for a contract.

    Args:
        idx: the index of the entry being built
        algo_app: pytest fixture instance of AlgoApp (see conftest.py)
        mock_ib: pytest fixture of contract_descriptions

    Returns:
        list with zero or more ComboLeg items

    """
    num_combo_legs = idx % 4  # vary the number built from 0 to 3
    combo_leg_list = []
    for j in range(num_combo_legs):
        combo_leg = ComboLeg()
        combo_leg.conId = \
            mock_ib.combo_legs.cl_conId.iloc[idx + j]
        combo_leg.ratio = \
            mock_ib.combo_legs.cl_ratio.iloc[idx + j]
        combo_leg.action = \
            mock_ib.combo_legs.cl_action.iloc[idx + j]
        combo_leg.exchange = \
            mock_ib.combo_legs.cl_exchange.iloc[idx + j]
        combo_leg.openClose = \
            mock_ib.combo_legs.cl_openClose.iloc[idx + j]
        combo_leg.shortSaleSlot = \
            mock_ib.combo_legs.cl_shortSaleSlot.iloc[idx + j]
        combo_leg.designatedLocation = \
            mock_ib.combo_legs.cl_designatedLocation.iloc[idx + j]
        combo_leg.exemptCode = \
            mock_ib.combo_legs.cl_exemptCode.iloc[idx + j]

        combo_leg_list.append(combo_leg)

    return combo_leg_list


###############################################################################
# get_contract_from_mock_desc
###############################################################################
def get_contract_from_mock_desc(idx: int,
                                mock_ib: Any) -> Contract:
    """Build and return a contract from the mock description.

    Args:
        idx: index of mock_desc and mock_dnc to use
        mock_ib: contains contract data frames

    Returns:
          Contract with fields from input mock_desc and mock_dnc

    """
    ret_con = Contract()
    ret_con.conId = mock_ib.contract_descriptions.conId.iloc[idx]
    ret_con.symbol = mock_ib.contract_descriptions.symbol.iloc[idx]
    ret_con.secType = mock_ib.contract_descriptions.secType.iloc[idx]
    ret_con.lastTradeDateOrContractMonth = \
        mock_ib.contract_descriptions.lastTradeDateOrContractMonth.iloc[idx]
    ret_con.strike = mock_ib.contract_descriptions.strike.iloc[idx]
    ret_con.right = mock_ib.contract_descriptions.right.iloc[idx]
    ret_con.multiplier = mock_ib.contract_descriptions.multiplier.iloc[idx]
    ret_con.exchange = mock_ib.contract_descriptions.exchange.iloc[idx]
    ret_con.primaryExchange = \
        mock_ib.contract_descriptions.primaryExchange.iloc[idx]
    ret_con.currency = mock_ib.contract_descriptions.currency.iloc[idx]
    ret_con.localSymbol = mock_ib.contract_descriptions.localSymbol.iloc[idx]
    ret_con.tradingClass = mock_ib.contract_descriptions.tradingClass.iloc[idx]
    ret_con.includeExpired = \
        mock_ib.contract_descriptions.includeExpired.iloc[idx]
    ret_con.secIdType = mock_ib.contract_descriptions.secIdType.iloc[idx]
    ret_con.secId = mock_ib.contract_descriptions.secId.iloc[idx]

    # combos
    ret_con.comboLegsDescrip = \
        mock_ib.contract_descriptions.comboLegsDescrip.iloc[idx]
    # ret_con.comboLegs = mock_ib.contract_descriptions.comboLegs

    # build a delta_neutral_contract every third time
    if (idx % 3) == 0:
        delta_neutral_contract = DeltaNeutralContract()
        # item() is used to convert numpy.int64 to python int
        delta_neutral_contract.conId = \
            mock_ib.delta_neutral_contract.conId.iloc[0]
        delta_neutral_contract.delta = \
            mock_ib.delta_neutral_contract.delta.iloc[0]
        delta_neutral_contract.price = \
            mock_ib.delta_neutral_contract.price.iloc[0]

        ret_con.deltaNeutralContract = delta_neutral_contract

    return ret_con


###############################################################################
# get_contract_details_from_mock_desc
###############################################################################
def get_contract_details_from_mock_desc(idx, mock_ib: Any) -> ContractDetails:
    """Build and return a contract_details from the mock description.

    Args:
        idx: index of entry to use
        mock_ib: DataFrame with values for contract_details

    Returns:
          ContractDetails with fields from input mock_desc

    """
    ret_con = ContractDetails()
    ret_con.contract = get_contract_from_mock_desc(idx, mock_ib)
    ret_con.marketName = mock_ib.contract_descriptions.marketName.iloc[idx]
    ret_con.minTick = mock_ib.contract_descriptions.minTick.iloc[idx]
    ret_con.orderTypes = mock_ib.contract_descriptions.orderTypes.iloc[idx]
    ret_con.validExchanges = \
        mock_ib.contract_descriptions.validExchanges.iloc[idx]
    ret_con.priceMagnifier = \
        mock_ib.contract_descriptions.priceMagnifier.iloc[idx]
    ret_con.underConId = mock_ib.contract_descriptions.underConId.iloc[idx]
    ret_con.longName = mock_ib.contract_descriptions.longName.iloc[idx]
    ret_con.contractMonth = \
        mock_ib.contract_descriptions.contractMonth.iloc[idx]
    ret_con.industry = mock_ib.contract_descriptions.industry.iloc[idx]
    ret_con.category = mock_ib.contract_descriptions.category.iloc[idx]
    ret_con.subcategory = mock_ib.contract_descriptions.subcategory.iloc[idx]
    ret_con.timeZoneId = mock_ib.contract_descriptions.timeZoneId.iloc[idx]
    ret_con.tradingHours = mock_ib.contract_descriptions.tradingHours.iloc[idx]
    ret_con.liquidHours = mock_ib.contract_descriptions.liquidHours.iloc[idx]
    ret_con.evRule = mock_ib.contract_descriptions.evRule.iloc[idx]
    ret_con.evMultiplier = mock_ib.contract_descriptions.evMultiplier.iloc[idx]
    ret_con.mdSizeMultiplier = \
        mock_ib.contract_descriptions.mdSizeMultiplier.iloc[idx]
    ret_con.aggGroup = mock_ib.contract_descriptions.aggGroup.iloc[idx]
    ret_con.underSymbol = mock_ib.contract_descriptions.underSymbol.iloc[idx]
    ret_con.underSecType = mock_ib.contract_descriptions.underSecType.iloc[idx]
    ret_con.marketRuleIds = \
        mock_ib.contract_descriptions.marketRuleIds.iloc[idx]

    secIdList = mock_ib.contract_descriptions.secIdList.iloc[idx]
    new_secIdList = []
    for j in range(0,
                   2 * mock_ib.contract_descriptions.secIdListCount.iloc[idx],
                   2):
        tag = secIdList[j]
        value = secIdList[j+1]
        tag_value = TagValue(tag, value)
        new_secIdList.append(tag_value)
    ret_con.secIdList = new_secIdList

    ret_con.realExpirationDate = \
        mock_ib.contract_descriptions.realExpirationDate
    # ret_con.lastTradeTime = mock_ib.contract_descriptions.lastTradeTime
    ret_con.stockType = mock_ib.contract_descriptions.stockType

    return ret_con


###############################################################################
# compare_tag_value
###############################################################################
def compare_tag_value(tag_value1: TagValue,
                      tag_value2: TagValue
                      ) -> None:
    """Compare two tag_value objects for equality.

    Args:
        tag_value1: tag_value 1
        tag_value2: tag_value 2

    """
    assert tag_value1.tag == tag_value2.tag

    assert isinstance(tag_value1.tag, str)

    assert isinstance(tag_value2.tag, str)

    assert tag_value1.value == tag_value2.value

    assert isinstance(tag_value1.value, str)

    assert isinstance(tag_value2.value, str)


###############################################################################
# compare_combo_legs
###############################################################################
def compare_combo_legs(cl1: ComboLeg,
                       cl2: ComboLeg
                       ) -> None:
    """Compare two combo leg objects for equality.

    Args:
        cl1: combo leg 1
        cl2: combo leg 2

    """
    assert cl1.conId == cl2.conId

    assert cl1.ratio == cl2.ratio

    assert cl1.action == cl2.action

    assert cl1.exchange == cl2.exchange

    assert cl1.openClose == cl2.openClose

    assert cl1.shortSaleSlot == cl2.shortSaleSlot

    assert cl1.designatedLocation == cl2.designatedLocation

    assert cl1.exemptCode == cl2.exemptCode

    verify_combo_leg_types(cl1)
    verify_combo_leg_types(cl1)


###############################################################################
# verify_combo_leg_types
###############################################################################
def verify_combo_leg_types(combo_leg: ComboLeg) -> None:
    """Verify that combo_leg fields are correct type.

    Args:
        combo_leg: combo_leg to verify

    """
    assert isinstance(combo_leg.conId, (int, np.int64))

    assert isinstance(combo_leg.ratio, (int, np.int64))

    assert isinstance(combo_leg.action, str)

    assert isinstance(combo_leg.exchange, str)

    assert isinstance(combo_leg.openClose, (int, np.int64))

    assert isinstance(combo_leg.shortSaleSlot, (int, np.int64))

    assert isinstance(combo_leg.designatedLocation, str)

    assert isinstance(combo_leg.exemptCode, (int, np.int64))


###############################################################################
# compare_delta_neutral_contracts
###############################################################################
def compare_delta_neutral_contracts(con1: DeltaNeutralContract,
                                    con2: DeltaNeutralContract
                                    ) -> None:
    """Compare two delta neutral contracts for equality.

    Args:
        con1: contract 1
        con2: contract 2

    """
    assert con1.conId == con2.conId

    assert isinstance(con1.conId, (int, np.int64))

    assert isinstance(con2.conId, int)

    assert con1.delta == con2.delta

    assert isinstance(con1.delta, float)

    assert isinstance(con2.delta, float)

    assert con1.price == con2.price

    assert isinstance(con1.price, float)

    assert isinstance(con2.price, float)


###############################################################################
# compare_contracts
###############################################################################
def compare_contracts(con1: Contract, con2: Contract) -> None:
    """Compare two contracts for equality.

    Args:
        con1: contract 1
        con2: contract 2

    """
    assert con1.conId == con2.conId

    assert con1.symbol == con2.symbol

    assert con1.secType == con2.secType

    assert (con1.lastTradeDateOrContractMonth
           == con2.lastTradeDateOrContractMonth)

    assert con1.strike == con2.strike

    assert con1.right == con2.right

    assert con1.multiplier == con2.multiplier

    assert con1.exchange == con2.exchange

    assert con1.primaryExchange == con2.primaryExchange

    assert con1.currency == con2.currency

    assert con1.localSymbol == con2.localSymbol

    assert con1.tradingClass == con2.tradingClass

    assert con1.includeExpired == con2.includeExpired

    assert con1.secIdType == con2.secIdType

    assert con1.secId == con2.secId

    # combos
    assert con1.comboLegsDescrip == con2.comboLegsDescrip

    if con1.comboLegs and con2.comboLegs:
        assert len(con1.comboLegs) == len(con2.comboLegs)

        for i in range(len(con1.comboLegs)):
            compare_combo_legs(con1.comboLegs[i],
                               con2.comboLegs[i])
    else:  # check whether one contract has it and the other does not
        assert not (con1.comboLegs or con2.comboLegs)

    if con1.deltaNeutralContract and con2.deltaNeutralContract:
        compare_delta_neutral_contracts(con1.deltaNeutralContract,
                                        con2.deltaNeutralContract)
    else:  # check whether one contract has it and one does not
        assert not (con1.deltaNeutralContract or con2.deltaNeutralContract)

    verify_contract_types(con1)
    verify_contract_types(con2)


###############################################################################
# verify_contract_types
###############################################################################
def verify_contract_types(contract: Contract) -> None:
    """Verify that contract fields are correct type.

    Args:
        contract: contract to verify

    """
    assert isinstance(contract.conId, (int, np.int64))

    assert isinstance(contract.symbol, str)

    assert isinstance(contract.secType, str)

    assert isinstance(contract.lastTradeDateOrContractMonth, str)

    assert isinstance(contract.strike, float)

    assert isinstance(contract.right, str)

    assert isinstance(contract.multiplier, str)

    assert isinstance(contract.exchange, str)

    assert isinstance(contract.primaryExchange, str)

    assert isinstance(contract.currency, str)

    assert isinstance(contract.localSymbol, str)

    assert isinstance(contract.tradingClass, str)

    assert isinstance(contract.includeExpired, (bool, np.bool_))

    assert isinstance(contract.secIdType, str)

    assert isinstance(contract.secId, str)

    # combos
    assert isinstance(contract.comboLegsDescrip, str)

    assert isinstance(contract.comboLegs, (list, type(None)))

    if contract.comboLegs:
        for combo_leg in contract.comboLegs:
            assert isinstance(combo_leg, ComboLeg)

    assert isinstance(contract.deltaNeutralContract,
                      (DeltaNeutralContract, type(None)))


###############################################################################
# compare_contract_details
###############################################################################
def compare_contract_details(con1: ContractDetails,
                             con2: ContractDetails
                             ) -> None:
    """Compare two contract_details for equality.

    Args:
        con1: contract_details 1
        con2: contract_details 2

    """
    if con1.contract and con2.contract:
        compare_contracts(con1.contract, con2.contract)

    else:  # check whether one contract_details has it, one does not
        assert not (con1.contract or con2.contract)

    assert con1.marketName == con2.marketName

    assert con1.minTick == con2.minTick

    assert con1.orderTypes == con2.orderTypes

    assert con1.validExchanges == con2.validExchanges

    assert con1.priceMagnifier == con2.priceMagnifier

    assert con1.underConId == con2.underConId

    assert con1.longName == con2.longName

    assert con1.contractMonth == con2.contractMonth

    assert con1.industry == con2.industry

    assert con1.category == con2.category

    assert con1.subcategory == con2.subcategory

    assert con1.timeZoneId == con2.timeZoneId

    assert con1.tradingHours == con2.tradingHours

    assert con1.liquidHours == con2.liquidHours

    assert con1.evRule == con2.evRule

    assert con1.evMultiplier == con2.evMultiplier

    assert con1.mdSizeMultiplier == con2.mdSizeMultiplier

    assert con1.aggGroup == con2.aggGroup

    assert con1.underSymbol == con2.underSymbol

    assert con1.underSecType == con2.underSecType

    assert con1.marketRuleIds == con2.marketRuleIds

    if con1.secIdList and con2.secIdList:
        assert len(con1.secIdList) == len(con2.secIdList)
        for i in range(len(con1.secIdList)):
            compare_tag_value(con1.secIdList[i], con2.secIdList[i])
    else:  # check whether one contract_details has it, one does not
        assert not (con1.secIdList or con2.secIdList)

    assert con1.realExpirationDate == con2.realExpirationDate

    assert con1.lastTradeTime == con2.lastTradeTime

    assert con1.stockType == con2.stockType

    # BOND values
    assert con1.cusip == con2.cusip

    assert con1.ratings == con2.ratings

    assert con1.descAppend == con2.descAppend

    assert con1.bondType == con2.bondType

    assert con1.couponType == con2.couponType

    assert con1.callable == con2.callable

    assert con1.putable == con2.putable

    assert con1.coupon == con2.coupon

    assert con1.convertible == con2.convertible

    assert con1.maturity == con2.maturity

    assert con1.issueDate == con2.issueDate

    assert con1.nextOptionDate == con2.nextOptionDate

    assert con1.nextOptionType == con2.nextOptionType

    assert con1.nextOptionPartial == con2.nextOptionPartial

    assert con1.notes == con2.notes


###############################################################################
# fundamental data
###############################################################################
# class TestAlgoAppFundamentalData:
#     """TestAlgoAppContractDetails class."""
#
#     def test_get_contract_details_0_entries(self,
#                                             algo_app: "AlgoApp",
#                                             mock_ib: Any
#                                             ) -> None:
#         """Test contract details for non-existent conId.
#
#         Args:
#             algo_app: pytest fixture instance of AlgoApp (see conftest.py)
#             mock_ib: pytest fixture of contract_descriptions
#
#         """
#         verify_algo_app_initialized(algo_app)
#
#         logger.debug("about to connect")
#         algo_app.connect_to_ib("127.0.0.1",
#                                algo_app.PORT_FOR_LIVE_TRADING,
#                                client_id=0)
#
#         # verify that algo_app is connected and alive with a valid reqId
#         verify_algo_app_connected(algo_app)
#
#         contract = Contract()  # create an empty contract with conId of 0
#         algo_app.get_contract_details(contract)
#
#         verify_contract_details(contract, algo_app, mock_ib, [0])
#
#         algo_app.disconnect_from_ib()
#         verify_algo_app_disconnected(algo_app)
