"""test_algo_api.py module."""

# from datetime import datetime, timedelta
# import pytest
# import sys
# from pathlib import Path
import numpy as np

from typing import Any  # Callable, cast, Tuple, Union
# from typing_extensions import Final

# from ibapi.contract import ContractDescription

from scottbrian_algo1.algo_api import AlgoApp
# from scottbrian_utils.diag_msg import diag_msg
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


def verify_algo_app_initialized(algo_app: "AlgoApp") -> None:
    """Helper function to verify the also_app instance is initialized.

    Args:
        algo_app: instance of AlgoApp that is to be checked

    """
    assert len(algo_app.ds_catalog) > 0
    assert algo_app.next_request_id == 0
    assert algo_app.stock_symbols.empty
    assert algo_app.response_complete_event.is_set() is False
    assert algo_app.nextValidId_event.is_set() is False
    assert algo_app.run_thread.is_alive() is False


def verify_algo_app_connected(algo_app: "AlgoApp") -> None:
    """Helper function to verify we are connected to ib.

    Args:
        algo_app: instance of AlgoApp that is to be checked

    """
    assert algo_app.run_thread.is_alive()
    assert algo_app.isConnected()
    assert algo_app.next_request_id == 1


def verify_algo_app_disconnected(algo_app: "AlgoApp") -> None:
    """Helper function to verify we are disconnected from ib.

    Args:
        algo_app: instance of AlgoApp that is to be checked

    """
    assert not algo_app.run_thread.is_alive()
    assert not algo_app.isConnected()

def verify_match_symbols(algo_app, mock_ib, pattern, exp_num_matches):
    """Verify that we find symbols correctly

    Args:
        algo_app: instance of AlgoApp from conftest pytest fixture
        mock_ib: pytest fixture of contract_descriptions
        pattern: symbols to use for searching
        exp_num_matches: number of matches expected

    """
    verify_algo_app_initialized(algo_app)

    try:
        logger.debug("about to connect")
        assert algo_app.connect_to_ib("127.0.0.1", 7496, client_id=0)
        verify_algo_app_connected(algo_app)

        # make request for symbol that will be returned
        algo_app.request_symbols(pattern)

        match_descs = mock_ib.contract_descriptions.loc[
            (mock_ib.contract_descriptions['symbol'].str.
             startswith(pattern))
            & (mock_ib.contract_descriptions['secType'] == 'STK')
            & (mock_ib.contract_descriptions['currency'] == 'USD')
            & (if_opt_in_derivative_types(mock_ib.contract_descriptions))
            ]

        if exp_num_matches <= 16:
            assert (len(algo_app.stock_symbols)
                    == len(match_descs)
                    == exp_num_matches)
        else:
            assert len(algo_app.stock_symbols) == 16
            assert len(match_descs) == exp_num_matches

        if exp_num_matches > 0:
            match_descs = match_descs.drop(columns=['secType',
                                                    'currency',
                                                    'derivative_types'])
            match_descs = match_descs.iloc[0:16]
            comp_df = algo_app.stock_symbols.compare(match_descs)
            assert comp_df.empty

    finally:
        logger.debug('disconnecting')
        algo_app.disconnect_from_ib()
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


class TestAlgoApp:
    """TestAlgoApp class."""

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
        assert not algo_app.connect_to_ib("127.0.0.1",
                                          mock_ib.PORT_FOR_REQID_TIMEOUT,
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
        # verify symbol table has zero entries for the symbol
        verify_match_symbols(algo_app,
                             mock_ib,
                             nonexistent_symbol_arg,
                             exp_num_matches=0)

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
        # verify symbol table has one entry for the symbol
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_1_arg,
                             exp_num_matches=1)

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
        # verify symbol table has one entry for the symbol
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_2_arg,
                             exp_num_matches=2)

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
        # verify symbol table has one entry for the symbol
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_3_arg,
                             exp_num_matches=3)

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
        # verify symbol table has one entry for the symbol
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_4_arg,
                             exp_num_matches=4)

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
        # verify symbol table has one entry for the symbol
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_8_arg,
                             exp_num_matches=8)

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
        # verify symbol table has one entry for the symbol
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_12_arg,
                             exp_num_matches=12)

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
        # verify symbol table has one entry for the symbol
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_16_arg,
                             exp_num_matches=16)

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
        # verify symbol table has one entry for the symbol
        verify_match_symbols(algo_app,
                             mock_ib,
                             symbol_pattern_match_20_arg,
                             exp_num_matches=20)

    # def test_get_symbols_recursive(self,
    #                                algo_app: "AlgoApp",
    #                                mock_ib: Any,
    #                                symbol_pattern_arg: str) -> None:
    #     """Test request_symbols with pattern that finds many symbols.
    #
    #     Args:
    #         algo_app: instance of AlgoApp from conftest pytest fixture
    #         mock_ib: pytest fixture of contract_descriptions
    #         symbol_pattern_arg: symbols to use for searching
    #
    #     The steps are:
    #         * mock connect to ib
    #         * request symbols with pattern for one match
    #         * verify that stock symbols table has the expected entry
    #
    #     """
    #     verify_algo_app_initialized(algo_app)
    #
    #     logger.debug("about to connect")
    #     assert algo_app.connect_to_ib("127.0.0.1", 7496, client_id=0)
    #     verify_algo_app_connected(algo_app)
    #
    #     # delete the stock_symbol csv file if it exists
    #     stock_symbols_path = algo_app.ds_catalog.get_path('symbols')
    #     logger.info('path: %s', stock_symbols_path)
    #     stock_symbols_path.unlink(missing_ok=True)
    #
    #     # make request for symbol that will be returned
    #
    #     algo_app.request_symbols(symbol_pattern_arg)
    #
    #     # verify symbol table has one entry for the symbol
    #     match_descs = mock_ib.contract_descriptions.loc[
    #         (mock_ib.contract_descriptions['symbol'].str.
    #          startswith(symbol_pattern_arg))]
    #     # & (mock_ib.contract_descriptions['secType'].str == 'STK') &
    #     # (mock_ib.contract_descriptions['currency'].str == 'USD')]
    #     diag_msg('len(match_descs):', len(match_descs))
    #     diag_msg('len(algo_app.stock_symbols):', len(algo_app.stock_symbols))
    #     diag_msg('match_descs:', match_descs)
    #     diag_msg('algo_app.stock_symbols:', algo_app.stock_symbols)
    #     assert len(algo_app.stock_symbols) == len(match_descs)
