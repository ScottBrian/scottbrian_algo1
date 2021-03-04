"""test_algo_api.py module."""

# from datetime import datetime, timedelta
# import pytest
# import sys

from typing import Any  # Callable, cast, Tuple, Union
# from typing_extensions import Final

from scottbrian_algo1.algo_api import AlgoApp
from scottbrian_utils.diag_msg import diag_msg
from scottbrian_utils.file_catalog import FileCatalog
# from datetime import datetime
import logging

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

        assert algo_app.next_request_id == 0

        print()
        # diag_msg("about to connect")

        # we are testing connect_to_ib and the subsequent code that gets
        # control as a result, such as getting the first requestID and then
        # starting a separate thread for the run loop.
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

    def test_get_symbols(self,
                         algo_app: "AlgoApp",
                         test_cat: FileCatalog,
                         monkeypatch: Any) -> None:
        """Test getting symbols and placing into data base.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            monkeypatch: pytest fixture

        The steps are:
        
            * load catalog for testing
            * connect to ib
            * request symbols that start with a
            * verify that table contains expected symbols and info for a
            * request symbols that start with c
            * verify that table contains expected symbols and info for a and c
            

        
        """
        pass
