"""test_algo_api.py module."""

# from datetime import datetime, timedelta
# import pytest
# import sys

# from typing import Any, Callable, cast, Tuple, Union
# from typing_extensions import Final

from scottbrian_algo1.algo_api import AlgoApp


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

    def test_connect_to_IB(self, algo_app: "AlgoApp") -> None:
        """Test connecting to IB.

        Args:
            algo_app: instance of AlgoApp from conftest fixture

        """
        assert algo_app.next_request_id == 0
