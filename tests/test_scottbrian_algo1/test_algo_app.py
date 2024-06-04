"""test_algo_app.py module."""

########################################################################
# Standard Library
########################################################################
from abc import ABC, abstractmethod
from collections import deque, defaultdict
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
import itertools as it
from itertools import combinations, chain
import importlib
from pathlib import Path
import queue

import logging

from more_itertools import roundrobin

import random
import re
import string
from sys import _getframe
import time
from typing import (
    Any,
    Callable,
    ClassVar,
    cast,
    Generator,
    NamedTuple,
    Type,
    TypeAlias,
    TypedDict,
    TYPE_CHECKING,
    Optional,
    Union,
)
from typing_extensions import Unpack, NotRequired
import threading

########################################################################
# Third Party
########################################################################
import pytest

import math
import numpy as np
import pandas as pd  # type: ignore

from scottbrian_utils.file_catalog import FileCatalog
from scottbrian_utils.msgs import Msgs
from scottbrian_utils.log_verifier import LogVer
from scottbrian_utils.diag_msg import get_formatted_call_sequence, get_caller_info
from scottbrian_locking import se_lock as sel

import scottbrian_paratools.smart_thread as st
from scottbrian_paratools.smart_thread import SmartThread, SmartThreadRequestTimedOut


########################################################################
# ibapi
########################################################################
from ibapi.connection import Connection
from ibapi.message import IN, OUT
from ibapi.comm import make_field, make_msg, read_msg, read_fields
from ibapi.common import NO_VALID_ID
from ibapi.errors import FAIL_CREATE_SOCK
import socket
from ibapi.tag_value import TagValue  # type: ignore
from ibapi.contract import ComboLeg  # type: ignore
from ibapi.contract import DeltaNeutralContract
from ibapi.contract import Contract, ContractDetails

########################################################################
# local
########################################################################
from scottbrian_algo1.algo_maps import get_contract_dict, get_contract_obj
from scottbrian_algo1.algo_maps import get_contract_details_obj

from test_scottbrian_algo1.conftest import MockIB

import scottbrian_algo1.algo_app

scottbrian_algo1.etrace_enabled = True
importlib.reload(scottbrian_algo1.algo_app)

from scottbrian_algo1.algo_app import (
    AlgoApp,
    AlreadyConnected,
    ConnectTimeout,
    DisconnectTimeout,
    RequestTimeout,
    DisconnectDuringRequest,
)


########################################################################
# get logger
########################################################################
logger = logging.getLogger(__name__)


########################################################################
# TypeAlias
########################################################################
IntFloat: TypeAlias = Union[int, float]
OptIntFloat: TypeAlias = Optional[IntFloat]


########################################################################
# AlgoApp test exceptions
########################################################################
class ErrorTstAlgoApp(Exception):
    """Base class for exception in this module."""

    pass


class FailedLockVerify(ErrorTstAlgoApp):
    """An expected lock position was not found."""

    pass


class InvalidConfigurationDetected(ErrorTstAlgoApp):
    """InvalidConfigurationDetected exception class."""

    pass


proj_dir = Path.cwd().resolve().parents[1]  # back two directories
test_cat = FileCatalog(
    {
        "symbols": Path(proj_dir / "t_datasets/symbols.csv"),
        "mock_contract_descs": Path(proj_dir / "t_datasets/mock_contract_descs.csv"),
    }
)


########################################################################
# LogVerMgr
########################################################################
class LogVerMgr(LogVer):
    """LogVerMgr to test with LogVer."""

    ####################################################################
    # __init__
    ####################################################################
    # def __init__(self, log_name: str = "root", str_col_width: Optional[int] = None):
    #     super().__init__(log_name=log_name, str_col_width=str_col_width)
    #
    # ####################################################################
    # # add_entry_trace_pattern
    # ####################################################################
    # def add_entry_trace_pattern(
    #     self,
    #     ip_addr: str,
    #     port: int,
    #     client_id: int,
    #     timeout: IntFloat,
    #     caller: str,
    # ):
    #     """Method to add entrr trace pattern to LogVer.
    #
    #     Args:
    #         ip_addr (str): IP address of client.
    #         port (int): Port of client.
    #         client_id (int): Client ID.
    #         timeout (IntFloat): Timeout in seconds.
    #         caller (str): Caller ID.
    #
    #     """
    #     con_etrace_entry = (
    #         "algo_app.py::AlgoApp.connect_to_ib:[0-9]+ entry: "
    #         f"{ip_addr=}, {port=}, {client_id=}, "
    #         f"_setup_args='...', {timeout=}, "
    #         f"caller: {caller}"
    #     )
    #     self.add_pattern(pattern=con_etrace_entry)
    #
    #     con_etrace_exit = (
    #         "algo_app.py::AlgoApp.connect_to_ib:[0-9]+ exit: return_value=None"
    #     )
    #
    #     self.add_pattern(pattern=con_etrace_exit)


########################################################################
# SyncAsync
########################################################################
class SyncAsync(Enum):
    RequestSync = auto()
    RequestAsync = auto()


########################################################################
# TimeoutType
########################################################################
class TimeoutType(Enum):
    TimeoutNone = auto()
    TimeoutFalse = auto()
    TimeoutTrue = auto()


########################################################################
# AlgoAppVer
########################################################################
class AlgoAppVer:
    """Class to test AlgoApp."""

    def __init__(
        self,
        log_ver: LogVer,
        app_cat: "FileCatalog",
        algo_group_name: str = "algo_group_1",
        algo_name: str = "algo_1",
        ip_addr: str = "127.0.0.1",
        port: int = AlgoApp.PORT_FOR_LIVE_TRADING,
        client_id: int = 1,
    ) -> None:
        """Initialize the object.

        Args:
            log_ver: log verifier
            algo_group_name: name of algo group
            algo_name: name of algo
            ip_addr: ip address
            port: port to use for IBAPI

        """
        self.log_ver = log_ver

        self.app_cat = app_cat

        self.algo_group_name = algo_group_name
        self.algo_name = algo_name
        self.ip_addr = ip_addr
        self.port = port
        self.client_id = client_id

        self.algo_app_msg_prefix = re.escape(f"AlgoApp {algo_name} ({algo_group_name})")
        self.algo_client_msg_prefix = re.escape(
            f"AlgoClient {algo_name} ({algo_group_name})"
        )
        self.algo_wrapper_msg_prefix = re.escape(
            f"AlgoWrapper {algo_name} ({algo_group_name})"
        )

        self.algo_app = AlgoApp(
            ds_catalog=app_cat, group_name=algo_group_name, algo_name=algo_name
        )

        self.connected: bool = False

        self.verify_algo_app_initialized()

    ####################################################################
    # connect_to_ib
    ####################################################################
    def connect_to_ib(
        self,
        timeout_type: TimeoutType = TimeoutType.TimeoutNone,
        delay: int = 0,
        async_tf: bool = False,
        caller: str = "",
    ) -> None:
        """Connect to ib.

        Args:
            timeout_type: the TimeoutType,
            delay: number of seconds to delay response for the request
                id
            async_tf: if True, connect is being done in remote thread
            caller: caller string to use for error message

        """

        if timeout_type == TimeoutType.TimeoutNone:
            timeout = None
        elif timeout_type == TimeoutType.TimeoutFalse:
            timeout = delay * 2
        else:
            timeout = delay / 2

        etrace_caller = f"{caller} -> test_algo_app.py::AlgoAppVer.connect_to_ib:[0-9]+"

        etrace_entry = (
            "algo_app.py::AlgoApp.connect_to_ib:[0-9]+ entry: "
            f"ip_addr='{self.ip_addr}', port={self.port}, client_id={self.client_id}, "
            f"_setup_args='...', {timeout=}, "
            f"caller: {etrace_caller}"
        )
        self.log_ver.add_pattern(
            pattern=etrace_entry, log_name="scottbrian_utils.entry_trace"
        )

        self.log_ver.add_pattern(
            pattern=f"{self.algo_app_msg_prefix} starting AlgoClient thread",
            log_name="scottbrian_algo1.algo_app",
        )

        if timeout_type != TimeoutType.TimeoutTrue:

            self.log_ver.add_pattern(
                pattern=f"{self.algo_app_msg_prefix} connect successful",
                level=20,
                log_name="scottbrian_algo1.algo_app",
            )
            self.log_ver.add_pattern(
                pattern=f"{self.algo_wrapper_msg_prefix} next valid ID is 1",
                level=20,
                log_name="scottbrian_algo1.algo_wrapper",
            )
            self.log_ver.add_pattern(
                pattern="algo_app.py::AlgoApp.connect_to_ib:[0-9]+ exit: "
                "return_value=None",
                log_name="scottbrian_utils.entry_trace",
            )

        self.connected = True  # tell client_disconnect we are connected
        if timeout_type == TimeoutType.TimeoutNone:
            self.algo_app.connect_to_ib(
                ip_addr="127.0.0.1",
                port=AlgoApp.PORT_FOR_LIVE_TRADING,
                client_id=self.client_id,
            )
        elif timeout_type == TimeoutType.TimeoutFalse:
            self.algo_app.connect_to_ib(
                ip_addr="127.0.0.1",
                port=AlgoApp.PORT_FOR_LIVE_TRADING,
                client_id=self.client_id,
                timeout=timeout,
            )
        else:
            # mock_ib.delay_value = -1
            if async_tf:
                smart_thread_name = f"algo_requestor_[0-9]+.[0-9]+"
            else:
                smart_thread_name = self.algo_name

            func_name = "connect_to_ib"
            error = "ConnectTimeout"
            extra = (
                "waiting to receive nextValid_ID. "
                f"SmartThread name={smart_thread_name}, "
                f"ip_addr='{self.ip_addr}', port={self.port}, "
                f"client_id={self.client_id}"
            )
            test_error_msg = (
                f"{self.algo_app_msg_prefix} {func_name} " f"raising {error} {extra}."
            )
            self.log_ver.add_pattern(
                pattern=test_error_msg,
                level=40,
                log_name="scottbrian_algo1.algo_app",
            )
            with pytest.raises(ConnectTimeout, match=test_error_msg):
                self.algo_app.connect_to_ib(
                    ip_addr="127.0.0.1",
                    port=AlgoApp.PORT_FOR_LIVE_TRADING,
                    client_id=self.client_id,
                    timeout=timeout,
                )

            self.disconnect_from_ib(do_algo_disc=False)

    ####################################################################
    # disconnect_from_ib
    ####################################################################
    def disconnect_from_ib(
        self,
        timeout_type: TimeoutType = TimeoutType.TimeoutNone,
        delay: int = 0,
        async_tf: bool = False,
        caller: str = "",
        do_algo_disc: bool = True,
    ) -> None:
        """Disconnect from ib.

        Args:
            timeout_type: the TimeoutType,
            delay: number of seconds to delay response for the request
                id
            async_tf: if True, disconnect is being done in remote thread
            caller: caller string to use for error message
            do_algo_disc: if True, call algo disconnect, else patterns
                only

        """
        if timeout_type == TimeoutType.TimeoutNone:
            timeout = None
        elif timeout_type == TimeoutType.TimeoutFalse:
            timeout = delay * 2
        else:
            timeout = delay / 2

        etrace_entry = (
            "algo_app.py::AlgoApp.disconnect_from_ib:[0-9]+ entry: "
            f"_setup_args='...', {timeout=}, "
            f"caller: "
        )
        self.log_ver.add_pattern(
            pattern=etrace_entry,
            log_name="scottbrian_utils.entry_trace",
            fullmatch=False,
        )

        if timeout_type != TimeoutType.TimeoutTrue:
            self.log_ver.add_pattern(
                pattern="algo_app.py::AlgoApp.disconnect_from_ib:[0-9]+ exit: "
                "return_value=None",
                log_name="scottbrian_utils.entry_trace",
            )

        self.log_ver.add_pattern(
            pattern=f"{self.algo_app_msg_prefix} calling EClient disconnect",
            log_name="scottbrian_algo1.algo_app",
        )
        self.log_ver.add_pattern(
            pattern=f"{self.algo_app_msg_prefix} joining algo_client",
            log_name="scottbrian_algo1.algo_app",
        )
        self.log_ver.add_pattern(
            pattern=f"{self.algo_app_msg_prefix} disconnect complete",
            level=20,
            log_name="scottbrian_algo1.algo_app",
        )

        self.client_disconnect(caller="algo_app.py::AlgoApp.disconnect_from_ib:[0-9]+")

        if do_algo_disc:
            self.algo_app.disconnect_from_ib()

    ####################################################################
    # disconnect_from_ib
    ####################################################################
    def client_disconnect(self, caller: str) -> None:
        """Client disconnect from ib.

        Args:
            caller: caller string to use in etrace

        """
        self.log_ver.add_pattern(
            pattern="algo_client.py::AlgoClient.disconnect:[0-9]+ entry: "
            f"caller: {caller}",
            log_name="scottbrian_utils.entry_trace",
        )

        self.log_ver.add_pattern(
            pattern="algo_client.py::AlgoClient.disconnect:[0-9]+ exit: "
            "return_value=None",
            log_name="scottbrian_utils.entry_trace",
        )

        self.log_ver.add_pattern(
            pattern=f"{self.algo_client_msg_prefix} setting conn state to "
            f"EClient.DISCONNECTED",
            log_name="scottbrian_algo1.algo_client",
        )

        if self.connected:
            self.connected = False

            self.log_ver.add_pattern(
                pattern=f"{self.algo_client_msg_prefix} disconnecting",
                log_name="scottbrian_algo1.algo_client",
            )
            self.log_ver.add_pattern(
                pattern=f"{self.algo_client_msg_prefix} about to join reader "
                f"reader_id=[0-9]+ for my_id=[0-9]+",
                log_name="scottbrian_algo1.algo_client",
            )
            self.log_ver.add_pattern(
                pattern=f"{self.algo_client_msg_prefix} back from join "
                f"reader_id=[0-9]+ for my_id=[0-9]+",
                log_name="scottbrian_algo1.algo_client",
            )

    ####################################################################
    # shut_down
    ####################################################################
    def shut_down(self, timeout: OptIntFloat = None) -> None:
        """Disconnect from ib.

        Args:
            timeout: number seconds to use for timeout of shut_down call

        """
        etrace_caller = (
            f"{get_formatted_call_sequence(latest=1, depth=1)} -> "
            "test_algo_app.py::AlgoAppVer.shut_down:[0-9]+"
        )

        shut_down_etrace_entry = (
            "algo_app.py::AlgoApp.shut_down:[0-9]+ entry: "
            f"_setup_args='...', {timeout=}, "
            f"caller: {etrace_caller}"
        )
        self.log_ver.add_pattern(
            pattern=shut_down_etrace_entry, log_name="scottbrian_utils.entry_trace"
        )

        self.log_ver.add_pattern(
            pattern="algo_app.py::AlgoApp.shut_down:[0-9]+ exit: return_value=None",
            log_name="scottbrian_utils.entry_trace",
        )

        self.algo_app.shut_down()

        self.disconnect_from_ib(do_algo_disc=False)

    ########################################################################
    # verify_algo_app_initialized
    ########################################################################
    def verify_algo_app_initialized(self) -> None:
        """Helper function to verify the algo_app instance is initialized."""
        assert len(self.algo_app.ds_catalog) > 0
        assert self.algo_app.algo_client.algo_wrapper.request_id == 0
        assert self.algo_app.market_data.symbols.empty
        assert self.algo_app.market_data.stock_symbols.empty
        assert self.algo_app.response_complete_event.is_set() is False
        assert self.algo_app.__repr__() == "AlgoApp(ds_catalog)"
        # assert self.algo_app.ibapi_client_smart_thread.thread is None


########################################################################
# LockMgr
########################################################################
class LockMgr:
    """Class LockMsg manages locking for test cases."""

    def __init__(self, config_ver: "ConfigVerifier", locker_names: list[str]):
        """Initialize the object.

        Args:
            config_ver: the ConfigVerifier object
            locker_names: thread names that do the locking
        """
        self.config_ver = config_ver
        self.locker_avail_q: deque[str] = deque(locker_names)
        self.lock_positions: list[str] = []

    ####################################################################
    # get_lock
    ####################################################################
    def get_lock(self, alt_frame_num: int = 1) -> None:
        """Get the lock and verify the lock positions.

        Args:
            alt_frame_num: frame to get line_num

        """
        locker_name = self.locker_avail_q.pop()
        obtain_lock_serial_num = self.config_ver.add_cmd(
            LockObtain(cmd_runners=locker_name), alt_frame_num=alt_frame_num
        )
        self.lock_positions.append(locker_name)

        # we can confirm only this first lock obtain
        if len(self.lock_positions) == 1:
            self.config_ver.add_cmd(
                ConfirmResponse(
                    cmd_runners=[self.config_ver.commander_name],
                    confirm_cmd="LockObtain",
                    confirm_serial_num=obtain_lock_serial_num,
                    confirmers=locker_name,
                ),
                alt_frame_num=alt_frame_num,
            )

        self.config_ver.add_cmd(
            LockVerify(
                cmd_runners=self.config_ver.commander_name,
                exp_positions=self.lock_positions.copy(),
            ),
            alt_frame_num=alt_frame_num,
        )

    ####################################################################
    # start_request
    ####################################################################
    def start_request(
        self, requestor_name: str, trailing_lock: bool = True, alt_frame_num: int = 1
    ) -> None:
        """Append a requestor and verify lock positions.

        Args:
            requestor_name: thread name of requestor that just obtained
                the lock
            trailing_lock: if True, add a lock at end
            alt_frame_num: frame to get line_num
        """
        self.lock_positions.append(requestor_name)
        self.config_ver.add_cmd(
            LockVerify(
                cmd_runners=self.config_ver.commander_name,
                exp_positions=self.lock_positions.copy(),
            ),
            alt_frame_num=alt_frame_num,
        )
        if trailing_lock:
            self.get_lock(alt_frame_num=alt_frame_num + 1)

    ####################################################################
    # drop_lock
    ####################################################################
    def drop_lock(
        self,
        requestor_complete: bool = False,
        free_all: bool = False,
        alt_frame_num: int = 1,
    ) -> None:
        """Drop the lock and verify positions.

        Args:
            requestor_complete: If True, the requestor has completed
                its smart request and can be removed from the positions
                list. If False, request has progressed and should now be
                behind another lock.
            free_all: specifies that all requests will complete
            alt_frame_num: frame to get line_num
        """
        locker_name = self.lock_positions.pop(0)
        self.locker_avail_q.append(locker_name)
        self.config_ver.add_cmd(
            LockRelease(cmd_runners=locker_name), alt_frame_num=alt_frame_num
        )

        requestor_name = self.lock_positions.pop(0)
        if not requestor_complete:
            self.lock_positions.append(requestor_name)
        if free_all:
            self.lock_positions = []

        self.config_ver.add_cmd(
            LockVerify(
                cmd_runners=self.config_ver.commander_name,
                exp_positions=self.lock_positions.copy(),
            ),
            alt_frame_num=alt_frame_num,
        )

    ####################################################################
    # complete_request
    ####################################################################
    def complete_request(self, free_all: bool = False, alt_frame_num: int = 1) -> None:
        """Drop the lock and verify positions.

        Args:
            free_all: specifies that all requests will complete
            alt_frame_num: frame to get line_num
        """
        self.drop_lock(
            requestor_complete=True, free_all=free_all, alt_frame_num=alt_frame_num
        )

    ####################################################################
    # advance_request
    ####################################################################
    def advance_request(
        self, num_times: int = 1, trailing_lock: bool = True, alt_frame_num: int = 1
    ) -> None:
        """Drop the lock, requeue the requestor, verify positions.

        Args:
            num_times: number of times to do the advance
            trailing_lock: if True, add a lock at end
            alt_frame_num: frame to get line_num
        """
        for _ in range(num_times):
            self.drop_lock(requestor_complete=False, alt_frame_num=alt_frame_num + 1)
            if trailing_lock:
                self.get_lock(alt_frame_num=alt_frame_num + 1)

    ####################################################################
    # advance_request
    ####################################################################
    def swap_requestors(self, alt_frame_num: int = 1) -> None:
        """Swap the requests lock positions.

        Args:
            alt_frame_num: frame to get line_num
        """
        lock_pos_1 = self.lock_positions[1]
        self.lock_positions[1] = self.lock_positions[3]
        self.lock_positions[3] = lock_pos_1

        self.config_ver.add_cmd(
            LockSwap(
                cmd_runners=self.config_ver.commander_name,
                new_positions=self.lock_positions.copy(),
            ),
            alt_frame_num=alt_frame_num,
        )
        self.config_ver.add_cmd(
            LockVerify(
                cmd_runners=self.config_ver.commander_name,
                exp_positions=self.lock_positions.copy(),
            ),
            alt_frame_num=alt_frame_num,
        )


########################################################################
# get_app_catalog
########################################################################
def get_app_catalog(tmp_path_to_use: Any) -> "FileCatalog":
    """Instantiate and return the test catalog.

    Returns:
        An instance of FileCatalog
    """

    d = tmp_path_to_use / "t_files"
    d.mkdir()
    symbols_path = d / "symbols.csv"
    stock_symbols_path = d / "stock_symbols.csv"
    symbol_status_path = d / "symbol_status.csv"
    contracts_path = d / "contracts.csv"
    contract_details_path = d / "contract_details.csv"
    extra_contract_path = d / "extra_contract.csv"
    fundamental_data_path = d / "fundamental_data.csv"

    catalog = FileCatalog(
        {
            "symbols": symbols_path,
            "stock_symbols": stock_symbols_path,
            "symbols_status": symbol_status_path,
            "contracts": contracts_path,
            "contract_details": contract_details_path,
            "extra_contract": extra_contract_path,
            "fundamental_data": fundamental_data_path,
        }
    )

    return catalog


########################################################################
# get_test_catalog
########################################################################
def get_test_catalog() -> "FileCatalog":
    """Instantiate and return the test catalog.

    Returns:
        An instance of FileCatalog
    """
    proj_dir = Path.cwd().resolve().parents[1]  # back two directories
    test_cat = FileCatalog(
        {
            "symbols": Path(proj_dir / "t_datasets/symbols.csv"),
            "mock_contract_descs": Path(
                proj_dir / "t_datasets/mock_contract_descs.csv"
            ),
        }
    )

    return catalog


########################################################################
# get_set
########################################################################
def get_set(item: Optional[Iterable[str]] = None) -> set[Any]:
    """Return a set given the iterable input.

    Args:
        item: iterable to be returned as a set

    Returns:
        A set created from the input iterable item. Note that the set
        will be empty if None was passed in.
    """
    return set({item} if isinstance(item, str) else item or "")


########################################################################
# ConfigCmd
########################################################################
class ConfigCmd(ABC):
    """Configuration command base class."""

    def __init__(self, cmd_runners: Iterable[str]) -> None:
        """Initialize the instance.

        Args:
            cmd_runners: thread names that will execute the command

        """
        # The serial number, line_num, and config_ver are filled in
        # by the ConfigVerifier add_cmd method just before queueing
        # the command.
        self.serial_num: int = 0
        self.line_num: int = 0
        self.alt_line_num: int = 0
        self.config_ver: "ConfigVerifier"

        # specified_args are set in each subclass
        self.specified_args: dict[str, Any] = {}

        self.cmd_runners = get_set(cmd_runners)

        self.arg_list: list[str] = ["cmd_runners", "serial_num", "line_num"]

    def __repr__(self) -> str:
        """Method to provide repr."""
        if TYPE_CHECKING:
            __class__: Type[ConfigVerifier]  # noqa: F842
        classname = self.__class__.__name__
        parms = f"serial={self.serial_num}, line={self.line_num}"
        if self.alt_line_num:
            parms += f"({self.alt_line_num})"
        comma = ", "
        for key, item in self.specified_args.items():
            if item:  # if not None
                if key in self.arg_list:
                    if type(item) is str:
                        parms += comma + f"{key}='{item}'"
                    else:
                        parms += comma + f"{key}={item}"
                    # comma = ', '  # after first item, now need comma
            if key == "f1_create_items":
                create_names: list[str] = []
                for create_item in item:
                    create_names.append(create_item.name)
                parms += comma + f"{create_names=}"

        return f"{classname}({parms})"

    @abstractmethod
    def run_process(self, cmd_runner: str) -> None:
        """Run the command.

        Args:
           cmd_runner: name of thread running the command
        """
        pass


########################################################################
# CreateAlgoApp
########################################################################
class CreateAlgoApp(ConfigCmd):
    """Confirm that an earlier command has completed."""

    def __init__(
        self,
        cmd_runners: Iterable[str],
        ds_catalog: FileCatalog,
        group_name: str = "algo_app_group",
        algo_name: str = "algo_app",
        default_timeout: OptIntFloat = None,
    ) -> None:
        """Initialize the instance.

        Args:
            cmd_runners: thread names that will execute the command
            ds_catalog: contain the paths for data sets
            group_name: name of SmartThread group
            algo_name: name of SmartThread instance for AlgoApp
            default_timeout: number of seconds to use as a timeout
                value if timeout is not specifies on a method that
                provides a timeout parameter. If this default_timeout is
                not specifies, the default_timeout is set to 30 seconds.

        """
        super().__init__(cmd_runners=cmd_runners)
        self.specified_args = locals()  # used for __repr__

        self.ds_catalog = ds_catalog
        self.group_name = group_name

        self.algo_name = algo_name

        self.default_timeout = default_timeout

        self.arg_list += ["group_name", "algo_name", "default_timeout"]

    def run_process(self, cmd_runner: str) -> None:
        """Run the command.

        Args:
           cmd_runner: name of thread running the command
        """
        self.config_ver.handle_create_algo_app(
            cmd_runner=cmd_runner,
            ds_catalog=self.ds_catalog,
            group_name=self.group_name,
            algo_name=self.algo_name,
            default_timeout=self.default_timeout,
        )


########################################################################
# ConnectToIb
########################################################################
class ConnectToIb(ConfigCmd):
    """Confirm that an earlier command has completed."""

    def __init__(
        self,
        cmd_runners: Iterable[str],
        mock_ib: MockIB,
        group_name: str,
        algo_name: str,
        ip_addr: str = "127.0.0.1",
        port: int = 7496,
        client_id: int = 1,
        delay_time: IntFloat = 0,
        timeout_type: TimeoutType = TimeoutType.TimeoutNone,
        timeout: OptIntFloat = None,
    ) -> None:
        """Initialize the instance.

        Args:
            cmd_runners: thread names that will execute the command
            mock_ib: contains monkey patched IB rtns
            group_name: name of SmartThread group
            algo_name: name of SmartThread instance for AlgoApp
            ip_addr: ip address of connection (127.0.0.2)
            port: port number (e.g., 7496)
            client_id: specific client number from 1 to x
            delay_time: number of seconds to delay the connect
            timeout_type: TimeoutType
            timeout: number of seconds for timeout arg of connect

        """
        super().__init__(cmd_runners=cmd_runners)
        self.specified_args = locals()  # used for __repr__

        self.mock_ib = mock_ib

        self.group_name = group_name

        self.algo_name = algo_name

        self.ip_addr = ip_addr
        self.port = port

        self.client_id = client_id

        self.delay_time = delay_time

        self.timeout_type = timeout_type

        self.timeout = timeout

        self.arg_list += ["ip_addr", "port", "client_id"]

    def run_process(self, cmd_runner: str) -> None:
        """Run the command.

        Args:
           cmd_runner: name of thread running the command
        """
        self.config_ver.handle_connect(
            cmd_runner=cmd_runner,
            mock_ib=self.mock_ib,
            group_name=self.group_name,
            algo_name=self.algo_name,
            ip_addr=self.ip_addr,
            port=self.port,
            client_id=self.client_id,
            delay_time=self.delay_time,
            timeout_type=self.timeout_type,
            timeout=self.timeout,
        )


########################################################################
# ShutDown
########################################################################
class ShutDown(ConfigCmd):
    """Confirm that an earlier command has completed."""

    def __init__(
        self,
        cmd_runners: Iterable[str],
        mock_ib: MockIB,
        group_name: str = "algo_app_group",
        algo_name: str = "algo_app",
        delay_time: IntFloat = 0,
        timeout_type: TimeoutType = TimeoutType.TimeoutNone,
        timeout: OptIntFloat = None,
        pending_names: Iterable[str] = None,
    ) -> None:
        """Shut down the AlgoApp.

        Args:
            cmd_runners: thread names that will execute the command
            mock_ib: contains monkey patched IB rtns
            group_name: name of SmartThread group
            algo_name: name of SmartThread instance for AlgoApp
            delay_time: seconds to delay shutdown to drive timeout
                scenarios
            timeout_type: specifies whether to expect the shutdown to
                result in a timeout error
            timeout: number of seconds to use as a timeout value
            pending_names: names of threads that cause a timeout during
                shutdown

        """
        super().__init__(cmd_runners=cmd_runners)
        self.specified_args = locals()  # used for __repr__

        self.mock_ib = mock_ib

        self.group_name = group_name

        self.algo_name = algo_name

        self.delay_time = delay_time

        self.timeout_type = timeout_type

        self.timeout = timeout

        self.pending_names = get_set(pending_names)

        self.arg_list += [
            "group_name",
            "algo_name",
            "delay_time",
            "timeout_type",
            "timeout",
            "pending_names",
        ]

    def run_process(self, cmd_runner: str) -> None:
        """Run the command.

        Args:
           cmd_runner: name of thread running the command
        """
        self.config_ver.handle_shutdown(
            cmd_runner=cmd_runner,
            mock_ib=self.mock_ib,
            group_name=self.group_name,
            algo_name=self.algo_name,
            delay_time=self.delay_time,
            timeout_type=self.timeout_type,
            timeout=self.timeout,
            pending_names=self.pending_names,
        )


########################################################################
# verify_algo_app_initialized
########################################################################
def verify_algo_app_initialized(algo_app: "AlgoApp") -> None:
    """Helper function to verify the algo_app instance is initialized.

    Args:
        algo_app: instance of AlgoApp that is to be checked

    """
    assert len(algo_app.ds_catalog) > 0
    assert algo_app.algo_client.algo_wrapper.request_id == 0
    assert algo_app.market_data.symbols.empty
    assert algo_app.market_data.stock_symbols.empty
    assert algo_app.response_complete_event.is_set() is False
    assert algo_app.__repr__() == "AlgoApp(ds_catalog)"
    # assert algo_app.ibapi_client_smart_thread.thread is None


########################################################################
# verify_algo_app_connected
########################################################################
def verify_algo_app_connected(algo_app: "AlgoApp") -> None:
    """Helper function to verify we are connected to ib.

    Args:
        algo_app: instance of AlgoApp that is to be checked

    """
    assert algo_app.algo_client.thread.is_alive()
    assert algo_app.algo_client.isConnected()
    assert algo_app.algo_client.request_id == 1


########################################################################
# verify_algo_app_disconnected
########################################################################
def verify_algo_app_disconnected(algo_app: "AlgoApp") -> None:
    """Helper function to verify we are disconnected from ib.

    Args:
        algo_app: instance of AlgoApp that is to be checked

    """
    assert not algo_app.algo_client.thread.is_alive()
    assert not algo_app.algo_client.isConnected()


########################################################################
# do_setup
########################################################################
def do_setup(cat_app: "FileCatalog"):
    """Setup the test thread and the algo_app thread.

    Args:
        cat_app: catalog of date sets to use for testing
    """
    test_smart_thread = SmartThread(name="tester1")

    algo_app = AlgoApp(ds_catalog=cat_app, algo_name="algo_app")

    verify_algo_app_initialized(algo_app)
    algo_app.smart_start()

    return test_smart_thread, algo_app


########################################################################
# do_breakdown
########################################################################
def do_breakdown(
    test_smart_thread: SmartThread,
    algo_app: AlgoApp,
    do_disconnect: bool = True,
):
    """Disconnect and join the algo_app.

    Args:
        test_smart_thread: the tester1 SmartThread instance
        algo_app: the AlgoApp to disconnect from and join
        do_disconnect: specifies whether to do disconnect
    """
    if do_disconnect:
        verify_algo_app_connected(algo_app)

        algo_app.disconnect_from_ib()

    verify_algo_app_disconnected(algo_app)

    test_smart_thread.smart_join(targets="algo_app")


####################################################################
# get_smart_thread_name
####################################################################
def get_smart_thread_name(
    search_thread: threading.Thread,
    group_name: str,
) -> str:
    """Get the smart thread for the search thread or None.

    Args:
        search_thread: thread to search for
        group_name: name of group to search

    Returns:
         A list (possibly empty) of SmartThread instances that are
             associated with the input search_thread.

    Note:
        The lock registry must be held excl or shared
    """
    if group_name in SmartThread._registry:
        for name, smart_thread in SmartThread._registry[group_name].registry.items():
            if smart_thread.thread is search_thread:
                return smart_thread.name

    return ""


####################################################################
# lock_verify
####################################################################
def lock_verify(exp_positions: list[list[str, str]], lock: sel.SELock) -> None:
    """Increment the pending operations count.

    Args:
        exp_positions: the expected positions on the lock queue

    Raises:
        FailedLockVerify: lock_verify from {line_num=} timed out
            after {timeout_value} seconds waiting for the
            {exp_positions=} to match
            {st.SmartThread._registry_lock.owner_wait_q=}.

    """
    frame = _getframe(1)
    caller_info = get_caller_info(frame)
    line_num = caller_info.line_num
    del frame
    logger.debug(f"lock_verify entry: {exp_positions=}, {line_num=}")
    start_time = time.time()
    timeout_value = 15
    lock_verified = False
    while not lock_verified:
        lock_verified = True  # assume lock will verify

        if len(exp_positions) != len(lock.owner_wait_q):
            logger.debug(
                f"lock_verify False 1: {len(exp_positions)=}, "
                f"{len(lock.owner_wait_q)=}"
            )
            lock_verified = False
        else:
            for idx, expected_name_group in enumerate(exp_positions):
                expected_name = expected_name_group[0]
                expected_group = expected_name_group[1]
                search_thread = lock.owner_wait_q[idx].thread
                test_name = get_smart_thread_name(
                    search_thread=search_thread, group_name=expected_group
                )
                if test_name != expected_name:
                    logger.debug(f"lock_verify False 2: {test_name=}, {expected_name=}")
                    lock_verified = False
                    break

        if not lock_verified:
            if (time.time() - start_time) > timeout_value:
                raise FailedLockVerify(
                    f"lock_verify from {line_num=} timed out after"
                    f" {timeout_value} seconds waiting for the "
                    f"{exp_positions=} to match \n"
                    f"{lock.owner_wait_q=} "
                )
            time.sleep(0.2)
    logger.debug(f"lock_verify exit: {exp_positions=}, {line_num=}")


########################################################################
# TestSmartThreadInterface class
########################################################################
@pytest.mark.cover
class TestAlgoAppInterface:
    """Test class for SmartThread interface tests."""

    ####################################################################
    # test_smart_thread_interface_1
    ####################################################################
    @pytest.mark.parametrize(
        "group_name_arg",
        ["", "algo_app_group", "group_name_1"],
    )
    @pytest.mark.parametrize(
        "algo_name_arg",
        ["", "algo_app", "algo_app_1"],
    )
    @pytest.mark.parametrize(
        "default_timeout_arg",
        [-1, 0, 0.0, 1, 1.1],
    )
    def test_algo_app_interface_1(
        self,
        group_name_arg: str,
        algo_name_arg: str,
        default_timeout_arg: IntFloat,
    ) -> None:
        """Test SmartThread interface.

        Args:
            group_name_arg: group name to use
            algo_name_arg: algo_name to use
            default_timeout_arg: default timeout arg to use
        """
        from scottbrian_algo1.algo_app import AlgoApp
        from pathlib import Path

        logger.debug("mainline entered")

        proj_dir = Path.cwd().resolve().parents[1]  # back two directories

        test_cat = FileCatalog(
            {
                "symbols": Path(proj_dir / "t_datasets/symbols.csv"),
                "mock_contract_descs": Path(
                    proj_dir / "t_datasets/mock_contract_descs.csv"
                ),
            }
        )

        if group_name_arg == "":
            exp_group_name = "algo_app_group"
        else:
            exp_group_name = group_name_arg

        if algo_name_arg == "":
            exp_algo_name = "algo_app"
        else:
            exp_algo_name = algo_name_arg

        if default_timeout_arg == -1:
            exp_default_timeout = 30
        else:
            exp_default_timeout = default_timeout_arg

        if group_name_arg == "":
            if algo_name_arg == "":
                if default_timeout_arg == -1:
                    algo_app = AlgoApp(ds_catalog=test_cat)
                else:
                    algo_app = AlgoApp(
                        ds_catalog=test_cat, default_timeout=default_timeout_arg
                    )
            else:
                if default_timeout_arg == -1:
                    algo_app = AlgoApp(ds_catalog=test_cat, algo_name=algo_name_arg)
                else:
                    algo_app = AlgoApp(
                        ds_catalog=test_cat,
                        algo_name=algo_name_arg,
                        default_timeout=default_timeout_arg,
                    )
        else:
            if algo_name_arg == "":
                if default_timeout_arg == -1:
                    algo_app = AlgoApp(ds_catalog=test_cat, group_name=group_name_arg)
                else:
                    algo_app = AlgoApp(
                        ds_catalog=test_cat,
                        group_name=group_name_arg,
                        default_timeout=default_timeout_arg,
                    )
            else:
                if default_timeout_arg == -1:
                    algo_app = AlgoApp(
                        ds_catalog=test_cat,
                        group_name=group_name_arg,
                        algo_name=algo_name_arg,
                    )
                else:
                    algo_app = AlgoApp(
                        ds_catalog=test_cat,
                        group_name=group_name_arg,
                        algo_name=algo_name_arg,
                        default_timeout=default_timeout_arg,
                    )

        assert algo_app.ds_catalog == test_cat
        assert algo_app.group_name == exp_group_name
        assert algo_app.algo_name == exp_algo_name
        assert algo_app.default_timeout == exp_default_timeout

        algo_app.shut_down()

        logger.debug("mainline exiting")


########################################################################
# TestAlgoExamples
########################################################################
@pytest.mark.cover
class TestAlgoExamples:
    """Test class for SmartThread example tests."""

    ####################################################################
    # test_smart_thread_instantiation_example_1
    ####################################################################
    def test_algo_instantiation_example_1(self, capsys: Any) -> None:
        """Test smart_thread instantiation example 1.

        Create a SmartThread configuration for threads named alpha and
        beta, send and receive a message, and resume a wait. Note the
        use of auto_start=False and doing the smart_start.

        Args:
            capsys: pytest fixture to get the print output
        """
        print("mainline alpha entered")

        logger.debug("mainline exiting")


########################################################################
# TestAlgoAppBasicTests
########################################################################
class TestAlgoAppBasicTests:
    """TestAlgoAppConnect class."""

    ####################################################################
    # test_mock_connect_to_ib
    ####################################################################
    @pytest.mark.parametrize("async_tf_arg", [True, False])
    @pytest.mark.parametrize("delay_arg", [0, 2, 4])
    @pytest.mark.parametrize(
        "timeout_type_arg",
        [
            TimeoutType.TimeoutNone,
            TimeoutType.TimeoutFalse,
            TimeoutType.TimeoutTrue,
        ],
    )
    def test_mock_connect_to_ib(
        self,
        async_tf_arg: bool,
        delay_arg: int,
        timeout_type_arg: TimeoutType,
        app_cat: "FileCatalog",
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test connecting to IB.

        Args:
            async_tf_arg: if True, do async connect
            delay_arg: number of seconds to delay
            timeout_type_arg: specifies whether timeout should occur
            app_cat: testing infrastructure
            caplog: pytest fixture that captures log messages
            monkeypatch: used to alter code
        """

        def f1(f1_smart_thread: SmartThread) -> None:
            """F1 connect routine."""

            algo_app_ver.connect_to_ib(
                timeout_type=timeout_type_arg,
                delay=delay_arg,
                caller="test_algo_app.py::f1:[0-9]+",
                async_tf=async_tf_arg,
            )

            f1_smart_thread.smart_resume(waiters="alpha")

        ################################################################
        # mainline
        ################################################################
        if timeout_type_arg == TimeoutType.TimeoutTrue and delay_arg == 0:
            return

        log_ver = LogVer(log_name="algo_app_test_log")
        algo_app_ver = AlgoAppVer(log_ver=log_ver, app_cat=app_cat)

        mock_ib = MockIB(
            test_cat=test_cat,
            log_ver=log_ver,
            monkeypatch_to_use=monkeypatch,
            group_name=algo_app_ver.algo_group_name,
            algo_name=algo_app_ver.algo_name,
            ip_addr=algo_app_ver.ip_addr,
            port=algo_app_ver.port,
        )

        mock_ib.delay_time = delay_arg

        # we are testing connect_to_ib and the subsequent code that gets
        # control as a result, such as getting the first requestID and
        # then starting a separate thread for the run loop.

        if async_tf_arg:
            smart_thread_name = "alpha"
            alpha_smart_thread = SmartThread(group_name="test1", name=smart_thread_name)
            SmartThread(
                group_name="test1",
                name="beta",
                target_rtn=f1,
                thread_parm_name="f1_smart_thread",
            )

            alpha_smart_thread.smart_wait(resumers="beta")
            alpha_smart_thread.smart_join(targets="beta")
            alpha_smart_thread.smart_unreg()

        else:
            caller = (
                "test_algo_app.py::TestAlgoAppBasicTests."
                "test_mock_connect_to_ib:[0-9]+"
            )
            algo_app_ver.connect_to_ib(
                timeout_type=timeout_type_arg,
                delay=delay_arg,
                caller=caller,
                async_tf=async_tf_arg,
            )

        if not (timeout_type_arg == TimeoutType.TimeoutTrue and delay_arg > 0):
            verify_algo_app_connected(algo_app_ver.algo_app)
            algo_app_ver.disconnect_from_ib(do_algo_disc=True)

        verify_algo_app_disconnected(algo_app_ver.algo_app)

        algo_app_ver.shut_down()
        algo_app_ver.client_disconnect(caller="client.py::EClient.run:[0-9]+")

        ################################################################
        # check log results
        ################################################################
        match_results = log_ver.get_match_results(caplog=caplog)
        log_ver.print_match_results(match_results, print_matched=True)
        log_ver.verify_match_results(match_results)

    ####################################################################
    # test_mock_disconnect_from_ib
    ####################################################################
    @pytest.mark.parametrize("async_tf_arg", [True, False])
    @pytest.mark.parametrize("delay_arg", [0, 2, 4])
    @pytest.mark.parametrize(
        "timeout_type_arg",
        [
            TimeoutType.TimeoutNone,
            TimeoutType.TimeoutFalse,
            TimeoutType.TimeoutTrue,
        ],
    )
    def test_mock_disconnect_from_ib(
        self,
        async_tf_arg: bool,
        delay_arg: int,
        timeout_type_arg: int,
        app_cat: "FileCatalog",
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test disconnecting from IB.

        Args:
            delay_arg: number of seconds to delay
            timeout_type_arg: specifies whether timeout should occur
            app_cat: catalog to use for connect
        """

        def lock_manager(f1_smart_thread: SmartThread):
            log_ver.test_msg("lock_man entry:")
            disc_lock = algo_app.algo_client.disconnect_lock

            # tell lock1 to get lock
            f1_smart_thread.smart_resume(waiters="lock1")

            # wait for lock1 to get lock
            f1_smart_thread.smart_wait(resumers="lock1")

            # tell mainline to do disconnect and get behind lock1
            log_ver.test_msg("lock_man about to resume alpha")
            f1_smart_thread.smart_resume(waiters="alpha")

            # get name of disconnector
            if not async_tf_arg:
                disc_name = "algo_1"
            else:
                while True:
                    with algo_app.request_threads_lock:
                        if algo_app.request_threads:
                            disc_name = list(algo_app.request_threads.keys())[0]
                            break
                    time.sleep(0.01)

            # verify lock1 and disconnector are locked
            log_ver.test_msg("lock_man about to verify locks held 1")
            lock_verify(
                exp_positions=[["lock1", "test1"], [disc_name, algo_group_name]],
                lock=disc_lock,
            )

            # tell lock2 to get lock
            f1_smart_thread.smart_resume(waiters="lock2")

            # verify lock1, disconnector, and lock2 are locked
            lock_verify(
                exp_positions=[
                    ["lock1", "test1"],
                    [disc_name, algo_group_name],
                    ["lock2", "test1"],
                ],
                lock=disc_lock,
            )

            # tell lock1 to drop lock
            f1_smart_thread.smart_resume(waiters="lock1")

            # wait for lock2 to get lock
            f1_smart_thread.smart_wait(resumers="lock2")

            # cause delay condition
            time.sleep(delay_arg)

            # tell lock2 to drop lock
            f1_smart_thread.smart_resume(waiters="lock2")

        def lock_f1(f1_smart_thread: SmartThread):
            f1_smart_thread.smart_wait(resumers="lock_man")
            with sel.SELockExcl(algo_app.algo_client.disconnect_lock):
                f1_smart_thread.smart_resume(waiters="lock_man")
                f1_smart_thread.smart_wait(resumers="lock_man")

        def f1(f1_smart_thread: SmartThread, f1_timeout_type_arg: int):
            disconnect_test(f1_timeout_type_arg)
            f1_smart_thread.smart_resume(waiters="alpha")

        def disconnect_test(ct_timeout_type_arg: int):
            if ct_timeout_type_arg == TimeoutType.TimeoutNone:
                algo_app.disconnect_from_ib()
            elif ct_timeout_type_arg == TimeoutType.TimeoutFalse:
                algo_app.disconnect_from_ib(
                    timeout=delay_arg * 2,
                )
            else:
                # mock_ib.delay_value = -1
                with pytest.raises(DisconnectTimeout):
                    algo_app.disconnect_from_ib(
                        timeout=delay_arg / 2.0,
                    )

        if timeout_type_arg == TimeoutType.TimeoutTrue and delay_arg == 0:
            return

        algo_group_name = "algo_group_1"
        algo_name = "algo_1"
        ip_addr = "127.0.0.1"
        port = AlgoApp.PORT_FOR_LIVE_TRADING

        log_ver = LogVer()

        mock_ib = MockIB(
            test_cat=test_cat,
            log_ver=log_ver,
            monkeypatch_to_use=monkeypatch,
            group_name=algo_group_name,
            algo_name=algo_name,
            ip_addr=ip_addr,
            port=port,
        )

        algo_app = AlgoApp(
            ds_catalog=app_cat,
            group_name=algo_group_name,
            algo_name=algo_name,
        )
        verify_algo_app_initialized(algo_app)

        # we are testing connect_to_ib and the subsequent code that gets
        # control as a result, such as getting the first requestID and
        # then starting a separate thread for the run loop.
        log_ver.test_msg("about to connect")
        algo_app.connect_to_ib(ip_addr=ip_addr, port=port, client_id=1)

        verify_algo_app_connected(algo_app)

        alpha_smart_thread = SmartThread(group_name="test1", name="alpha")

        SmartThread(
            group_name="test1",
            name="lock1",
            target_rtn=lock_f1,
            thread_parm_name="f1_smart_thread",
        )

        SmartThread(
            group_name="test1",
            name="lock2",
            target_rtn=lock_f1,
            thread_parm_name="f1_smart_thread",
        )

        SmartThread(
            group_name="test1",
            name="lock_man",
            target_rtn=lock_manager,
            thread_parm_name="f1_smart_thread",
        )

        log_ver.test_msg("alpha about to wait for lock_man")
        alpha_smart_thread.smart_wait(resumers="lock_man")

        log_ver.test_msg("about to disconnect")

        if async_tf_arg:
            log_ver.test_msg("alpha about to start beta")
            SmartThread(
                group_name="test1",
                name="beta",
                target_rtn=f1,
                thread_parm_name="f1_smart_thread",
                kwargs={"f1_timeout_type_arg": timeout_type_arg},
            )

            alpha_smart_thread.smart_wait(resumers="beta")
            alpha_smart_thread.smart_join(targets="beta")

        else:
            log_ver.test_msg("alpha about to call disconnect_test")
            disconnect_test(timeout_type_arg)

        log_ver.test_msg("back from disconnect")

        if timeout_type_arg == TimeoutType.TimeoutTrue and delay_arg > 0:
            algo_app.disconnect_from_ib()

        verify_algo_app_disconnected(algo_app)

        alpha_smart_thread.smart_join(targets=["lock1", "lock2", "lock_man"])
        alpha_smart_thread.smart_unreg()

        algo_app.shut_down()

        ################################################################
        # check log results
        ################################################################
        match_results = log_ver.get_match_results(caplog=caplog)
        log_ver.print_match_results(match_results, print_matched=True)
        log_ver.verify_match_results(match_results)


########################################################################
# TestAlgoAppConnect class
########################################################################
class TestAlgoAppConnect:
    """TestAlgoAppConnect class."""

    ####################################################################
    # test_connect_scenario
    ####################################################################
    @pytest.mark.parametrize(
        "timeout_type_arg",
        [
            TimeoutType.TimeoutNone,
            TimeoutType.TimeoutFalse,
            TimeoutType.TimeoutTrue,
        ],
    )
    def test_connect_scenario(
        self,
        timeout_type_arg: int,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Any,
    ) -> None:
        """Test meta configuration scenarios.

        Args:
            timeout_type_arg: specifies whether timeout should occur
            caplog: pytest fixture to capture log output
            monkeypatch: pytest monkeypatch

        Notes:
            1) Sync will do various combinations of targets such that
               a partial or full sync of all targets is done

        """
        # async_tf_arg: if True, do async connect
        # delay_arg: number of seconds to delay
        sdparms: list[ScenarioDriverParms] = []
        config_idx = -1
        for async_tf_arg in (True, False):
            for delay_arg in (0, 2, 4):
                if timeout_type_arg == TimeoutType.TimeoutTrue and delay_arg == 0:
                    continue
                config_idx += 1
                args_for_scenario_builder: dict[str, Any] = {
                    "group_name": f"algo_group_{config_idx}",
                    "algo_name": f"algo_name_{config_idx}",
                    "ip_addr": "127.0.0.1",
                    "port": MockIB.PORT_FOR_MOCK_TRADING + config_idx,
                    "client_id": config_idx,
                    "async_tf": async_tf_arg,
                    "delay": delay_arg,
                    "timeout_type": timeout_type_arg,
                    "monkeypatch_to_use": monkeypatch,
                    "tmp_path_to_use": tmp_path,
                }

                sdparms.append(
                    ScenarioDriverParms(
                        scenario_builder=ConfigVerifier.build_connect_scenario,
                        scenario_builder_args=args_for_scenario_builder,
                        commander_config=AppConfig(config_idx % len(AppConfig) + 1),
                        commander_name=f"alpha{config_idx}",
                        group_name=f"test{config_idx}",
                    )
                )

        scenario_driver(
            caplog_to_use=caplog,
            scenario_driver_parms=sdparms,
        )

    ####################################################################
    # test_mock_connect_to_ib_async
    ####################################################################
    @pytest.mark.parametrize("delay_arg", [0, 2, 4])
    @pytest.mark.parametrize(
        "timeout_type_arg",
        [
            TimeoutType.TimeoutNone,
            TimeoutType.TimeoutFalse,
            TimeoutType.TimeoutTrue,
        ],
    )
    def test_mock_connect_to_ib_async(
        self,
        delay_arg: int,
        timeout_type_arg: TimeoutType,
        cat_app: "MockIB",
    ) -> None:
        """Test connecting to IB.

        Args:
            delay_arg: number of seconds to delay
            timeout_type_arg: specifies whether timeout should occur
            cat_app: pytest fixture (see conftest.py)
        """

        def f1(smart_thread: SmartThread, timeout_type_arg: TimeoutType):
            if timeout_type_arg == TimeoutType.TimeoutNone:
                req_num = algo_app.start_async_request(
                    algo_app.connect_to_ib,
                    ip_addr="127.0.0.1",
                    port=algo_app.PORT_FOR_LIVE_TRADING,
                    client_id=1,
                )
            elif timeout_type_arg == TimeoutType.TimeoutFalse:
                timeout_value = delay_arg * 2
                req_num = algo_app.start_async_request(
                    algo_app.connect_to_ib,
                    ip_addr="127.0.0.1",
                    port=algo_app.PORT_FOR_LIVE_TRADING,
                    client_id=1,
                    timeout=timeout_value,
                )
            else:
                # set timeout high so we don't timeout on the actual
                # request - instead, we want to timeout getting the results
                timeout_value = 10
                req_num = algo_app.start_async_request(
                    algo_app.connect_to_ib,
                    ip_addr="127.0.0.1",
                    port=algo_app.PORT_FOR_LIVE_TRADING,
                    client_id=1,
                    timeout=timeout_value,
                )
            assert req_num is not None

        if timeout_type_arg == TimeoutType.TimeoutTrue and delay_arg == 0:
            return

        cat_app.delay_value = delay_arg

        alpha_smart_thread = SmartThread(group_name="test1", name="alpha")
        beta_smart_thread = SmartThread(
            group_name="test1",
            name="beta",
            target_rtn=f1,
            auto_start=False,
            thread_parm_name="smart_thread",
            kwargs={"timeout_type_arg": timeout_type_arg},
        )

        algo_app = AlgoApp(ds_catalog=cat_app.app_cat, algo_name="algo_app")
        verify_algo_app_initialized(algo_app)

        # we are testing connect_to_ib and the subsequent code that gets
        # control as a result, such as getting the first requestID and
        # then starting a separate thread for the run loop.
        logger.debug("about to connect")

        if timeout_type_arg == TimeoutType.TimeoutNone:
            req_result = None
            while req_result is None:
                req_result = algo_app.get_async_results(req_num)
                time.sleep(0.2)
            assert req_result.ret_data is None
        elif timeout_type_arg == TimeoutType.TimeoutFalse:
            req_result = algo_app.get_async_results(req_num, timeout=10)
            assert req_result.ret_data is None
        else:
            with pytest.raises(SmartThreadRequestTimedOut):
                timeout_value = delay_arg / 2.0
                algo_app.get_async_results(req_num, timeout=timeout_value)

            # allow more time for request to complete
            time.sleep(delay_arg + 1)
            req_result = algo_app.get_async_results(req_num, timeout=timeout_value)
            assert req_result.ret_data is None

        verify_algo_app_connected(algo_app)

        cat_app.delay_value = 0  # no delay for disconnect

        algo_app.disconnect_from_ib()
        req_num = algo_app.start_async_request(algo_app.disconnect_from_ib)
        req_result = algo_app.get_async_results(req_num, timeout=10)
        assert req_result.ret_data is None

        verify_algo_app_disconnected(algo_app)

        # algo_app.shut_down()

    ####################################################################
    # test_mock_disconnect_from_ib_async
    ####################################################################
    @pytest.mark.parametrize("delay_arg", [0, 2, 4])
    @pytest.mark.parametrize(
        "timeout_type_arg",
        [
            TimeoutType.TimeoutNone,
            TimeoutType.TimeoutFalse,
            TimeoutType.TimeoutTrue,
        ],
    )
    def test_mock_disconnect_from_ib_async(
        self,
        delay_arg: int,
        timeout_type_arg: int,
        cat_app: "MockIB",
    ) -> None:
        """Test disconnecting from IB.

        Args:
            delay_arg: number of seconds to delay
            timeout_type_arg: specifies whether timeout should occur
            cat_app: pytest fixture (see conftest.py)
        """
        if timeout_type_arg == TimeoutType.TimeoutTrue and delay_arg == 0:
            return

        algo_app = AlgoApp(ds_catalog=cat_app.app_cat, algo_name="algo_app")
        verify_algo_app_initialized(algo_app)

        logger.debug("about to connect")
        algo_app.connect_to_ib(
            ip_addr="127.0.0.1", port=algo_app.PORT_FOR_LIVE_TRADING, client_id=1
        )

        verify_algo_app_connected(algo_app)

        cat_app.delay_value = delay_arg

        if timeout_type_arg == TimeoutType.TimeoutNone:
            req_num = algo_app.start_async_request(algo_app.disconnect_from_ib)
        elif timeout_type_arg == TimeoutType.TimeoutFalse:
            timeout_value = abs(delay_arg) * 2
            req_num = algo_app.start_async_request(
                algo_app.disconnect_from_ib, timeout=timeout_value
            )
        else:
            # set timeout high so we don't timeout on the actual
            # request - instead, we want to timeout getting the results
            timeout_value = 10
            req_num = algo_app.start_async_request(
                algo_app.disconnect_from_ib, timeout=timeout_value
            )

        assert req_num is not None
        if timeout_type_arg == TimeoutType.TimeoutNone:
            req_result = None
            while req_result is None:
                req_result = algo_app.get_async_results(req_num)
                time.sleep(0.2)
            assert req_result.ret_data is None
        elif timeout_type_arg == TimeoutType.TimeoutFalse:
            req_result = algo_app.get_async_results(req_num, timeout=10)
            assert req_result.ret_data is None
        else:
            with pytest.raises(SmartThreadRequestTimedOut):
                timeout_value = delay_arg / 2.0
                algo_app.get_async_results(req_num, timeout=timeout_value)

            # allow more time for request to complete
            time.sleep(delay_arg + 1)
            req_result = algo_app.get_async_results(req_num, timeout=10)
            assert req_result.ret_data is None

        verify_algo_app_disconnected(algo_app)

        # algo_app.shut_down()

    ####################################################################
    # test_connect_to_ib_already_connected
    ####################################################################
    @pytest.mark.parametrize(
        "first_sync_async_arg",
        [
            SyncAsync.RequestSync,
            SyncAsync.RequestAsync,
        ],
    )
    @pytest.mark.parametrize(
        "second_sync_async_arg",
        [
            SyncAsync.RequestSync,
            SyncAsync.RequestAsync,
        ],
    )
    def test_connect_to_ib_already_connected(
        self,
        first_sync_async_arg: SyncAsync,
        second_sync_async_arg: SyncAsync,
        cat_app: "MockIB",
    ) -> None:
        """Test connecting to IB twice.

        Args:
            first_sync_async_arg: specifies how to connect
            second_sync_async_arg: specifies how to connect
            cat_app: pytest fixture (see conftest.py)

        """
        algo_app = AlgoApp(ds_catalog=cat_app.app_cat, algo_name="algo_app")
        verify_algo_app_initialized(algo_app)

        # we are testing connect_to_ib and the subsequent code that gets
        # control as a result, such as getting the first requestID and
        # then starting a separate thread for the run loop.
        logger.debug("about to connect")

        if first_sync_async_arg == SyncAsync.RequestSync:
            algo_app.connect_to_ib(
                ip_addr="127.0.0.1", port=algo_app.PORT_FOR_LIVE_TRADING, client_id=1
            )
        else:
            req_num = algo_app.start_async_request(
                algo_app.connect_to_ib,
                ip_addr="127.0.0.1",
                port=algo_app.PORT_FOR_LIVE_TRADING,
                client_id=1,
            )
            req_result = algo_app.get_async_results(req_num, timeout=10)
            assert req_result.ret_data is None

        # try to connect again - should get error
        if second_sync_async_arg == SyncAsync.RequestSync:
            with pytest.raises(AlreadyConnected):
                algo_app.connect_to_ib(
                    ip_addr="127.0.0.1",
                    port=algo_app.PORT_FOR_LIVE_TRADING,
                    client_id=1,
                )
        else:
            req_num = algo_app.start_async_request(
                algo_app.connect_to_ib,
                ip_addr="127.0.0.1",
                port=algo_app.PORT_FOR_LIVE_TRADING,
                client_id=1,
            )
            with pytest.raises(AlreadyConnected):
                algo_app.get_async_results(req_num, timeout=10)

        # verify that algo_app is still connected
        verify_algo_app_connected(algo_app)

        algo_app.disconnect_from_ib()

        verify_algo_app_disconnected(algo_app)

        # algo_app.shut_down()

    ####################################################################
    # test_connect_to_ib_with_lock_held
    ####################################################################
    def test_connect_to_ib_with_lock_contention(
        self,
        cat_app: "MockIB",
    ) -> None:
        """Test connecting to IB with lock contention.

        Args:
            cat_app: pytest fixture (see conftest.py)

        """
        algo_app = AlgoApp(ds_catalog=cat_app.app_cat, algo_name="algo_app")
        verify_algo_app_initialized(algo_app)
        msgs = Msgs()

        def do_connect1(smart_thread: SmartThread):
            """Do connect to ib.

            Args:
                smart_thread: SmartThread that is running

            """
            logger.debug("do_connect1 entry:")
            # req_num3 = algo_app.start_async_request(
            #     algo_app.connect_to_ib,
            #     ip_addr="127.0.0.1",
            #     port=algo_app.PORT_FOR_LIVE_TRADING,
            #     client_id=1,
            # )
            # msgs.queue_msg(
            #     target="mainline", msg=f"async_{req_num3.req_smart_thread.name}"
            # )
            # req_result3 = algo_app.get_async_results(req_num3, timeout=10)
            # assert req_result3.ret_data is None

            ret_value = algo_app.connect_to_ib(
                ip_addr="127.0.0.1",
                port=algo_app.PORT_FOR_LIVE_TRADING,
                client_id=1,
            )
            assert ret_value is None

            smart_thread.smart_wait(resumers="mainline")

            logger.debug("do_connect1 exit:")

        def do_disconnect2(smart_thread: SmartThread):
            """Do disconnect from ib.

            Args:
                smart_thread: SmartThread that is running

            """
            logger.debug("do_disconnect2 entry:")
            logger.debug("about to disconnect 2")
            # req_num2 = algo_app.start_async_request(algo_app.disconnect_from_ib)
            # msgs.queue_msg(
            #     target="mainline", msg=f"async_{req_num2.req_smart_thread.name}"
            # )
            # req_result2 = algo_app.get_async_results(req_num2, timeout=10)
            # assert req_result2.ret_data is None
            ret_value = algo_app.disconnect_from_ib()
            assert ret_value is None
            smart_thread.smart_wait(resumers="mainline")

            logger.debug("do_disconnect2 exit:")

        def do_connect3(smart_thread: SmartThread):
            """Do connect to ib.

            Args:
                smart_thread: SmartThread that is running

            """
            logger.debug("do_connect3 entry:")
            # req_num3 = algo_app.start_async_request(
            #     algo_app.connect_to_ib,
            #     ip_addr="127.0.0.1",
            #     port=algo_app.PORT_FOR_LIVE_TRADING,
            #     client_id=1,
            # )
            # msgs.queue_msg(
            #     target="mainline", msg=f"async_{req_num3.req_smart_thread.name}"
            # )
            # req_result3 = algo_app.get_async_results(req_num3, timeout=10)
            # assert req_result3.ret_data is None

            ret_value = algo_app.connect_to_ib(
                ip_addr="127.0.0.1",
                port=algo_app.PORT_FOR_LIVE_TRADING,
                client_id=1,
            )
            assert ret_value is None

            smart_thread.smart_wait(resumers="mainline")

            logger.debug("do_connect2 exit:")

        def do_disconnect4(smart_thread: SmartThread):
            """Do disconnect from ib.

            Args:
                smart_thread: SmartThread that is running

            """
            logger.debug("do_disconnect4 entry:")
            # req_num4 = algo_app.start_async_request(algo_app.disconnect_from_ib)
            # msgs.queue_msg(
            #     target="mainline", msg=f"async_{req_num4.req_smart_thread.name}"
            # )
            # req_result4 = algo_app.get_async_results(req_num4, timeout=10)
            # assert req_result4.ret_data is None

            ret_value = algo_app.disconnect_from_ib()
            assert ret_value is None

            smart_thread.smart_resume(waiters="mainline")
            smart_thread.smart_wait(resumers="mainline")

            logger.debug("do_disconnect4 exit:")

        mainline_smart_thread = SmartThread(group_name="test1", name="mainline")

        # we are testing connect_to_ib and the subsequent code that gets
        # control as a result, such as getting the first requestID and
        # then starting a separate thread for the run loop.
        logger.debug("about to connect 1")

        cat_app.delay_value = 1005

        # req_num1 = algo_app.start_async_request(
        #     algo_app.connect_to_ib,
        #     ip_addr="127.0.0.1",
        #     port=algo_app.PORT_FOR_LIVE_TRADING,
        #     client_id=1,
        # )
        conn1_smart_thread = SmartThread(
            group_name="test1",
            name="conn1",
            target_rtn=do_connect1,
            thread_parm_name="smart_thread",
        )
        smart_threads: list[SmartThread] = []

        time.sleep(0.5)
        smart_threads = SmartThread.find_smart_threads(
            search_thread=conn1_smart_thread.thread
        )
        logger.debug(f"SBT {smart_threads=}")
        name1 = ""
        for st in smart_threads:
            if st.name != "conn1":
                name1 = st.name
                break

        lock_verify([name1])

        # logger.debug("about to disconnect 1")
        # req_num2 = algo_app.start_async_request(algo_app.disconnect_from_ib)
        logger.debug("about to disconnect 2")
        disc2_smart_thread = SmartThread(
            group_name="test1",
            name="disc2",
            target_rtn=do_disconnect2,
            thread_parm_name="smart_thread",
        )
        time.sleep(0.5)
        smart_threads = SmartThread.find_smart_threads(
            search_thread=disc2_smart_thread.thread
        )
        name2 = ""
        for st in smart_threads:
            if st.name != "disc2":
                name2 = st.name
                break

        lock_verify([name1, name2])

        logger.debug("about to connect 2")
        # req_num3 = algo_app.start_async_request(
        #     algo_app.connect_to_ib,
        #     ip_addr="127.0.0.1",
        #     port=algo_app.PORT_FOR_LIVE_TRADING,
        #     client_id=1,
        # )
        conn3_smart_thread = SmartThread(
            group_name="test1",
            name="conn3",
            target_rtn=do_connect3,
            thread_parm_name="smart_thread",
        )
        time.sleep(0.5)
        smart_threads = SmartThread.find_smart_threads(
            search_thread=conn3_smart_thread.thread
        )
        name3 = ""
        for st in smart_threads:
            if st.name != "conn3":
                name3 = st.name
                break

        lock_verify([name1, name2, name3])

        logger.debug("about to disconnect 2")
        # req_num4 = algo_app.start_async_request(algo_app.disconnect_from_ib)
        disc4_smart_thread = SmartThread(
            group_name="test1",
            name="disc4",
            target_rtn=do_disconnect4,
            thread_parm_name="smart_thread",
        )
        time.sleep(0.5)
        smart_threads = SmartThread.find_smart_threads(
            search_thread=disc4_smart_thread.thread
        )
        name4 = ""
        for st in smart_threads:
            if st.name != "disc4":
                name4 = st.name
                break

        lock_verify([name1, name2, name3, name4])

        # req_result1 = algo_app.get_async_results(req_num1, timeout=10)
        # assert req_result1.ret_data is None

        # req_result2 = algo_app.get_async_results(req_num2, timeout=10)
        # assert req_result2.ret_data is None

        # req_result3 = algo_app.get_async_results(req_num3, timeout=10)
        # assert req_result3.ret_data is None
        #
        # req_result4 = algo_app.get_async_results(req_num4, timeout=10)
        # assert req_result4.ret_data is None

        mainline_smart_thread.smart_wait(resumers="disc4")

        verify_algo_app_disconnected(algo_app)

        # algo_app.shut_down()

        mainline_smart_thread.smart_resume(waiters=["conn1", "disc2", "conn3", "disc4"])
        mainline_smart_thread.smart_join(targets=["conn1", "disc2", "conn3", "disc4"])

    @pytest.mark.skip(reason="enable only when Trader Workstation is open")
    def test_real_connect_to_ib(self) -> None:
        """Test connecting to IB."""
        proj_dir = Path.cwd().resolve().parents[1]  # back two directories
        test_cat = FileCatalog({"symbols": Path(proj_dir / "t_datasets/symbols.csv")})
        algo_app = AlgoApp(test_cat)
        verify_algo_app_initialized(algo_app)

        # we are testing connect_to_ib and the subsequent code that gets
        # control as a result, such as getting the first requestID and then
        # starting a separate thread for the run loop.
        logger.debug("about to connect")
        connect_ans = algo_app.connect_to_ib("127.0.0.1", 7496, client_id=0)

        # verify that algo_app is connected and alive with a valid reqId
        assert connect_ans
        assert algo_app.ibapi_client_smart_thread.thread.is_alive()
        assert algo_app.isConnected()
        assert algo_app.request_id == 1

        algo_app.disconnect_from_ib()
        assert not algo_app.ibapi_client_smart_thread.thread.is_alive()
        assert not algo_app.isConnected()


########################################################################
# ExpCounts
########################################################################
class ExpCounts(NamedTuple):
    """NamedTuple for the expected counts."""

    sym_non_recursive: int
    sym_recursive: int
    stock_sym_non_recursive: int
    stock_sym_recursive: int


########################################################################
# SymDfs
########################################################################
class SymDfs:
    """Saved sym dfs."""

    def __init__(
        self, mock_sym_df: Any, sym_df: Any, mock_stock_sym_df: Any, stock_sym_df: Any
    ) -> None:
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


########################################################################
# TestAlgoAppMatchingSymbols
########################################################################
class TestAlgoAppMatchingSymbols:
    """TestAlgoAppMatchingSymbols class."""

    ####################################################################
    # test_request_symbols_all_combos
    ####################################################################
    def test_request_symbols_all_combos(
        self,
        cat_app: "FileCatalog",
        mock_ib: Any,
    ) -> None:
        """Test request_symbols with all patterns.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see
                conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        logger.debug("about to connect")
        algo_app.connect_to_ib("127.0.0.1", algo_app.PORT_FOR_LIVE_TRADING, client_id=0)
        verify_algo_app_connected(algo_app)
        algo_app.request_throttle_secs = 0.01

        try:
            for idx, search_pattern in enumerate(mock_ib.search_patterns()):
                exp_counts = get_exp_counts(search_pattern, mock_ib)
                # verify symbol table has zero entries for the symbol
                logger.info(
                    "calling verify_match_symbols req_type 1 " "sym %s num %d",
                    search_pattern,
                    idx,
                )
                algo_app.market_data.symbols = pd.DataFrame()
                algo_app.market_data.stock_symbols = pd.DataFrame()
                verify_match_symbols(
                    algo_app, mock_ib, search_pattern, exp_counts=exp_counts, req_type=1
                )

                logger.info(
                    "calling verify_match_symbols req_type 2 " "sym %s num %d",
                    search_pattern,
                    idx,
                )
                algo_app.market_data.symbols = pd.DataFrame()
                algo_app.market_data.stock_symbols = pd.DataFrame()
                verify_match_symbols(
                    algo_app, mock_ib, search_pattern, exp_counts=exp_counts, req_type=2
                )
        finally:
            logger.debug("disconnecting")
            algo_app.disconnect_from_ib()
            logger.debug("verifying disconnected")
            verify_algo_app_disconnected(algo_app)
            logger.debug("disconnected - test case returning")

    ####################################################################
    # test_request_symbols_zero_result
    ####################################################################
    def test_request_symbols_zero_result(
        self, algo_app: "AlgoApp", mock_ib: Any
    ) -> None:
        """Test request_symbols with pattern that finds exactly 1 symbol.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)
        logger.debug("about to connect")
        algo_app.connect_to_ib("127.0.0.1", algo_app.PORT_FOR_LIVE_TRADING, client_id=0)
        verify_algo_app_connected(algo_app)
        algo_app.request_throttle_secs = 0.01

        try:
            exp_counts = ExpCounts(0, 0, 0, 0)

            # verify symbol table has zero entries for the symbols
            for idx, search_pattern in enumerate(mock_ib.no_find_search_patterns()):
                logger.info(
                    "calling verify_match_symbols req_type 1 " "sym %s num %d",
                    search_pattern,
                    idx,
                )
                verify_match_symbols(
                    algo_app, mock_ib, search_pattern, exp_counts=exp_counts, req_type=1
                )

                logger.info(
                    "calling verify_match_symbols req_type 2 " "sym %s num %d",
                    search_pattern,
                    idx,
                )
                verify_match_symbols(
                    algo_app, mock_ib, search_pattern, exp_counts=exp_counts, req_type=2
                )

        finally:
            logger.debug("disconnecting")
            algo_app.disconnect_from_ib()
            logger.debug("verifying disconnected")
            verify_algo_app_disconnected(algo_app)
            logger.debug("disconnected - test case returning")

    ####################################################################
    # test_get_symbols_timeout
    ####################################################################
    def test_get_symbols_timeout(self, algo_app: "AlgoApp", mock_ib: Any) -> None:
        """Test get_symbols gets timeout.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)
        try:
            logger.debug("about to connect")
            algo_app.connect_to_ib(
                "127.0.0.1", mock_ib.PORT_FOR_SIMULATE_REQUEST_TIMEOUT, client_id=0
            )
            verify_algo_app_connected(algo_app)

            with pytest.raises(RequestTimeout):
                algo_app.request_symbols("A")

        finally:
            logger.debug("disconnecting")
            algo_app.disconnect_from_ib()
            logger.debug("verifying disconnected")
            verify_algo_app_disconnected(algo_app)
            logger.debug("disconnected - test case returning")

    ####################################################################
    # test_get_symbols_disconnect
    ####################################################################
    def test_get_symbols_disconnect(self, algo_app: "AlgoApp", mock_ib: Any) -> None:
        """Test get_symbols gets disconnected while waiting.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)
        try:
            logger.debug("about to connect")
            algo_app.connect_to_ib(
                "127.0.0.1", mock_ib.PORT_FOR_SIMULATE_REQUEST_DISCONNECT, client_id=0
            )
            verify_algo_app_connected(algo_app)

            with pytest.raises(DisconnectDuringRequest):
                algo_app.request_symbols("A")

        finally:
            logger.debug("disconnecting")
            algo_app.disconnect_from_ib()
            logger.debug("verifying disconnected")
            verify_algo_app_disconnected(algo_app)
            logger.debug("disconnected - test case returning")

    ####################################################################
    # test_get_symbols
    ####################################################################
    def test_get_symbols(self, algo_app: "AlgoApp", mock_ib: Any) -> None:
        """Test get_symbols with pattern that finds no symbols.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)
        try:
            logger.debug("about to connect")
            algo_app.connect_to_ib(
                "127.0.0.1", algo_app.PORT_FOR_LIVE_TRADING, client_id=0
            )
            verify_algo_app_connected(algo_app)
            algo_app.request_throttle_secs = 0.01

            sym_dfs = SymDfs(
                pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            )
            # full_stock_sym_match_descs = pd.DataFrame()
            # stock_symbols_ds = pd.DataFrame()
            # full_sym_match_descs = pd.DataFrame()
            # symbols_ds = pd.DataFrame()
            # we need to loop from A to Z
            for letter in string.ascii_uppercase:
                logger.debug("about to verify_get_symbols for letter %s", letter)
                # full_stock_sym_match_descs, stock_symbols_ds,\
                #     full_sym_match_descs, symbols_ds = \
                sym_dfs = verify_get_symbols(letter, algo_app, mock_ib, sym_dfs)

        finally:
            logger.debug("disconnecting")
            algo_app.disconnect_from_ib()
            logger.debug("verifying disconnected")
            verify_algo_app_disconnected(algo_app)
            logger.debug("disconnected - test case returning")

    ####################################################################
    # test_get_symbols_with_connect_disconnect
    ####################################################################
    def test_get_symbols_with_connect_disconnect(
        self, algo_app: "AlgoApp", mock_ib: Any
    ) -> None:
        """Test get_symbols with pattern that finds no symbols.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        sym_dfs = SymDfs(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
        # full_stock_sym_match_descs = pd.DataFrame()
        # full_sym_match_descs = pd.DataFrame()
        # stock_symbols_ds = pd.DataFrame()
        # symbols_ds = pd.DataFrame()
        # we need to loop from A to Z
        for letter in string.ascii_uppercase:
            try:
                logger.debug("about to connect")
                algo_app.connect_to_ib(
                    "127.0.0.1", algo_app.PORT_FOR_LIVE_TRADING, client_id=0
                )
                verify_algo_app_connected(algo_app)
                algo_app.request_throttle_secs = 0.01

                logger.debug("about to verify_get_symbols for letter %s", letter)
                # full_stock_sym_match_descs, stock_symbols_ds, \
                #     full_sym_match_descs, symbols_ds = \
                sym_dfs = verify_get_symbols(letter, algo_app, mock_ib, sym_dfs)

            finally:
                logger.debug("disconnecting")
                algo_app.disconnect_from_ib()
                logger.debug("verifying disconnected")
                verify_algo_app_disconnected(algo_app)


###############################################################################
# verify_match_symbols
###############################################################################
def verify_match_symbols(
    algo_app: "AlgoApp",
    mock_ib: Any,
    pattern: str,
    exp_counts: ExpCounts,
    req_type: int = 1,
) -> None:
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
        # algo_app.market_data.stock_symbols.drop_duplicates(inplace=True)

    logger.debug("getting stock_sym_match_descs")
    symbol_starts_with_pattern = mock_ib.contract_descriptions["symbol"].map(
        lambda symbol: symbol.startswith(pattern)
    )
    stock_sym_match_descs = mock_ib.contract_descriptions.loc[
        symbol_starts_with_pattern
        & (mock_ib.contract_descriptions["secType"] == "STK")
        & (mock_ib.contract_descriptions["currency"] == "USD")
        & (if_opt_in_derivativeSecTypes(mock_ib.contract_descriptions)),
        [
            "conId",
            "symbol",
            "secType",
            "primaryExchange",
            "currency",
            "derivativeSecTypes",
        ],
    ]

    sym_match_descs = mock_ib.contract_descriptions.loc[
        symbol_starts_with_pattern
        & (
            (mock_ib.contract_descriptions["secType"] != "STK")
            | (mock_ib.contract_descriptions["currency"] != "USD")
            | if_opt_not_in_derivativeSecTypes(mock_ib.contract_descriptions)
        ),
        [
            "conId",
            "symbol",
            "secType",
            "primaryExchange",
            "currency",
            "derivativeSecTypes",
        ],
    ]

    logger.debug("verifying results counts")

    if req_type == 1:
        assert (
            len(algo_app.market_data.stock_symbols)
            == exp_counts.stock_sym_non_recursive
        )
        assert len(algo_app.market_data.symbols) == exp_counts.sym_non_recursive
        assert len(stock_sym_match_descs) == exp_counts.stock_sym_recursive
        assert len(sym_match_descs) == exp_counts.sym_recursive
    else:
        assert len(algo_app.market_data.stock_symbols) == exp_counts.stock_sym_recursive
        assert len(algo_app.market_data.symbols) == exp_counts.sym_recursive
        assert len(stock_sym_match_descs) == exp_counts.stock_sym_recursive
        assert len(sym_match_descs) == exp_counts.sym_recursive

    logger.debug("verifying results match DataFrame")
    if exp_counts.stock_sym_recursive > 0:
        if req_type == 1:
            stock_sym_match_descs = stock_sym_match_descs.iloc[
                0 : exp_counts.stock_sym_non_recursive
            ]
        stock_sym_match_descs = stock_sym_match_descs.set_index(["conId"]).sort_index()

        algo_app.market_data.stock_symbols.sort_index(inplace=True)
        comp_df = algo_app.market_data.stock_symbols.compare(stock_sym_match_descs)
        assert comp_df.empty

    if exp_counts.sym_recursive > 0:
        if req_type == 1:
            sym_match_descs = sym_match_descs.iloc[0 : exp_counts.sym_non_recursive]
        sym_match_descs = sym_match_descs.set_index(["conId"]).sort_index()

        algo_app.market_data.symbols.sort_index(inplace=True)
        comp_df = algo_app.market_data.symbols.compare(sym_match_descs)
        # logger.debug(f'{algo_app.market_data.symbols=}')
        # logger.debug(f'{sym_match_descs=}')
        # logger.debug(f'{comp_df=}')
        assert comp_df.empty
    logger.debug("all results verified for req_type %d", req_type)


########################################################################
# if_opt_in_derivativeSecTypes
########################################################################
def if_opt_in_derivativeSecTypes(df: Any) -> Any:
    """Find the symbols that have options.

    Args:
        df: pandas DataFrame of symbols

    Returns:
          array of boolean values used in pandas loc function

    """
    ret_array = np.full(len(df), False)
    for i in range(len(df)):
        if "OPT" in df.iloc[i].derivativeSecTypes:
            ret_array[i] = True
    return ret_array


########################################################################
# if_opt_not_in_derivativeSecTypes
########################################################################
def if_opt_not_in_derivativeSecTypes(df: Any) -> Any:
    """Find the symbols that do not have options.

    Args:
        df: pandas DataFrame of symbols

    Returns:
          array of boolean values used in pandas loc function

    """
    ret_array = np.full(len(df), True)
    for i in range(len(df)):
        if "OPT" in df.iloc[i].derivativeSecTypes:
            ret_array[i] = False
    return ret_array


########################################################################
# get_exp_counts
########################################################################
def get_exp_counts(search_pattern: str, mock_ib: Any) -> ExpCounts:
    """Helper function to get number of expected symbols.

    Args:
        search_pattern: search arg as string of one or more chars
        mock_ib: mock of ib

    Returns:
        number of expected matches for recursive and non-recursive requests
    """
    combo_factor = 1 + 3 + 3**2 + 3**3
    if len(search_pattern) > 4:
        # 5 or more chars will never match (for our mock setup)
        return ExpCounts(0, 0, 0, 0)
    if search_pattern[0] not in string.ascii_uppercase[0:17]:
        return ExpCounts(0, 0, 0, 0)  # not in A-Q, inclusive
    if len(search_pattern) >= 2:
        if search_pattern[1] not in string.ascii_uppercase[1:3] + ".":
            return ExpCounts(0, 0, 0, 0)  # not in 'BC.'
        combo_factor = 1 + 3 + 3**2
    if len(search_pattern) >= 3:
        if search_pattern[2] not in string.ascii_uppercase[2:5]:
            return ExpCounts(0, 0, 0, 0)  # not in 'CDE'
        combo_factor = 1 + 3
    if len(search_pattern) == 4:
        if search_pattern[3] not in string.ascii_uppercase[3:5] + ".":
            return ExpCounts(0, 0, 0, 0)  # not in 'DE.'
        combo_factor = 1

    num_stock_sym_combos = 0
    num_sym_combos = 0
    combo = mock_ib.get_combos(search_pattern[0])

    for item in combo:
        if item[0] == "STK" and item[2] == "USD" and "OPT" in item[3]:
            num_stock_sym_combos += 1
        else:
            num_sym_combos += 1
    exp_stock_sym_recursive = num_stock_sym_combos * combo_factor
    exp_sym_recursive = num_sym_combos * combo_factor
    exp_stock_sym_non_recursive = math.ceil(
        min(16, len(combo) * combo_factor) * (num_stock_sym_combos / len(combo))
    )
    exp_sym_non_recursive = math.floor(
        min(16, len(combo) * combo_factor) * (num_sym_combos / len(combo))
    )

    return ExpCounts(
        exp_sym_non_recursive,
        exp_sym_recursive,
        exp_stock_sym_non_recursive,
        exp_stock_sym_recursive,
    )


########################################################################
# verify_get_symbols
########################################################################
def verify_get_symbols(
    letter: str, algo_app: "AlgoApp", mock_ib: Any, sym_dfs: SymDfs
) -> SymDfs:
    """Verify get_symbols.

    Args:
        letter: the single letter we are collecting symbols for
        algo_app: instance of AlgoApp from conftest pytest fixture
        mock_ib: pytest fixture of contract_descriptions
        sym_dfs: saved DataFrames between calls

    Returns:
        updated sym_dfs

    """
    if letter != "A":
        # verify the symbol_status ds
        symbols_status_path = algo_app.ds_catalog.get_path("symbols_status")
        logger.info("symbols_status_path: %s", symbols_status_path)

        assert symbols_status_path.exists()
        symbols_status = pd.read_csv(symbols_status_path, header=0, index_col=0)
        test_letter = symbols_status.iloc[0, 0]
        assert test_letter == letter

    exp_counts = get_exp_counts(letter, mock_ib)
    logger.debug("about to get_symbols for %s", letter)
    algo_app.get_symbols()
    assert algo_app.request_id >= 2

    logger.debug("getting stock_sym_match_descs for %s", letter)
    symbol_starts_with_pattern = mock_ib.contract_descriptions["symbol"].map(
        lambda symbol: symbol.startswith(letter)
    )
    stock_sym_match_descs = mock_ib.contract_descriptions.loc[
        symbol_starts_with_pattern
        & (mock_ib.contract_descriptions["secType"] == "STK")
        & (mock_ib.contract_descriptions["currency"] == "USD")
        & (if_opt_in_derivativeSecTypes(mock_ib.contract_descriptions)),
        [
            "conId",
            "symbol",
            "secType",
            "primaryExchange",
            "currency",
            "derivativeSecTypes",
        ],
    ]

    sym_match_descs = mock_ib.contract_descriptions.loc[
        symbol_starts_with_pattern
        & (
            (mock_ib.contract_descriptions["secType"] != "STK")
            | (mock_ib.contract_descriptions["currency"] != "USD")
            | if_opt_not_in_derivativeSecTypes(mock_ib.contract_descriptions)
        ),
        [
            "conId",
            "symbol",
            "secType",
            "primaryExchange",
            "currency",
            "derivativeSecTypes",
        ],
    ]
    # we expect the stock_symbols to accumulate and grow, so the
    # number should now be what was there from the previous
    # iteration of this loop plus what we just now added
    assert len(stock_sym_match_descs) == exp_counts.stock_sym_recursive
    assert len(algo_app.market_data.stock_symbols) == (
        exp_counts.stock_sym_recursive + len(sym_dfs.stock_sym_df)
    )

    assert len(sym_match_descs) == exp_counts.sym_recursive
    assert len(algo_app.market_data.symbols) == (
        exp_counts.sym_recursive + len(sym_dfs.sym_df)
    )

    if exp_counts.stock_sym_recursive > 0:
        stock_sym_match_descs = stock_sym_match_descs.set_index(["conId"]).sort_index()
        # sym_dfs.mock_stock_sym_df \
        #     = sym_dfs.mock_stock_sym_df.append(stock_sym_match_descs)
        sym_dfs.mock_stock_sym_df = pd.concat(
            [sym_dfs.mock_stock_sym_df, stock_sym_match_descs]
        )
        sym_dfs.mock_stock_sym_df.sort_index(inplace=True)

        # check the data set
        stock_symbols_path = algo_app.ds_catalog.get_path("stock_symbols")
        logger.info("stock_symbols_path: %s", stock_symbols_path)

        sym_dfs.stock_sym_df = pd.read_csv(
            stock_symbols_path,
            header=0,
            index_col=0,
            converters={"derivativeSecTypes": lambda x: eval(x)},
        )
        comp_df = algo_app.market_data.stock_symbols.compare(sym_dfs.stock_sym_df)
        assert comp_df.empty

        comp_df = algo_app.market_data.stock_symbols.compare(sym_dfs.mock_stock_sym_df)
        assert comp_df.empty

    if exp_counts.sym_recursive > 0:
        sym_match_descs = sym_match_descs.set_index(["conId"]).sort_index()
        # sym_dfs.mock_sym_df = \
        #     sym_dfs.mock_sym_df.append(sym_match_descs)
        sym_dfs.mock_sym_df = pd.concat([sym_dfs.mock_sym_df, sym_match_descs])
        sym_dfs.mock_sym_df.sort_index(inplace=True)

        # check the data set
        symbols_path = algo_app.ds_catalog.get_path("symbols")
        logger.info("symbols_path: %s", symbols_path)

        sym_dfs.sym_df = pd.read_csv(
            symbols_path,
            header=0,
            index_col=0,
            converters={"derivativeSecTypes": lambda x: eval(x)},
        )

        comp_df = algo_app.market_data.symbols.compare(sym_dfs.sym_df)
        assert comp_df.empty

        comp_df = algo_app.market_data.symbols.compare(sym_dfs.mock_sym_df)
        assert comp_df.empty

    return sym_dfs


########################################################################
# TestErrorPath
########################################################################
class TestErrorPath:
    """Class to test error path."""

    ####################################################################
    # test_error_path_by_request_when_not_connected
    ####################################################################
    def test_error_path_by_request_when_not_connected(
        self, algo_app: "AlgoApp", capsys: Any
    ) -> None:
        """Test the error callback by any request while not connected.

        Args:
            algo_app: instance of AlgoApp from conftest pytest fixture
            capsys: pytest fixture to capture print output

        """
        verify_algo_app_initialized(algo_app)
        logger.debug("verifying disconnected")
        verify_algo_app_disconnected(algo_app)

        logger.debug("about to request time")
        algo_app.reqCurrentTime()
        captured = capsys.readouterr().out
        assert captured == "Error:  -1   504   Not connected" + "\n"


########################################################################
# TestAlgoAppContractDetails
########################################################################
class TestAlgoAppContractDetails:
    """TestAlgoAppContractDetails class."""

    ####################################################################
    # test_get_contract_details_0_entries
    ####################################################################
    def test_get_contract_details_0_entries(
        self, algo_app: "AlgoApp", mock_ib: Any
    ) -> None:
        """Test contract details for non-existent conId.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        logger.debug("about to connect")
        algo_app.connect_to_ib("127.0.0.1", algo_app.PORT_FOR_LIVE_TRADING, client_id=0)

        # verify that algo_app is connected and alive with a valid reqId
        verify_algo_app_connected(algo_app)

        contract = Contract()  # create an empty contract with conId of 0
        algo_app.get_contract_details(contract)

        verify_contract_details(contract, algo_app, mock_ib, [])

        algo_app.disconnect_from_ib()
        verify_algo_app_disconnected(algo_app)

    ####################################################################
    # test_get_contract_details_1_entry
    ####################################################################
    def test_get_contract_details_1_entry(
        self, algo_app: "AlgoApp", mock_ib: Any
    ) -> None:
        """Test contract details for 1 entry.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        logger.debug("about to connect")
        algo_app.connect_to_ib("127.0.0.1", algo_app.PORT_FOR_LIVE_TRADING, client_id=0)

        # verify that algo_app is connected and alive with a valid reqId
        verify_algo_app_connected(algo_app)

        contract = Contract()  # create an empty contract with conId of 0
        contract.conId = 7001
        algo_app.get_contract_details(contract)

        verify_contract_details(contract, algo_app, mock_ib, [7001])

        algo_app.disconnect_from_ib()
        verify_algo_app_disconnected(algo_app)

    ####################################################################
    # test_get_contract_details_2_entries
    ####################################################################
    def test_get_contract_details_2_entries(
        self, algo_app: "AlgoApp", mock_ib: Any
    ) -> None:
        """Test contract details for 2 entries.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        logger.debug("about to connect")
        algo_app.connect_to_ib("127.0.0.1", algo_app.PORT_FOR_LIVE_TRADING, client_id=0)

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

    ####################################################################
    # test_get_contract_details_duplicates
    ####################################################################
    def test_get_contract_details_duplicates(
        self, algo_app: "AlgoApp", mock_ib: Any
    ) -> None:
        """Test contract details for 3 entries plus a duplicate.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        logger.debug("about to connect")
        algo_app.connect_to_ib("127.0.0.1", algo_app.PORT_FOR_LIVE_TRADING, client_id=0)

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

    ####################################################################
    # test_get_contract_details_many_entries
    ####################################################################
    def test_get_contract_details_many_entries(
        self, algo_app: "AlgoApp", mock_ib: Any
    ) -> None:
        """Test contract details for many entries.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        verify_algo_app_initialized(algo_app)

        logger.debug("about to connect")
        algo_app.connect_to_ib("127.0.0.1", algo_app.PORT_FOR_LIVE_TRADING, client_id=0)

        # verify that algo_app is connected and alive with a valid reqId
        verify_algo_app_connected(algo_app)

        try:
            conId_list = []
            for conId in range(7001, 7033):
                contract = Contract()  # create an empty contract
                contract.conId = conId
                conId_list.append(conId)
                algo_app.get_contract_details(contract)

                verify_contract_details(contract, algo_app, mock_ib, conId_list)
        finally:
            algo_app.disconnect_from_ib()
            verify_algo_app_disconnected(algo_app)


########################################################################
# verify_contract_details
########################################################################
def verify_contract_details(
    contract: "Contract", algo_app: "AlgoApp", mock_ib: Any, conId_list: list[int]
) -> None:
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
        contracts_path = algo_app.ds_catalog.get_path("contracts")
        logger.info("contracts_path: %s", contracts_path)
        algo_app.contracts = algo_app.load_contracts(contracts_path)
        algo_app.load_contract_details()

        # print('contract_details_ds:\n', contract_details_ds)
        # print('contract_details_ds.__dict__:\n',
        #       contract_details_ds.__dict__)

        for conId in conId_list:
            # match_desc = mock_ib.contract_descriptions.loc[
            #     mock_ib.contract_descriptions['conId'] == conId]

            # match_desc = match_desc.iloc[0]

            contract1 = get_contract_obj(algo_app.contracts.loc[conId].to_dict())

            contract2 = get_contract_obj(contracts_ds.loc[conId].to_dict())

            compare_contracts(contract1, contract2)

            contract3 = get_contract_from_mock_desc(conId, mock_ib)

            compare_contracts(contract1, contract3)

            contract_details1 = get_contract_details_obj(
                algo_app.contract_details.loc[conId].to_dict()
            )

            contract_details2 = get_contract_details_obj(
                contract_details_ds.loc[conId].to_dict()
            )

            compare_contract_details(contract_details1, contract_details2)

            contract_details3 = get_contract_details_from_mock_desc(conId, mock_ib)

            compare_contract_details(contract_details1, contract_details3)


###############################################################################
###############################################################################
# TestExtraContractFields
###############################################################################
###############################################################################
class TestExtraContractFields:
    """TestExtraContractFields class."""

    ###########################################################################
    # test_contract_extra_fields
    ###########################################################################
    def test_contract_extra_fields(self, algo_app: "AlgoApp", mock_ib: Any) -> None:
        """Test combo legs in contract.

        Args:
            algo_app: pytest fixture instance of AlgoApp (see conftest.py)
            mock_ib: pytest fixture of contract_descriptions

        """
        num_contracts = 50
        contract_list = []
        contract_df = pd.DataFrame()
        # get the path for saving/loading the combo legs contract df
        extra_contract_path = algo_app.ds_catalog.get_path("extra_contract")
        logger.info("extra_contract_path: %s", extra_contract_path)

        for i in range(num_contracts):
            conId = 7001 + i
            contract = get_contract_from_mock_desc(
                conId, mock_ib, include_extra_details=True
            )

            # add combo legs
            combo_leg_list = build_combo_legs(i, mock_ib)
            if combo_leg_list:
                contract.comboLegs = combo_leg_list
            elif i % 2 == 1:  # empty list
                # empty list for odd, None for even
                contract.comboLegs = []

            contract_list.append(contract)
            contract_dict = get_contract_dict(contract)
            # contract_df = \
            #     contract_df.append(pd.DataFrame(contract_dict,
            #                                     index=[contract.conId]))
            contract_df = pd.concat(
                [contract_df, pd.DataFrame(contract_dict, index=[contract.conId])]
            )

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
def build_combo_legs(idx: int, mock_ib: Any) -> list[ComboLeg]:
    """Build the combo leg list for a contract.

    Args:
        idx: the index of the entry being built
        mock_ib: pytest fixture of contract_descriptions

    Returns:
        list with zero or more ComboLeg items

    """
    num_combo_legs = idx % 4  # vary the number built from 0 to 3
    combo_leg_list = []
    for j in range(num_combo_legs):
        combo_leg = ComboLeg()
        combo_leg.conId = mock_ib.combo_legs.cl_conId.iloc[idx + j]
        combo_leg.ratio = mock_ib.combo_legs.cl_ratio.iloc[idx + j]
        combo_leg.action = mock_ib.combo_legs.cl_action.iloc[idx + j]
        combo_leg.exchange = mock_ib.combo_legs.cl_exchange.iloc[idx + j]
        combo_leg.openClose = mock_ib.combo_legs.cl_openClose.iloc[idx + j]
        combo_leg.shortSaleSlot = mock_ib.combo_legs.cl_shortSaleSlot.iloc[idx + j]
        combo_leg.designatedLocation = mock_ib.combo_legs.cl_designatedLocation.iloc[
            idx + j
        ]
        combo_leg.exemptCode = mock_ib.combo_legs.cl_exemptCode.iloc[idx + j]

        combo_leg_list.append(combo_leg)

    return combo_leg_list


###############################################################################
# get_contract_from_mock_desc
###############################################################################
def get_contract_from_mock_desc(
    conId: int, mock_ib: Any, include_extra_details: bool = False
) -> Contract:
    """Build and return a contract from the mock description.

    Args:
        conId: index of mock_desc and mock_dnc to use
        mock_ib: contains contract data frames
        include_extra_details: include more details beyond what is
                                 returned for reqContractDetails

    Returns:
          Contract with fields from input mock_desc and mock_dnc

    """
    ret_con = Contract()
    ret_con.conId = mock_ib.contract_descriptions.at[conId, "conId"]  # cd
    ret_con.symbol = mock_ib.contract_descriptions.at[conId, "symbol"]  # cd
    ret_con.secType = mock_ib.contract_descriptions.at[conId, "secType"]  # cd

    if mock_ib.contract_descriptions.at[conId, "lastTradeDateOrContractMonth"]:
        split_date = mock_ib.contract_descriptions.at[
            conId, "lastTradeDateOrContractMonth"
        ].split()
        if len(split_date) > 0:  # very well better be!
            ret_con.lastTradeDateOrContractMonth = split_date[0]

    ret_con.strike = mock_ib.contract_descriptions.at[conId, "strike"]  # cd
    ret_con.right = mock_ib.contract_descriptions.at[conId, "right"]  # cd
    ret_con.multiplier = mock_ib.contract_descriptions.at[conId, "multiplier"]  # cd
    ret_con.exchange = mock_ib.contract_descriptions.at[conId, "exchange"]  # cd
    ret_con.primaryExchange = mock_ib.contract_descriptions.at[
        conId, "primaryExchange"
    ]  # cd
    ret_con.currency = mock_ib.contract_descriptions.at[conId, "currency"]  # cd
    ret_con.localSymbol = mock_ib.contract_descriptions.at[conId, "localSymbol"]  # cd
    ret_con.tradingClass = mock_ib.contract_descriptions.at[conId, "tradingClass"]  # cd

    ###########################################################################
    # following fields are not included with reqContractDetails
    ###########################################################################
    if include_extra_details:
        ret_con.includeExpired = mock_ib.contract_descriptions.at[
            conId, "includeExpired"
        ]
        ret_con.secIdType = mock_ib.contract_descriptions.at[conId, "secIdType"]
        ret_con.secId = mock_ib.contract_descriptions.at[conId, "secId"]

        # combos
        ret_con.comboLegsDescrip = mock_ib.contract_descriptions.at[
            conId, "comboLegsDescrip"
        ]
        # ret_con.comboLegs = mock_ib.contract_descriptions.comboLegs

        # build a delta_neutral_contract every third time
        if (conId % 3) == 0:
            delta_neutral_contract = DeltaNeutralContract()
            # item() is used to convert numpy.int64 to python int
            delta_neutral_contract.conId = mock_ib.delta_neutral_contract.at[
                conId, "conId"
            ]
            delta_neutral_contract.delta = mock_ib.delta_neutral_contract.at[
                conId, "delta"
            ]
            delta_neutral_contract.price = mock_ib.delta_neutral_contract.at[
                conId, "price"
            ]

            ret_con.deltaNeutralContract = delta_neutral_contract

    return ret_con


###############################################################################
# get_contract_details_from_mock_desc
###############################################################################
def get_contract_details_from_mock_desc(conId: int, mock_ib: Any) -> ContractDetails:
    """Build and return a contract_details from the mock description.

    Args:
        conId: index of entry to use
        mock_ib: DataFrame with values for contract_details

    Returns:
          ContractDetails with fields from input mock_desc

    """
    ret_con = ContractDetails()
    ret_con.contract = get_contract_from_mock_desc(conId, mock_ib)
    ret_con.marketName = mock_ib.contract_descriptions.at[conId, "marketName"]  # cd
    ret_con.minTick = mock_ib.contract_descriptions.at[conId, "minTick"]  # cd
    ret_con.orderTypes = mock_ib.contract_descriptions.at[conId, "orderTypes"]  # cd
    ret_con.validExchanges = mock_ib.contract_descriptions.at[
        conId, "validExchanges"
    ]  # cd
    ret_con.priceMagnifier = mock_ib.contract_descriptions.at[
        conId, "priceMagnifier"
    ]  # cd
    ret_con.underConId = mock_ib.contract_descriptions.at[conId, "underConId"]  # cd
    ret_con.longName = mock_ib.contract_descriptions.at[conId, "longName"]  # cd
    ret_con.contractMonth = mock_ib.contract_descriptions.at[
        conId, "contractMonth"
    ]  # cd
    ret_con.industry = mock_ib.contract_descriptions.at[conId, "industry"]  # cd
    ret_con.category = mock_ib.contract_descriptions.at[conId, "category"]  # cd
    ret_con.subcategory = mock_ib.contract_descriptions.at[conId, "subcategory"]  # cd
    ret_con.timeZoneId = mock_ib.contract_descriptions.at[conId, "timeZoneId"]  # cd
    ret_con.tradingHours = mock_ib.contract_descriptions.at[conId, "tradingHours"]  # cd
    ret_con.liquidHours = mock_ib.contract_descriptions.at[conId, "liquidHours"]  # cd
    ret_con.evRule = mock_ib.contract_descriptions.at[conId, "evRule"]  # cd
    ret_con.evMultiplier = mock_ib.contract_descriptions.at[conId, "evMultiplier"]  # cd
    ret_con.mdSizeMultiplier = mock_ib.contract_descriptions.at[
        conId, "mdSizeMultiplier"
    ]  # cd
    ret_con.aggGroup = mock_ib.contract_descriptions.at[conId, "aggGroup"]  # cd
    ret_con.underSymbol = mock_ib.contract_descriptions.at[conId, "underSymbol"]  # cd
    ret_con.underSecType = mock_ib.contract_descriptions.at[conId, "underSecType"]  # cd
    ret_con.marketRuleIds = mock_ib.contract_descriptions.at[
        conId, "marketRuleIds"
    ]  # cd

    secIdList = mock_ib.contract_descriptions.at[conId, "secIdList"]
    new_secIdList = []
    for j in range(0, 2 * mock_ib.contract_descriptions.at[conId, "secIdListCount"], 2):
        tag = secIdList[j]
        value = secIdList[j + 1]
        tag_value = TagValue(tag, value)
        new_secIdList.append(tag_value)
    ret_con.secIdList = new_secIdList  # cd

    ret_con.realExpirationDate = mock_ib.contract_descriptions.at[
        conId, "realExpirationDate"
    ]  # cd

    # last trade time come from lastTradeDate as 'date time' (i.e., 2 items)
    if mock_ib.contract_descriptions.at[conId, "lastTradeDateOrContractMonth"]:
        split_date = mock_ib.contract_descriptions.at[
            conId, "lastTradeDateOrContractMonth"
        ].split()
        if len(split_date) > 1:
            ret_con.lastTradeTime = split_date[1]

    ret_con.stockType = mock_ib.contract_descriptions.at[conId, "stockType"]  # cd

    return ret_con


###############################################################################
# compare_tag_value
###############################################################################
def compare_tag_value(tag_value1: TagValue, tag_value2: TagValue) -> None:
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
def compare_combo_legs(cl1: ComboLeg, cl2: ComboLeg) -> None:
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
def compare_delta_neutral_contracts(
    con1: DeltaNeutralContract, con2: DeltaNeutralContract
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

    assert con1.lastTradeDateOrContractMonth == con2.lastTradeDateOrContractMonth

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
            compare_combo_legs(con1.comboLegs[i], con2.comboLegs[i])
    else:  # check whether one contract has it and the other does not
        assert not (con1.comboLegs or con2.comboLegs)

    if con1.deltaNeutralContract and con2.deltaNeutralContract:
        compare_delta_neutral_contracts(
            con1.deltaNeutralContract, con2.deltaNeutralContract
        )
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

    assert isinstance(contract.deltaNeutralContract, (DeltaNeutralContract, type(None)))


###############################################################################
# compare_contract_details
###############################################################################
def compare_contract_details(con1: ContractDetails, con2: ContractDetails) -> None:
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

    # assert con1.mdSizeMultiplier == con2.mdSizeMultiplier

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
