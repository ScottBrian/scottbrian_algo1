"""test_algo_app.py module."""
import time

########################################################################
# Standard Library
########################################################################
# from datetime import datetime, timedelta
# from pathlib import Path
# import sys
from abc import ABC, abstractmethod
from collections import deque, defaultdict
from collections.abc import Iterable
from enum import Enum, auto
import re
import string
from sys import _getframe
import threading
from typing import Any, List, NamedTuple, Optional, TypeAlias

# from typing_extensions import Final
########################################################################
# Third Party
########################################################################
from ibapi.tag_value import TagValue  # type: ignore
from ibapi.contract import ComboLeg  # type: ignore
from ibapi.contract import DeltaNeutralContract
from ibapi.contract import Contract, ContractDetails

import logging
import math
import numpy as np
import pandas as pd  # type: ignore
import pytest

from scottbrian_algo1.algo_app import (
    AlgoApp,
    AlreadyConnected,
    ConnectTimeout,
    RequestTimeout,
    DisconnectDuringRequest,
    ThreadConfig,
)

from scottbrian_algo1.algo_maps import get_contract_dict, get_contract_obj
from scottbrian_algo1.algo_maps import get_contract_details_obj

# from scottbrian_utils.diag_msg import diag_msg
# from scottbrian_utils.file_catalog import FileCatalog

from scottbrian_paratools.smart_thread import SmartThread, SmartThreadRequestTimedOut

from scottbrian_utils.diag_msg import get_caller_info
from scottbrian_utils.log_verifier import LogVer
from scottbrian_utils.msgs import Msgs

########################################################################
# TypeAlias
########################################################################
IntFloat: TypeAlias = Union[int, float]
OptIntFloat: TypeAlias = Optional[IntFloat]


########################################################################
# get logger
########################################################################
logger = logging.getLogger(__name__)


########################################################################
# AlgoApp test exceptions
########################################################################
class ErrorTstAlgoApp(Exception):
    """Base class for exception in this module."""

    pass


class FailedLockVerify(ErrorTstAlgoApp):
    """An expected lock position was not found."""

    pass


########################################################################
# connect style
########################################################################
class TestThreadConfig(Enum):
    TestNoSmartThreadAlgoAppCurrent = auto()
    TestSmartThreadAlgoAppCurrent = auto()
    TestSmartThreadAlgoAppRemote = auto()


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
def lock_verify(exp_positions: list[str]) -> None:
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

        if len(exp_positions) != len(AlgoApp._config_lock.owner_wait_q):
            logger.debug(
                f"lock_verify False 1: {len(exp_positions)=}, "
                f"{len(AlgoApp._config_lock.owner_wait_q)=}"
            )
            lock_verified = False
        else:
            for idx, expected_name in enumerate(exp_positions):
                search_thread = AlgoApp._config_lock.owner_wait_q[idx].thread
                test_name = get_smart_thread_name(
                    search_thread=search_thread, group_name="algo_app_group"
                )
                if test_name != expected_name:
                    logger.debug(
                        f"lock_verify False 2: {test_name=}, " f"{expected_name=}"
                    )
                    lock_verified = False
                    break

        if not lock_verified:
            if (time.time() - start_time) > timeout_value:
                raise FailedLockVerify(
                    f"lock_verify from {line_num=} timed out after"
                    f" {timeout_value} seconds waiting for the "
                    f"{exp_positions=} to match \n"
                    f"{AlgoApp._config_lock.owner_wait_q=} "
                )
            time.sleep(0.2)
    logger.debug(f"lock_verify exit: {exp_positions=}, {line_num=}")


class LogMsgBuilder:
    """Build log messages that are to be added to LogVer."""

    def __init__(self):
        self.msg1 = ""
        self.msg2 = ""
        self.msg3 = ""
        self.msg4 = ""

    def build_msg1(
        self,
        ip_addr: str,
        port: int,
        client_id: int,
        timeout: OptIntFloat = None,
    ):
        self.msg1 = (
            f"connect_to_ib entry: {ip_addr=}, {port=}, {client_id=}, {timeout=} "
        )
        cat_app.log_ver.add_msg(
            log_msg=re.escape(log_msg), log_name="scottbrian_algo1.algo_app"
        )
        cat_app.log_ver.add_msg(
            log_msg="starting AlgoClient thread 1",
            log_name="scottbrian_algo1.algo_app",
            log_level=logging.INFO,
        )

    connect_log_msgs: list[tuple[int, str]] = [
        (
            logging.INFO,
            "next valid ID is 1, "
            "threading.current_thread()=AlgoClient(algo_name='algo_app'), "
            "self=AlgoWrapper(, group_name=algo_app_group, algo_name=algo_app, "
            "client_name=ibapi_client)",
        )
    ]


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
# ConfirmResponse
########################################################################
class ConfirmResponse(ConfigCmd):
    """Confirm that an earlier command has completed."""

    def __init__(
        self,
        cmd_runners: Iterable[str],
        confirm_cmd: str,
        confirm_serial_num: int,
        confirmers: Iterable[str],
    ) -> None:
        """Initialize the instance.

        Args:
            cmd_runners: thread names that will execute the command
            confirm_cmd: command to be confirmed
            confirm_serial_num: serial number of command to confirm
            confirmers: cmd runners of the cmd to be confirmed
        """
        super().__init__(cmd_runners=cmd_runners)
        self.specified_args = locals()  # used for __repr__

        self.confirm_cmd = confirm_cmd
        self.confirm_serial_num = confirm_serial_num

        self.confirmers = get_set(confirmers)

        self.arg_list += ["confirm_cmd", "confirm_serial_num", "confirmers"]

    def run_process(self, cmd_runner: str) -> None:
        """Run the command.

        Args:
           cmd_runner: name of thread running the command
        """
        start_time = time.time()
        work_confirmers = self.confirmers.copy()
        if not work_confirmers:
            raise InvalidInputDetected(
                "ConfirmResponse detected an empty set of confirmers"
            )
        while work_confirmers:
            for name in work_confirmers:
                # If the serial number is in the completed_cmds for
                # this name then the command was completed. Remove the
                # target_rtn name and break to start looking again with
                # one less target_rtn until no targets remain.
                if self.confirm_serial_num in self.config_ver.completed_cmds[name]:
                    work_confirmers.remove(name)
                    break
            time.sleep(0.2)
            timeout_value = 60
            if time.time() - start_time > timeout_value:
                raise CmdTimedOut(
                    "ConfirmResponse serial_num "
                    f"{self.serial_num} took longer than "
                    f"{timeout_value} seconds waiting "
                    f"for {work_confirmers} to complete "
                    f"cmd {self.confirm_cmd} with "
                    f"serial_num {self.confirm_serial_num}."
                )


########################################################################
# ConfirmResponse
########################################################################
class ConnectToIb(ConfigCmd):
    """Confirm that an earlier command has completed."""

    def __init__(
        self,
        cmd_runners: Iterable[str],
        ip_addr: str = "127.0.0.1",
        port: int = 7496,
        client_id: int = 1,
        timeout: OptIntFloat = None,
    ) -> None:
        """Initialize the instance.

        Args:
            cmd_runners: thread names that will execute the command
            ip_addr: ip address of connection (127.0.0.2)
            port: port number (e.g., 7496)
            client_id: specific client number from 1 to x
        """
        super().__init__(cmd_runners=cmd_runners)
        self.specified_args = locals()  # used for __repr__

        self.ip_addr = ip_addr
        self.port = port

        self.client_id = client_id

        self.arg_list += ["ip_addr", "port", "client_id"]

    def run_process(self, cmd_runner: str) -> None:
        """Run the command.

        Args:
           cmd_runner: name of thread running the command
        """
        self.config_ver.handle_connect(
            cmd_runner=cmd_runner,
            ip_addr=self.ip_addr,
            port=self.port,
            client_id=self.client_id,
            timeout_type=TimeoutType.TimeoutNone,
            timeout=self.timeout,
        )


class ConfigVerifier:
    """Class that tracks and verifies the SmartThread configuration."""

    def __init__(
        self,
        algo_app: AlgoApp,
        group_name: str,
        commander_name: str,
        log_ver: LogVer,
        caplog_to_use: pytest.LogCaptureFixture,
        msgs: Msgs,
        max_msgs: int = 10,
        allow_log_test_msg: bool = True,
    ) -> None:
        """Initialize the ConfigVerifier.

        Args:
            algo_app: the AlgoApi
            group_name: name of group for this ConfigVerifier
            commander_name: name of the thread running the commands
            log_ver: the log verifier to track and verify log msgs
            caplog_to_use: pytest fixture to capture log messages
            msgs: Msgs class instance used to communicate with threads
            max_msgs: max message for the SmartThread msg_q

        """
        self.specified_args = locals()  # used for __repr__, see below
        self.algo_app = algo_app
        self.group_name = group_name
        self.commander_name = commander_name
        self.commander_thread_config_built = False

        self.monitor_thread = threading.Thread(target=self.monitor)
        self.monitor_exit = False

        self.main_driver_unreg: threading.Event = threading.Event()

        self.cmd_suite: deque[ConfigCmd] = deque()
        self.cmd_serial_num: int = 0
        self.completed_cmds: dict[str, list[int]] = defaultdict(list)
        self.f1_process_cmds: dict[str, bool] = {}
        self.thread_names: list[str] = [
            commander_name,
            "beta",
            "charlie",
            "delta",
            "echo",
            "fox",
            "george",
            "henry",
            "ida",
            "jack",
            "king",
            "love",
            "mary",
            "nancy",
            "oscar",
            "peter",
            "queen",
            "roger",
            "sam",
            "tom",
            "uncle",
            "victor",
            "wanda",
            "xander",
        ]
        self.unregistered_names: set[str] = set(self.thread_names)
        self.registered_names: set[str] = set()
        self.active_names: set[str] = set()
        self.thread_target_names: set[str] = set()
        self.stopped_remotes: set[str] = set()
        self.expected_registered: dict[str, ThreadTracker] = {}
        # self.expected_pairs: dict[tuple[str, str],
        #                           dict[str, ThreadPairStatus]] = {}
        self.expected_pairs: dict[st.PairKey, dict[str, ThreadPairStatus]] = {}
        self.log_ver = log_ver
        self.allow_log_test_msg = allow_log_test_msg
        self.caplog_to_use = caplog_to_use
        self.msgs = msgs
        self.ops_lock = threading.RLock()

        self.all_threads: dict[str, st.SmartThread] = {}

        self.max_msgs = max_msgs

        self.expected_num_recv_timeouts: int = 0

        self.test_case_aborted = False

        self.stopped_event_items: dict[str, MonitorEventItem] = {}
        self.cmd_waiting_event_items: dict[str, threading.Event] = {}

        self.stopping_names: list[str] = []

        self.recently_stopped: dict[str, int] = defaultdict(int)

        self.log_start_idx: int = 0
        self.log_search_items: tuple[LogSearchItems, ...] = (
            RequestEntryExitLogSearchItem(config_ver=self),
            SetupCompleteLogSearchItem(config_ver=self),
            SubProcessEntryExitLogSearchItem(config_ver=self),
            RegistryStatusLogSearchItem(config_ver=self),
            AddPairArrayEntryLogSearchItem(config_ver=self),
            AddStatusBlockEntryLogSearchItem(config_ver=self),
            DidCleanPairArrayUtcLogSearchItem(config_ver=self),
            UpdatePairArrayUtcLogSearchItem(config_ver=self),
            AddRegEntryLogSearchItem(config_ver=self),
            RemRegEntryLogSearchItem(config_ver=self),
            RemPairArrayEntryLogSearchItem(config_ver=self),
            DidCleanRegLogSearchItem(config_ver=self),
            SetStateLogSearchItem(config_ver=self),
            InitCompleteLogSearchItem(config_ver=self),
            F1AppExitLogSearchItem(config_ver=self),
            AlreadyUnregLogSearchItem(config_ver=self),
            RequestAckLogSearchItem(config_ver=self),
            DetectedStoppedRemoteLogSearchItem(config_ver=self),
            RequestRefreshLogSearchItem(config_ver=self),
            UnregJoinSuccessLogSearchItem(config_ver=self),
            JoinWaitingLogSearchItem(config_ver=self),
            StoppedLogSearchItem(config_ver=self),
            CmdWaitingLogSearchItem(config_ver=self),
            DebugLogSearchItem(config_ver=self),
            RemStatusBlockEntryLogSearchItem(config_ver=self),
            RemStatusBlockEntryDefLogSearchItem(config_ver=self),
            CRunnerRaisesLogSearchItem(config_ver=self),
            MonitorCheckpointLogSearchItem(config_ver=self),
        )

        self.log_found_items: deque[LogSearchItem] = deque()

        self.pending_events: dict[str, PendEvents] = {}
        self.auto_calling_refresh_msg = True
        self.auto_sync_ach_or_back_msg = True
        self.potential_def_del_pairs: dict[PotentialDefDelKey, int] = defaultdict(int)
        self.setup_pending_events()

        self.snap_shot_data: dict[int, SnapShotDataItem] = {}

        self.last_clean_reg_msg_idx: int = 0
        self.last_thread_stop_msg_idx: dict[str, int] = defaultdict(int)

        self.monitor_event: threading.Event = threading.Event()
        self.monitor_condition: threading.Condition = threading.Condition()
        self.monitor_pause: int = 0
        self.check_pending_events_complete_event: threading.Event = threading.Event()
        self.verify_config_complete_event: threading.Event = threading.Event()
        self.monitor_thread.start()

    ####################################################################
    # __repr__
    ####################################################################
    def __repr__(self) -> str:
        """Return a representation of the class.

        Returns:
            The representation as how the class is instantiated

        """
        if TYPE_CHECKING:
            __class__: Type[ConfigVerifier]  # noqa: F842
        classname = self.__class__.__name__
        parms = ""
        comma = ""

        for key, item in self.specified_args.items():
            if item:  # if not None
                if key in ("log_ver",):
                    if type(item) is str:
                        parms += comma + f"{key}='{item}'"
                    else:
                        parms += comma + f"{key}={item}"
                    comma = ", "  # after first item, now need comma

        return f"{classname}({parms})"

    ####################################################################
    # setup_pending_events
    ####################################################################
    def abort_test_case(self) -> None:
        """Abort the test case."""
        self.log_test_msg(f"aborting test case {get_formatted_call_sequence()}")
        self.test_case_aborted = True
        self.abort_all_f1_threads()

    ####################################################################
    # add_cmd
    ####################################################################
    def add_cmd(self, cmd: ConfigCmd, alt_frame_num: Optional[int] = None) -> int:
        """Add a command to the deque.

        Args:
            cmd: command to add
            alt_frame_num: non-zero indicates to add the line number for
                the specified frame to the cmd object so that it will be
                included with in the log just after the line_num in
                parentheses

        Returns:
            the serial number for the command

        """
        if alt_frame_num is not None:
            alt_frame_num += 2
        serial_num = self.add_cmd_info(
            cmd=cmd, frame_num=2, alt_frame_num=alt_frame_num
        )
        self.cmd_suite.append(cmd)
        return serial_num

    ####################################################################
    # add_cmd_info
    ####################################################################
    def add_cmd_info(
        self,
        cmd: ConfigCmd,
        frame_num: int = 1,
        alt_frame_num: Optional[int] = None,
    ) -> int:
        """Add a command to the deque.

        Args:
            cmd: command to add
            frame_num: how many frames back to go for line number
            alt_frame_num: non-zero indicates to add the line number for
                the specified frame to the cmd object so that it will be
                included with in the log just after the line_num in
                parentheses

        Returns:
            the serial number for the command
        """
        self.cmd_serial_num += 1
        cmd.serial_num = self.cmd_serial_num

        frame = _getframe(frame_num)
        caller_info = get_caller_info(frame)
        cmd.line_num = caller_info.line_num
        del frame
        if alt_frame_num is not None and alt_frame_num > 0:
            frame = _getframe(alt_frame_num)
            caller_info = get_caller_info(frame)
            cmd.alt_line_num = caller_info.line_num
            del frame

        cmd.config_ver = self

        return self.cmd_serial_num

    ####################################################################
    # add_log_msg
    ####################################################################
    def add_log_msg(
        self,
        new_log_msg: str,
        log_level: int = logging.DEBUG,
        fullmatch: bool = True,
        log_name: Optional[str] = None,
    ) -> None:
        """Add log message to log_ver for SmartThread logger.

        Args:
            new_log_msg: msg to add to log_ver
            log_level: the logging severity level to use
            fullmatch: specify whether fullmatch should be done instead
                of match
            log_name: name of log to use for add_msg
        """
        if log_name is None:
            log_name = "scottbrian_paratools.smart_thread"
        self.log_ver.add_msg(
            log_name=log_name,
            log_level=log_level,
            log_msg=new_log_msg,
            fullmatch=fullmatch,
        )

    ####################################################################
    # handle_join
    ####################################################################
    def handle_connect(
        self,
        cmd_runner: str,
        ip_addr: str,
        port: int,
        client_id: int,
        timeout_type: TimeoutType = TimeoutType.TimeoutNone,
        timeout: OptIntFloat = None,
    ) -> None:
        """Handle the connect execution and log msgs.

        Args:
            cmd_runner: name of thread doing the cmd
            ip_addr: ip address of connection (127.0.0.2)
            port: port number (e.g., 7496)
            client_id: specific client number from 1 to x
            timeout_type: None, False, or True
            timeout: value for timeout on connect request

        """
        self.log_test_msg(
            f"connect entry: {cmd_runner=}, {client_id=}, {timeout_type=}, {timeout=}"
        )
        self.log_ver.add_call_seq(
            name="connect_ib", seq="test_algo_app.py::ConfigVerifier.handle_connect"
        )

        start_time = time.time()

        # pe = self.pending_events[cmd_runner]
        #
        # pe[PE.start_request].append(
        #     StartRequest(
        #         req_type=st.ReqType.Smart_join,
        #         timeout_type=timeout_type,
        #         targets=join_names.copy(),
        #         unreg_remotes=unreg_names.copy(),
        #         not_registered_remotes=set(),
        #         timeout_remotes=timeout_remotes,
        #         stopped_remotes=set(),
        #         deadlock_remotes=set(),
        #         eligible_targets=set(),
        #         completed_targets=set(),
        #         first_round_completed=set(),
        #         stopped_target_threads=set(),
        #         exp_senders=set(),
        #         exp_resumers=set(),
        #     )
        # )

        # req_key_entry: RequestKey = ("smart_join", "entry")
        #
        # pe[PE.request_msg][req_key_entry] += 1
        #
        # req_key_exit: RequestKey = ("smart_join", "exit")

        # enter_exit = ('entry', 'exit')
        if timeout_type == TimeoutType.TimeoutNone:
            # pe[PE.request_msg][req_key_exit] += 1
            # self.all_threads[cmd_runner].smart_join(
            #     targets=join_names, log_msg=log_msg)
            self.algo_app.connect_to_ib(
                ip_addr=ip_addr,
                port=port,
                client_id=client_id,
            )

        elif timeout_type == TimeoutType.TimeoutFalse:
            # pe[PE.request_msg][req_key_exit] += 1
            # self.all_threads[cmd_runner].smart_join(
            #     targets=join_names, timeout=timeout, log_msg=log_msg
            # )
            self.algo_app.connect_to_ib(
                ip_addr=ip_addr,
                port=port,
                client_id=client_id,
                timeout=timeout,
            )

        elif timeout_type == TimeoutType.TimeoutTrue:
            # enter_exit = ('entry', )
            error_msg = self.get_error_msg(
                cmd_runner=cmd_runner,
                smart_request="smart_join",
                targets=join_names,
                error_str="SmartThreadRequestTimedOut",
            )
            with pytest.raises(st.SmartThreadRequestTimedOut) as exc:
                self.algo_app.connect_to_ib(
                    ip_addr=ip_addr,
                    port=port,
                    client_id=client_id,
                    timeout=timeout,
                )

            err_str = str(exc.value)
            assert re.fullmatch(error_msg, err_str)

            self.add_log_msg(error_msg, log_level=logging.ERROR)

        # self.wait_for_monitor(cmd_runner=cmd_runner, rtn_name="handle_connect")

        self.log_test_msg(
            f"connect exit: {cmd_runner=}, {client_id=}, {timeout_type=}, {timeout=}"
        )


def scenario_driver(
    caplog_to_use: pytest.LogCaptureFixture,
    scenario_driver_parms: list[ScenarioDriverParms],
) -> None:
    """Build and run a scenario.

    Args:
        caplog_to_use: the capsys to capture log messages
        scenario_driver_parms: args for scenario_driver_part_1


    """
    log_ver = LogVer(log_name=__name__)

    config_vers: list[ConfigVerifier] = []
    for sdparm in scenario_driver_parms:
        msgs = Msgs()
        config_ver = ConfigVerifier(
            group_name=sdparm.group_name,
            commander_name=sdparm.commander_name,
            log_ver=log_ver,
            caplog_to_use=caplog_to_use,
            msgs=msgs,
            max_msgs=10,
            allow_log_test_msg=sdparm.allow_log_test_msg,
        )
        config_vers.append(config_ver)

        scenario_driver_part1(
            config_ver=config_ver,
            scenario_builder=sdparm.scenario_builder,
            scenario_builder_args=sdparm.scenario_builder_args,
            log_ver=log_ver,
            cmder_config=sdparm.commander_config,
            commander_name=sdparm.commander_name,
            group_name=sdparm.group_name,
        )

    for idx, config_ver in enumerate(config_vers):
        if scenario_driver_parms[idx].commander_config in [
            AppConfig.F1Rtn,
            AppConfig.RemoteThreadApp,
            AppConfig.RemoteSmartThreadApp,
            AppConfig.RemoteSmartThreadApp2,
        ]:
            config_ver.all_threads[config_ver.commander_name].thread.join()

    ################################################################
    # check log results
    ################################################################
    match_results = log_ver.get_match_results(caplog=caplog_to_use)
    log_ver.print_match_results(match_results, print_matched=False)
    log_ver.verify_log_results(match_results)


########################################################################
# scenario_driver
########################################################################
def scenario_driver_part1(
    config_ver: ConfigVerifier,
    scenario_builder: Callable[..., None],
    scenario_builder_args: dict[str, Any],
    log_ver: LogVer,
    cmder_config: AppConfig,
    commander_name: str,
    group_name: str,
) -> None:
    """Build and run a scenario.

    Args:
        config_ver: the ConfigVerifier
        scenario_builder: the ConfigVerifier builder method to call
        scenario_builder_args: the args to pass to the builder
        log_ver: log verification object
        cmder_config: specifies how the commander will run
        commander_name: name of commander thread
        group_name: group_name to use

    """

    ################################################################
    # f1
    ################################################################
    def f1(f1_smart_thread: st.SmartThread, f1_config_ver: ConfigVerifier) -> None:
        f1_config_ver.log_test_msg(f"f1 entry: {f1_smart_thread.name=}")

        f1_config_ver.main_driver()

        ############################################################
        # exit
        ############################################################
        f1_config_ver.log_test_msg(f"f1 exit: {f1_smart_thread.name=}")

    ################################################################
    # Set up log verification and start tests
    ################################################################
    # log_ver = LogVer(log_name=__name__)
    log_ver.add_call_seq(name=commander_name, seq=get_formatted_call_sequence())

    random.seed(42)

    config_ver.log_test_msg(
        f"scenario_driver_part1 entry: {commander_name=} "
        f"{group_name=} {scenario_builder=} "
        f"{scenario_builder_args=} {cmder_config=}"
    )

    config_ver.unregistered_names -= {commander_name}
    config_ver.active_names |= {commander_name}

    scenario_builder(config_ver, **scenario_builder_args)

    config_ver.add_cmd(
        VerifyConfig(
            cmd_runners=commander_name, verify_type=VerifyType.VerifyStructures
        )
    )

    names = list(config_ver.active_names - {commander_name})
    config_ver.build_exit_suite(cmd_runner=commander_name, names=names)

    config_ver.build_join_suite(
        cmd_runners=[config_ver.commander_name], join_target_names=names
    )

    def initialize_config_ver(
        cmd_thread: st.SmartThread,
        auto_start: bool,
        auto_start_decision: AutoStartDecision,
        exp_alive: bool,
        thread_create: st.ThreadCreate,
        exp_state: st.ThreadState,
    ) -> None:
        """Set up the mock registry for the commander.

        Args:
            cmd_thread: the commander thread
            auto_start: specifies whether auto_start was specified
                on the init
            auto_start_decision: specifies whether an auto start is
                not needed, yes, or no
            exp_alive: specifies whether the thread is expected to
                be alive at the end of smart_init
            thread_create: specifies which create style is done
            exp_state: the expected state after smart_init

        """
        config_ver.all_threads[commander_name] = cmd_thread

        config_ver.expected_registered[commander_name] = ThreadTracker(
            thread=cmd_thread,
            is_alive=False,
            exiting=False,
            is_auto_started=auto_start,
            is_TargetThread=False,
            exp_init_is_alive=exp_alive,
            exp_init_thread_state=exp_state,
            thread_create=thread_create,
            auto_start_decision=auto_start_decision,
            # st_state=st.ThreadState.Unregistered,
            st_state=st.ThreadState.Initialized,
            found_del_pairs=defaultdict(int),
        )

        pe = config_ver.pending_events[commander_name]
        pe[PE.start_request].append(
            StartRequest(
                req_type=st.ReqType.Smart_init,
                targets={commander_name},
                unreg_remotes=set(),
                not_registered_remotes=set(),
                timeout_remotes=set(),
                stopped_remotes=set(),
                deadlock_remotes=set(),
                eligible_targets=set(),
                completed_targets=set(),
                first_round_completed=set(),
                stopped_target_threads=set(),
                exp_senders=set(),
                exp_resumers=set(),
            )
        )

        req_key_entry: RequestKey = ("smart_init", "entry")
        pe[PE.request_msg][req_key_entry] += 1

        req_key_exit: RequestKey = ("smart_init", "exit")
        pe[PE.request_msg][req_key_exit] += 1

        config_ver.commander_thread_config_built = True

    ################################################################
    # start commander
    ################################################################
    config_ver.monitor_pause = True
    outer_thread_app: Union[OuterThreadApp, OuterSmartThreadApp, OuterSmartThreadApp2]
    if cmder_config == AppConfig.ScriptStyle:
        commander_thread = st.SmartThread(group_name=group_name, name=commander_name)

        initialize_config_ver(
            cmd_thread=commander_thread,
            auto_start=True,
            auto_start_decision=AutoStartDecision.auto_start_obviated,
            exp_alive=True,
            exp_state=st.ThreadState.Alive,
            thread_create=st.ThreadCreate.Current,
        )
        config_ver.monitor_pause = False
        config_ver.main_driver()
    elif cmder_config == AppConfig.F1Rtn:
        f1_thread = st.SmartThread(
            group_name=group_name,
            name=commander_name,
            target_rtn=f1,
            auto_start=False,
            kwargs={"f1_config_ver": config_ver},
            thread_parm_name="f1_smart_thread",
        )

        initialize_config_ver(
            cmd_thread=f1_thread,
            auto_start=False,
            auto_start_decision=AutoStartDecision.auto_start_no,
            exp_alive=False,
            exp_state=st.ThreadState.Registered,
            thread_create=st.ThreadCreate.Target,
        )
        config_ver.monitor_pause = False

        pe = config_ver.pending_events[commander_name]
        pe[PE.start_request].append(
            StartRequest(
                req_type=st.ReqType.Smart_start,
                targets={commander_name},
                unreg_remotes=set(),
                not_registered_remotes=set(),
                timeout_remotes=set(),
                stopped_remotes=set(),
                deadlock_remotes=set(),
                eligible_targets=set(),
                completed_targets=set(),
                first_round_completed=set(),
                stopped_target_threads=set(),
                exp_senders=set(),
                exp_resumers=set(),
            )
        )

        req_key_entry: RequestKey = ("smart_start", "entry")

        pe[PE.request_msg][req_key_entry] += 1

        req_key_exit: RequestKey = ("smart_start", "exit")
        pe[PE.request_msg][req_key_exit] += 1

        f1_thread.smart_start()
        # if not skip_join:
        #     outer_thread_app.join()
    elif cmder_config == AppConfig.CurrentThreadApp:
        cmd_current_app = CommanderCurrentApp(
            config_ver=config_ver, name=commander_name, max_msgs=10
        )

        initialize_config_ver(
            cmd_thread=cmd_current_app.smart_thread,
            auto_start=False,
            auto_start_decision=AutoStartDecision.auto_start_no,
            exp_alive=True,
            exp_state=st.ThreadState.Alive,
            thread_create=st.ThreadCreate.Current,
        )
        config_ver.monitor_pause = False
        cmd_current_app.run()
    elif cmder_config == AppConfig.RemoteThreadApp:
        outer_thread_app = OuterThreadApp(
            config_ver=config_ver, name=commander_name, max_msgs=10
        )

        initialize_config_ver(
            cmd_thread=outer_thread_app.smart_thread,
            auto_start=False,
            auto_start_decision=AutoStartDecision.auto_start_no,
            exp_alive=False,
            exp_state=st.ThreadState.Registered,
            thread_create=st.ThreadCreate.Thread,
        )
        config_ver.monitor_pause = False

        pe = config_ver.pending_events[commander_name]
        pe[PE.start_request].append(
            StartRequest(
                req_type=st.ReqType.Smart_start,
                targets={commander_name},
                unreg_remotes=set(),
                not_registered_remotes=set(),
                timeout_remotes=set(),
                stopped_remotes=set(),
                deadlock_remotes=set(),
                eligible_targets=set(),
                completed_targets=set(),
                first_round_completed=set(),
                stopped_target_threads=set(),
                exp_senders=set(),
                exp_resumers=set(),
            )
        )

        req_key_entry = ("smart_start", "entry")

        pe[PE.request_msg][req_key_entry] += 1

        req_key_exit = ("smart_start", "exit")
        pe[PE.request_msg][req_key_exit] += 1

        outer_thread_app.smart_thread.smart_start()
        # if not skip_join:
        #     outer_thread_app.join()
    elif cmder_config == AppConfig.RemoteSmartThreadApp:
        outer_thread_app = OuterSmartThreadApp(
            config_ver=config_ver, name=commander_name, max_msgs=10
        )

        initialize_config_ver(
            cmd_thread=outer_thread_app,
            auto_start=False,
            auto_start_decision=AutoStartDecision.auto_start_no,
            exp_alive=False,
            exp_state=st.ThreadState.Registered,
            thread_create=st.ThreadCreate.Thread,
        )
        config_ver.monitor_pause = False

        pe = config_ver.pending_events[commander_name]
        pe[PE.start_request].append(
            StartRequest(
                req_type=st.ReqType.Smart_start,
                targets={commander_name},
                unreg_remotes=set(),
                not_registered_remotes=set(),
                timeout_remotes=set(),
                stopped_remotes=set(),
                deadlock_remotes=set(),
                eligible_targets=set(),
                completed_targets=set(),
                first_round_completed=set(),
                stopped_target_threads=set(),
                exp_senders=set(),
                exp_resumers=set(),
            )
        )

        req_key_entry = ("smart_start", "entry")
        pe[PE.request_msg][req_key_entry] += 1

        req_key_exit = ("smart_start", "exit")
        pe[PE.request_msg][req_key_exit] += 1

        outer_thread_app.smart_start(commander_name)
        # threading.Thread.join(outer_thread_app)
    elif cmder_config == AppConfig.RemoteSmartThreadApp2:
        outer_thread_app = OuterSmartThreadApp2(
            config_ver=config_ver, name=commander_name, max_msgs=10
        )

        initialize_config_ver(
            cmd_thread=outer_thread_app,
            auto_start=False,
            auto_start_decision=AutoStartDecision.auto_start_no,
            exp_alive=False,
            exp_state=st.ThreadState.Registered,
            thread_create=st.ThreadCreate.Thread,
        )
        config_ver.monitor_pause = False

        pe = config_ver.pending_events[commander_name]
        pe[PE.start_request].append(
            StartRequest(
                req_type=st.ReqType.Smart_start,
                targets={commander_name},
                unreg_remotes=set(),
                not_registered_remotes=set(),
                timeout_remotes=set(),
                stopped_remotes=set(),
                deadlock_remotes=set(),
                eligible_targets=set(),
                completed_targets=set(),
                first_round_completed=set(),
                stopped_target_threads=set(),
                exp_senders=set(),
                exp_resumers=set(),
            )
        )

        req_key_entry = ("smart_start", "entry")
        pe[PE.request_msg][req_key_entry] += 1

        req_key_exit = ("smart_start", "exit")
        pe[PE.request_msg][req_key_exit] += 1

        outer_thread_app.smart_start(commander_name)
        # threading.Thread.join(outer_thread_app)
    else:
        raise UnrecognizedCmd(f"scenario_driver does not recognize {cmder_config=}")

    config_ver.log_test_msg(
        f"scenario_driver_part1 exit: {commander_name=} "
        f"{group_name=} {scenario_builder=} "
        f"{scenario_builder_args=} {cmder_config=}"
    )


########################################################################
# TestAlgoAppConnect class
########################################################################
class TestAlgoAppConnect:
    """TestAlgoAppConnect class."""

    ####################################################################
    # test_mock_connect_to_ib
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
    def test_mock_connect_to_ib(
        self,
        delay_arg: int,
        timeout_type_arg: int,
        cat_app: "MockIB",
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test connecting to IB.

        Args:
            delay_arg: number of seconds to delay
            timeout_type_arg: specifies whether timeout should occur
            cat_app: pytest fixture (see conftest.py)
        """
        # print(f"{logging.Logger.manager.loggerDict.keys()=}\n")

        for key, item in logging.Logger.manager.loggerDict.items():
            # print(f"{key=} {type(item)=}\n")
            if key[0:5] == "ibapi" and not isinstance(item, logging.PlaceHolder):
                logging.Logger.manager.loggerDict[key].setLevel(logging.CRITICAL)
            # if isinstance(lock_manager_logger, logging.PlaceHolder):
            #     raise InvalidConfigurationDetected(
            #         "test_smart_thread_log_msg detected that the manager "
            #         "logger for scottbrian_locking is a PlaceHolder"
            #     )

        # logging.Logger.manager.loggerDict["ibapi"].setLevel(logging.CRITICAL)

        def f1(f1_smart_thread: SmartThread, f1_timeout_type_arg: int):
            connect_test(f1_timeout_type_arg)
            f1_smart_thread.smart_resume(waiters="alpha")

        def connect_test(ct_timeout_type_arg: int):
            ip_addr = "127.0.0.1"
            port = algo_app.PORT_FOR_LIVE_TRADING
            client_id = 1

            # log_msg = (
            #     f"connect_to_ib entry: {ip_addr=}, {port=}, {client_id=}, {timeout=} "
            #     f"{setup_args=}"
            # )

            if ct_timeout_type_arg == TimeoutType.TimeoutNone:
                timeout = None
                algo_app.connect_to_ib(
                    ip_addr="127.0.0.1",
                    port=algo_app.PORT_FOR_LIVE_TRADING,
                    client_id=1,
                )
            elif ct_timeout_type_arg == TimeoutType.TimeoutFalse:
                timeout_value = delay_arg * 2
                timeout = timeout_value

                algo_app.connect_to_ib(
                    ip_addr="127.0.0.1",
                    port=algo_app.PORT_FOR_LIVE_TRADING,
                    client_id=1,
                    timeout=timeout_value,
                )
            else:
                timeout_value = delay_arg / 2.0
                timeout = timeout_value

                cat_app.delay_value = -1
                with pytest.raises(ConnectTimeout):
                    algo_app.connect_to_ib(
                        ip_addr="127.0.0.1",
                        port=algo_app.PORT_FOR_LIVE_TRADING,
                        client_id=1,
                        timeout=timeout_value,
                    )

            # log_msg = (
            #     f"connect_to_ib entry: {ip_addr=}, {port=}, {client_id=}, "
            #     f"{timeout=} "
            # )
            # cat_app.log_ver.add_msg(
            #     log_msg=re.escape(log_msg), log_name="scottbrian_algo1.algo_app"
            # )
            # cat_app.log_ver.add_msg(
            #     log_msg="starting AlgoClient thread 1",
            #     log_name="scottbrian_algo1.algo_app",
            #     log_level=logging.INFO,
            # )

        # log_ver = LogVer(log_name=__name__)

        if timeout_type_arg == TimeoutType.TimeoutTrue and delay_arg == 0:
            return

        cat_app.delay_value = delay_arg

        algo_app = AlgoApp(ds_catalog=cat_app.app_cat, algo_name="algo_app")
        verify_algo_app_initialized(algo_app)

        # we are testing connect_to_ib and the subsequent code that gets
        # control as a result, such as getting the first requestID and
        # then starting a separate thread for the run loop.
        # logger.debug("about to connect")
        cat_app.log_test_msg("about to connect")

        alpha_smart_thread = SmartThread(group_name="test1", name="alpha")
        beta_smart_thread = SmartThread(
            group_name="test1",
            name="beta",
            target_rtn=f1,
            thread_parm_name="f1_smart_thread",
            kwargs={"f1_timeout_type_arg": timeout_type_arg},
        )

        alpha_smart_thread.smart_wait(resumers="beta")

        # connect_test(timeout_type_arg)

        # if timeout_type_arg == TimeoutType.TimeoutNone:
        #     algo_app.connect_to_ib(
        #         ip_addr="127.0.0.1", port=algo_app.PORT_FOR_LIVE_TRADING, client_id=1
        #     )
        # elif timeout_type_arg == TimeoutType.TimeoutFalse:
        #     timeout_value = delay_arg * 2
        #     algo_app.connect_to_ib(
        #         ip_addr="127.0.0.1",
        #         port=algo_app.PORT_FOR_LIVE_TRADING,
        #         client_id=1,
        #         timeout=timeout_value,
        #     )
        # else:
        #     timeout_value = delay_arg / 2.0
        #     cat_app.delay_value = -1
        #     with pytest.raises(ConnectTimeout):
        #         algo_app.connect_to_ib(
        #             ip_addr="127.0.0.1",
        #             port=algo_app.PORT_FOR_LIVE_TRADING,
        #             client_id=1,
        #             timeout=timeout_value,
        #         )

        logger.debug("back from connect")

        if timeout_type_arg == TimeoutType.TimeoutTrue and delay_arg > 0:
            time.sleep(delay_arg + 1)

        if cat_app.delay_value != -1:
            verify_algo_app_connected(algo_app)

            algo_app.disconnect_from_ib()

        verify_algo_app_disconnected(algo_app)

        alpha_smart_thread.smart_join(targets="beta")
        alpha_smart_thread.smart_unreg()

        algo_app.shut_down()

        ################################################################
        # check log results
        ################################################################
        match_results = cat_app.log_ver.get_match_results(caplog=caplog)
        cat_app.log_ver.print_match_results(match_results, print_matched=False)
        cat_app.log_ver.verify_log_results(match_results)

    ####################################################################
    # test_mock_disconnect_from_ib
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
    def test_mock_disconnect_from_ib(
        self,
        delay_arg: int,
        timeout_type_arg: int,
        cat_app: "MockIB",
        caplog: pytest.LogCaptureFixture,
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

        # we are testing connect_to_ib and the subsequent code that gets
        # control as a result, such as getting the first requestID and
        # then starting a separate thread for the run loop.
        logger.debug("about to connect")
        algo_app.connect_to_ib(
            ip_addr="127.0.0.1", port=algo_app.PORT_FOR_LIVE_TRADING, client_id=1
        )

        verify_algo_app_connected(algo_app)

        cat_app.delay_value = delay_arg

        if timeout_type_arg == TimeoutType.TimeoutNone:
            algo_app.disconnect_from_ib()
        elif timeout_type_arg == TimeoutType.TimeoutFalse:
            timeout_value = abs(delay_arg) * 2
            algo_app.disconnect_from_ib(timeout=timeout_value)
        else:
            timeout_value = abs(delay_arg) * 0.5
            algo_app.disconnect_from_ib(timeout=timeout_value)

        if timeout_type_arg == TimeoutType.TimeoutTrue and delay_arg > 0:
            time.sleep(delay_arg + 1)

        verify_algo_app_disconnected(algo_app)

        # algo_app.shut_down()

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
    contract: "Contract", algo_app: "AlgoApp", mock_ib: Any, conId_list: List[int]
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
def build_combo_legs(idx: int, mock_ib: Any) -> List[ComboLeg]:
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
