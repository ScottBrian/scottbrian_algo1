"""Mappings of various classes used in the AlgoApp class."""

import pandas as pd  # type: ignore

# Function get_contract_details_obj evals a string containing Nat or Timestamp
# and this causes an interpreter message about these being unknown. To solve
# that, we simply include the pandas Timestamp and NaT, but then the linter
# fails to see these being used since they are not visible inside the string
# (which is in a variable, so even we can't see it). The noqa F401 comments
# are used to suppress the linter messages.
from pandas import Timestamp  # noqa F401
from pandas import NaT  # noqa F401
import copy

from typing import Any, Dict


from ibapi.contract import ComboLeg, Contract, ContractDetails  # type: ignore
from ibapi.contract import DeltaNeutralContract
from ibapi.tag_value import TagValue  # type: ignore

# from scottbrian_utils.diag_msg import diag_msg


###############################################################################
# get TagValue dictionary/obj
###############################################################################
def get_tag_value_dict(tag_value: TagValue) -> Dict[str, Any]:
    """Get dictionary to be used for DataFrame entry.

    Args:
        tag_value: instance of TagValue class

    Returns:
        dictionary of tag_value object

    """
    ret_dict: Dict[str, Any] = copy.deepcopy(tag_value.__dict__)
    return ret_dict


def get_tag_value_obj(tag_value_dict: Dict[str, Any]) -> TagValue:
    """Get object from dictionary.

    Args:
        tag_value_dict: dictionary to be used to restore the tag_value

    Returns:
          An instance of TagValue

    """
    tag_value = TagValue()
    tag_value.__dict__ = copy.deepcopy(tag_value_dict)
    return tag_value


###############################################################################
# get ComboLeg dictionary/obj
###############################################################################
def get_combo_leg_dict(combo_leg: ComboLeg) -> Dict[str, Any]:
    """Get dictionary to be used for DataFrame entry.

    Args:
        combo_leg: instance of ComboLeg class

    Returns:
        dictionary of combo_leg object

    """
    # diag_msg('\combo_leg.__dict__\n', combo_leg.__dict__)
    ret_dict: Dict[str, Any] = copy.deepcopy(combo_leg.__dict__)
    return ret_dict


def get_combo_leg_obj(combo_leg_dict: Dict[str, Any]) -> ComboLeg:
    """Get object from dictionary.

    Args:
        combo_leg_dict: dictionary to be used to restore the combo_leg

    Returns:
          An instance of ComboLeg

    """
    combo_leg = ComboLeg()
    combo_leg.__dict__ = copy.deepcopy(combo_leg_dict)
    return combo_leg


###############################################################################
# get DeltaNeutralContract dictionary
###############################################################################
def get_delta_neutral_contract_dict(delta_neutral_contract:
                                    DeltaNeutralContract) -> Dict[str, Any]:
    """Get dictionary to be used for DataFrame entry.

    Args:
        delta_neutral_contract: instance of DeltaNeutralContract class

    Returns:
        dictionary of delta_neutral_contract object

    """
    ret_dict: Dict[str, Any] = copy.deepcopy(delta_neutral_contract.__dict__)
    return ret_dict


def get_delta_neutral_contract_obj(delta_neutral_contract_dict: Dict[str, Any]
                                   ) -> DeltaNeutralContract:
    """Get object from dictionary.

    Args:
        delta_neutral_contract_dict: dictionary to be used to restore
                                       the delta_neutral_contract

    Returns:
          An instance of DeltaNeutralContract

    """
    delta_neutral_contract = DeltaNeutralContract()
    delta_neutral_contract.__dict__ = \
        copy.deepcopy(delta_neutral_contract_dict)
    return delta_neutral_contract


###############################################################################
# get Contract dictionary/obj
###############################################################################
def get_contract_dict(contract: Contract) -> Dict[str, Any]:
    """Get dictionary to be used for DataFrame entry.

    Args:
        contract: instance of Contract class

    Returns:
        dictionary of contract object

    """
    # We must make a copy of the contract dictionary because we are
    # going to modify it when we change the string date into a Timestamp
    # and add a new field to retain the string date. If we don't make
    # a copy, the contract will be changed as well when we change its
    # dictionary.
    ret_dict: Dict[str, Any] = copy.deepcopy(contract.__dict__)
    # diag_msg("\nret_dict\n", ret_dict)
    # Handle comboLegs
    if contract.comboLegs:
        combo_leg_list = []
        for combo_leg in contract.comboLegs:
            # diag_msg('\ncombo_leg\n', combo_leg)
            # diag_msg('\ncombo_leg.__dict__\n', combo_leg.__dict__)
            combo_leg_list.append(get_combo_leg_dict(combo_leg))
        ret_dict['comboLegs'] = str(tuple(combo_leg_list))
        # diag_msg("\nret_dict['comboLegs']\n", ret_dict['comboLegs'])

    # Handle deltaNeutralContract
    if contract.deltaNeutralContract:
        dnc_dict = \
            get_delta_neutral_contract_dict(contract.deltaNeutralContract)
        ret_dict['deltaNeutralContract'] = str(dnc_dict)

    # Convert string date to Timestamp.
    # We want Timestamps in the DataFrame for analysis,
    # and we want string dates in the contract for ib requests
    # We save the original so we can restore what it was in case
    # we need to add the day for the shortened FUT date which has
    # only year and month
    ret_dict['originalLastTradeDate'] = \
        ret_dict['lastTradeDateOrContractMonth']
    if ret_dict['lastTradeDateOrContractMonth']:  # if not empty
        # if year and month only (secType of FUT)
        if len(ret_dict['lastTradeDateOrContractMonth']) == 6:
            # add day 01 so we have a legitimate date
            ret_dict['lastTradeDateOrContractMonth'] = \
                ret_dict['lastTradeDateOrContractMonth'] + '01'
        ret_dict['lastTradeDateOrContractMonth'] = \
            pd.Timestamp(ret_dict['lastTradeDateOrContractMonth'])
    else:
        ret_dict['lastTradeDateOrContractMonth'] = pd.NaT

    return ret_dict


def get_contract_obj(contract_dict: Dict[str, Any]) -> Contract:
    """Get object from dictionary.

    Args:
        contract_dict: dictionary to be used to restore contract

    Returns:
          An instance of Contract

    """
    work_contract_dict = copy.deepcopy(contract_dict)
    contract = Contract()  # start with a default contract

    # Handle comboLegs
    if work_contract_dict['comboLegs']:
        # diag_msg("\nwork_contract_dict['comboLegs']\n",
        #          work_contract_dict['comboLegs'])
        combo_leg_list = []
        for combo_leg_dict in eval(work_contract_dict['comboLegs']):
            combo_leg_list.append(get_combo_leg_obj(combo_leg_dict))
        work_contract_dict['comboLegs'] = combo_leg_list

    # Handle deltaNeutralContract
    if work_contract_dict['deltaNeutralContract']:
        delta_neutral_contract = \
            get_delta_neutral_contract_obj(
                eval(work_contract_dict['deltaNeutralContract']))
        work_contract_dict['deltaNeutralContract'] = delta_neutral_contract

    # Convert Timestamp back to string date.
    # We want Timestamps in the DataFrame for analysis,
    # and we want string dates in the contract for ib requests.
    # We can simply copy back the original that we had saved earlier
    # when we created the dictionary in get_contract_dict. Note that
    # read_csv converts the string to a fixed unless we have a conversion
    # routine to make sure it is read as a string (and we do).
    work_contract_dict['lastTradeDateOrContractMonth'] = \
        str(work_contract_dict.pop('originalLastTradeDate'))

    contract.__dict__ = work_contract_dict

    return contract


###############################################################################
# get ContractDetails dictionary
###############################################################################
def get_contract_details_dict(contract_details: ContractDetails
                              ) -> Dict[str, Any]:
    """Get dictionary to be used for DataFrame entry.

    Args:
        contract_details: instance of ContractDetails class

    Returns:
        dictionary of contract_details object

    """
    ret_dict: Dict[str, Any] = copy.deepcopy(contract_details.__dict__)
    if contract_details.contract:
        contract_dict = get_contract_dict(contract_details.contract)
        ret_dict['contract'] = str(contract_dict)

    if contract_details.secIdList:
        sid_list = []
        for secId in contract_details.secIdList:
            sid_list.append(get_tag_value_dict(secId))
        ret_dict['secIdList'] = str(tuple(sid_list))

    return ret_dict


def get_contract_details_obj(contract_details_dict: Dict[str, Any]
                             ) -> ContractDetails:
    """Get object from dictionary.

    Args:
        contract_details_dict: dictionary to be used to restore
                                 contract_details

    Returns:
          An instance of contract_details

    """
    work_con_det_dict = copy.deepcopy(contract_details_dict)
    contract_details = ContractDetails()
    if work_con_det_dict['contract']:
        contract = get_contract_obj(eval(work_con_det_dict['contract']))
        work_con_det_dict['contract'] = contract

    if work_con_det_dict['secIdList']:
        secId_tuple = eval(work_con_det_dict['secIdList'])
        secIdList = []
        for tag_value_dict in secId_tuple:
            secIdList.append(get_tag_value_obj(tag_value_dict))
        work_con_det_dict['secIdList'] = secIdList

    contract_details.__dict__ = work_con_det_dict

    return contract_details
###############################################################################
# AlgoTagValue
###############################################################################
# class AlgoTagValue(TagValue):
#     """Class for AlgoTagValue."""
#     def __init__(self, tag: str = None, value: str = None) -> None:
#         """Init method for AlgoTagValue.
#
#         Args:
#             tag: tag to be passed to the TagValue init
#             value: value to be passed to the TagValue init
#
#         """
#         super().__init__(tag, value)
#
#     def get_dict(self) -> Dict:
#         """Get dictionary to be used for DataFrame entry."""
#         return self.__dict__
###############################################################################
# AlgoComboLeg
###############################################################################
# class AlgoComboLeg(ComboLeg):
#     """Class for AlgoComboLeg."""
#
#     def __init__(self):
#         """Init method for AlgoComboLeg."""
#         super().__init__()
#
#     def get_dict(self) -> Dict:
#         """Get dictionary to be used for DataFrame entry."""
#         return self.__dict__
###############################################################################
# AlgoDeltaNeutralContract
###############################################################################
# class AlgoDeltaNeutralContract(DeltaNeutralContract):
#     """Class for AlgoDeltaNeutralContract."""
#
#     def __init__(self):
#         """Init method for AlgoDeltaNeutralContract."""
#         super().__init__()
#
#     def get_dict(self) -> Dict:
#         """Get dictionary to be used for DataFrame entry."""
#         return self.__dict__
###############################################################################
# AlgoContract
###############################################################################
# class AlgoContract(Contract):
#     """Class for AlgoContract."""
#
#     def __init__(self):
#         """Init method for AlgoContract"""
#         super().__init__()
#
#     def get_dict(self) -> Dict:
#         """Get dictionary to be used for DataFrame entry."""
#         ret_dict = self.__dict__
#         if self.comboLegs:
#             cl_list = []
#             for cl in self.comboLegs:
#                 cl_list.append(cl.get_dict())
#             ret_dict['comboLegs'] = str(tuple(cl_list))
#
#         if self.deltaNeutralContract:
#             dnc_dict = self.deltaNeutralContract.get_dict()
#             ret_dict['deltaNeutralContract'] = str(dnc_dict)
#
#         return ret_dict
#
###############################################################################
# AlgoContractDetails
###############################################################################
# class AlgoContractDetails(ContractDetails):
#     """Class for ib contract details."""
#
#     def __init__(self):
#         """Init method for AlgoContractDetails"""
#         super().__init__()
#
#     def get_dict(self) -> Dict:
#         """Get dictionary to be used for DataFrame entry."""
#         ret_dict = self.__dict__
#         if self.contract:
#             con_dict = self.contract.get_dict()
#             ret_dict['contract'] = str(con_dict)
#
#         if self.secIdList:
#             sid_list = []
#             for sid in self.secIdList:
#                 sid.append(sid.get_dict())
#             ret_dict['secIdList'] = str(tuple(sid_list))
#         return ret_dict
