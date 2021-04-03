"""Mappings of various classes used in the AlgoApp class."""

from typing import Dict

from ibapi.contract import ComboLeg, Contract, ContractDetails  # type: ignore
from ibapi.contract import DeltaNeutralContract  # type: ignore
from ibapi.tag_value import TagValue  # type: ignore


###############################################################################
# get TagValue dictionary/obj
###############################################################################
def get_tag_value_dict(tag_value: TagValue) -> Dict:
    """Get dictionary to be used for DataFrame entry.

    Args:
        tag_value: instance of TagValue class
    """
    return tag_value.__dict__


def get_tag_value_obj(tag_value_dict: Dict) -> TagValue:
    """Get object from dictionary.

    Args:
        tag_value_dict: dictionary to be used to restore the tag_value

    Returns:
          An instance of TagValue

    """
    tag_value = TagValue()
    tag_value.__dict__ = tag_value_dict
    return tag_value

###############################################################################
# get ComboLeg dictionary/obj
###############################################################################
def get_combo_leg_dict(combo_leg: ComboLeg) -> Dict:
    """Get dictionary to be used for DataFrame entry.

    Args:
        combo_leg: instance of ComboLeg class
    """
    return combo_leg.__dict__

def get_combo_leg_obj(combo_leg_dict: Dict) -> ComboLeg:
    """Get object from dictionary.

    Args:
        combo_leg_dict: dictionary to be used to restore the combo_leg

    Returns:
          An instance of ComboLeg

    """
    combo_leg = ComboLeg()
    combo_leg.__dict__ = combo_leg_dict
    return combo_leg

###############################################################################
# get DeltaNeutralContract dictionary
###############################################################################
def get_delta_neutral_contract_dict(delta_neutral_contract:
                                    DeltaNeutralContract) -> Dict:
    """Get dictionary to be used for DataFrame entry.

    Args:
        delta_neutral_contract: instance of DeltaNeutralContract class
    """
    return delta_neutral_contract.__dict__


def get_delta_neutral_contract_obj(delta_neutral_contract_dict: Dict
                                   ) -> DeltaNeutralContract:
    """Get object from dictionary.

    Args:
        delta_neutral_contract_dict: dictionary to be used to restore
                                       the delta_neutral_contract

    Returns:
          An instance of DeltaNeutralContract

    """
    delta_neutral_contract = DeltaNeutralContract()
    delta_neutral_contract.__dict__ = delta_neutral_contract_dict
    return delta_neutral_contract


###############################################################################
# get Contract dictionary/obj
###############################################################################
def get_contract_dict(contract: Contract) -> Dict:
    """Get dictionary to be used for DataFrame entry.

    Args:
        contract: instance of Contract class
    """
    ret_dict = contract.__dict__
    if contract.comboLegs:
        combo_leg_list = []
        for combo_leg in contract.comboLegs:
            combo_leg_list.append(get_combo_leg_dict(combo_leg))
        ret_dict['comboLegs'] = str(tuple(combo_leg_list))

    if contract.deltaNeutralContract:
        dnc_dict = \
            get_delta_neutral_contract_dict(contract.deltaNeutralContract)
        ret_dict['deltaNeutralContract'] = str(dnc_dict)

    return ret_dict


def get_contract_obj(contract_dict: Dict) -> Contract:
    """Get object from dictionary.

    Args:
        contract_dict: dictionary to be used to restore contract

    Returns:
          An instance of Contract

    """
    contract = Contract()
    if contract_dict['comboLeg']:
        combo_leg = get_combo_leg_obj(eval(contract_dict['comboLeg']))
        contract_dict['comboLeg'] = combo_leg

    if contract_dict['deltaNeutralContract']:
        delta_neutral_contract = \
            get_delta_neutral_contract_obj(
                eval(contract_dict['deltaNeutralContract']))
        contract_dict['deltaNeutralContract'] = delta_neutral_contract

    contract.__dict__ = contract_dict

    return contract


###############################################################################
# get ContractDetails dictionary
###############################################################################
def get_contract_details_dict(contract_details: ContractDetails) -> Dict:
    """Get dictionary to be used for DataFrame entry.

    Args:
        contract_details: instance of ContractDetails class
    """
    ret_dict = contract_details.__dict__
    if contract_details.contract:
        contract_dict = get_contract_dict(contract_details.contract)
        ret_dict['contract'] = str(contract_dict)

    if contract_details.secIdList:
        sid_list = []
        for secId in contract_details.secIdList:
            sid_list.append(get_tag_value_dict(secId))
        ret_dict['secIdList'] = str(tuple(sid_list))

    return ret_dict


def get_contract_details_obj(contract_details_dict: Dict) -> ContractDetails:
    """Get object from dictionary.

    Args:
        contract_details_dict: dictionary to be used to restore
                                 contract_details

    Returns:
          An instance of contract_details

    """
    contract_details = ContractDetails()
    if contract_details_dict['contract']:
        contract = get_contract_obj(eval(contract_details_dict['contract']))
        contract_details_dict['contract'] = contract

    if contract_details_dict['secIdList']:
        secId_tuple = eval(contract_details_dict['secIdList'])
        secIdList = []
        for tag_value_dict in secId_tuple:
            secIdList.append(get_tag_value_obj(tag_value_dict))
        contract_details_dict['secIdList'] = secIdList

    contract_details.__dict__ = contract_details_dict

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
