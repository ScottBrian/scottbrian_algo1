"""scottbrian_algo1 algo_data.

=========
algo_data
=========

This class contains the data frames and methods used by the algo and
associated classes.
"""
########################################################################
# Standard Library
########################################################################
from typing import Any, Callable, Optional, Type, TYPE_CHECKING, Union

########################################################################
# Third Party
########################################################################
import pandas as pd

########################################################################
# Local
########################################################################


########################################################################
# Shared Data Area
########################################################################
class MarketData:
    """Collection of market data areas."""

    def __init__(self):
        self.symbols_status = pd.DataFrame()
        # self.num_symbols_received = 0
        self.symbols = pd.DataFrame()
        self.stock_symbols = pd.DataFrame()

        # contract details
        self.contracts = pd.DataFrame()
        self.contract_details = pd.DataFrame()

        # fundamental data
        # self.fundamental_data = pd.DataFrame()

    ####################################################################
    # symbolSamples - callback
    ####################################################################
    def update_symbols(
        self,
        symbol_descriptions: list[dict[str, Any]],
    ) -> None:
        """Update dataframe for symbols from reqMatchingSymbols request.

        Args:
            symbol_descriptions: contains a list of symbol descriptions.
                Each description includes the condID, symbol, security
                type, primary exchange, currency, and derivative
                security types.
        """
        updates: list[dict[str, Any]] = []
        concats: list[dict[str, Any]] = []
        for entry in symbol_descriptions:
            if entry["conID"] in self.stock_symbols.index:
                updates.append(entry)
            else:
                concats.append(entry)
        if updates:
            updates_df = pd.DataFrame(updates)
            updates_df.set_index("conID", inplace=True)
            self.stock_symbols.update(updates_df)

        if concats:
            concats_df = pd.DataFrame(concats)
            concats_df.set_index("conID", inplace=True)
            self.stock_symbols = pd.concat([self.stock_symbols,
                                            concats,])

        concates_df = pd.DataFrame(updates)
        new_df.set_index("conID", inplace=True)

        self.num_symbols_received = len(contract_descriptions)
        logger.info("Number of descriptions received: %d", self.num_symbols_received)

        for desc in contract_descriptions:
            logger.debug("Symbol: {}".format(desc.contract.symbol))

            conId = desc.contract.conId

            if (
                desc.contract.secType == "STK"
                and desc.contract.currency == "USD"
                and "OPT" in desc.derivativeSecTypes
            ):
                if conId in self.stock_symbols.index:
                    self.stock_symbols.loc[conId] = pd.Series(
                        get_contract_description_dict(desc)
                    )
                else:
                    self.stock_symbols = pd.concat(
                        [
                            self.stock_symbols,
                            pd.DataFrame(
                                get_contract_description_dict(desc, df=True),
                                index=[conId],
                            ),
                        ]
                    )
            else:  # all other symbols
                # update the descriptor if it already exists in the DataFrame
                # as we want the newest information to replace the old
                if conId in self.symbols.index:
                    self.symbols.loc[conId] = pd.Series(
                        get_contract_description_dict(desc)
                    )
                else:
                    self.symbols = pd.concat(
                        [
                            self.symbols,
                            pd.DataFrame(
                                get_contract_description_dict(desc, df=True),
                                index=[conId],
                            ),
                        ]
                    )

        self.response_complete_event.set()
