"""scottbrian_algo1 algo_data.

=========
algo_data
=========

This class contains the data frames and methods used by the algo and
associated classes.
"""


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
    def symbolSamples(
        self,
        request_id: int,
        contract_descriptions: ibcommon.ListOfContractDescription,
    ) -> None:
        """Receive IB reply for reqMatchingSymbols request.

            Args:
            request_id: the id used on the request
            contract_descriptions: contains a list of contract descriptions.
            Each description includes the symbol,
            conId, security type, primary exchange,
            currency, and derivative security
            types.

        The contracts are filtered for stocks traded in the USA and are
        stored into a data frame as contracts that can be used later to
        request additional information or to make trades."""


        logger.info("entered for request_id %d", request_id)
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
                    self.symbols.loc[conId] = pd.Series(get_contract_description_dict(desc))
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