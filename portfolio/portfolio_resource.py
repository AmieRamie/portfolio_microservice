
from portfolio.portfolio_model import PortfolioModel
from typing import List
import pandas as pd


class PortfolioResource():
    #
    # This code is just to get us started.
    # It is also pretty sloppy code.
    #

    def __init__(self, config):
        super().__init__()
        self.data_service = config["data_service"]

    def get_portfolios(self, member_id: str = None, is_benchmark: bool = None) -> List[PortfolioModel]:
        result = self.data_service.get_portfolio(member_id, is_benchmark)
        return result

    def add_portfolio(self):
        result = self.data_service.add_portfolio()
        return result

    def delete_portfolio(self):
        result = self.data_service.delete_portfolio()
        return result

    def get_leaderboard(self, leaderboard_size = 10) -> List[PortfolioModel]:
        result = self.data_service.get_portfolio()
        portfolio_df = pd.DataFrame(result)
        return portfolio_df.sort_values(by = ['portfolio_value'], ascending = [False]).iloc[0:leaderboard_size].to_dict(orient='records')

    def add_holdings(self, member_id: str, stock_id:str ,body:dict) -> List[PortfolioModel]:
        result = self.data_service.add_holdings(member_id, stock_id,body)
        return result

    def remove_holdings(self, member_id: str, stock_id:str ,body:dict) -> List[PortfolioModel]:
        result = self.data_service.remove_holdings(member_id, stock_id,body)
        return result    
