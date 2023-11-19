
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

    def get_portfolios(self, query, limit, page) -> List[PortfolioModel]:
        result = self.data_service.get_portfolio(query, limit, page)
        return result
    def update_portfolio_value(self,member_id):
        result = self.data_service.update_portfolio_value(member_id)
        return result

    def add_portfolio(self,new_member_id):
        result = self.data_service.add_portfolio(new_member_id)
        return result

    def delete_portfolio(self,member_id):
        result = self.data_service.delete_portfolio(member_id)
        return result

    def get_leaderboard(self, leaderboard_size = 10) -> List[PortfolioModel]:
        result = self.data_service.get_leaderboard()
        return result

    def add_holdings(self, member_id: str, stock_id:str ,body:dict) -> List[PortfolioModel]:
        result = self.data_service.add_holdings(member_id, stock_id,body)
        return result

    def remove_holdings(self, member_id: str, stock_id:str ,body:dict) -> List[PortfolioModel]:
        result = self.data_service.remove_holdings(member_id, stock_id,body)
        return result    
