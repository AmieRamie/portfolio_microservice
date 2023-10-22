
import json
import pandas as pd
import pickle


class portfoliosDataService():

    def __init__(self, config: dict):
        """

        :param config: A dictionary of configuration parameters.
        """
        super().__init__()

        self.data_dir = config['data_directory']
        self.data_file = config["data_file"]
        self.students = []

        self._load()

    def _get_data_file_name(self):
        # DFF TODO Using os.path is better than string concat
        result = self.data_dir + "/" + self.data_file
        return result

    def _load(self):

        fn = self._get_data_file_name()
        with open(fn, 'rb') as f:
            self.portfolios  = pickle.load(f)

    def _save(self):
        fn = self._get_data_file_name()
        with open(fn, 'wb') as f:
            pickle.dump(self.portfolios, f)

    def get_portfolio(self, member_id: str = None, is_benchmark: bool = None) -> list:
        portfolio_df = pd.DataFrame(self.portfolios)

        if pd.isnull(member_id):
            if pd.isnull(is_benchmark):
                return self.portfolios
            else:
                return portfolio_df[portfolio_df['is_benchmark']==True].to_dict(orient='records')
        else:
            return portfolio_df[portfolio_df['member_id']==member_id].to_dict(orient='records')

    def add_portfolio(self) -> list:
        portfolio_df = pd.DataFrame(self.portfolios)
        new_member_id = max(portfolio_df['member_id']) + 1
        self.portfolios.append({'member_id':new_member_id,'is_benchmark':False,'portfolio_value':0,'portfolio':[]})
        self._save()
        return self.portfolios
    def add_holdings(self, member_id: str, stock_id:str ,body:dict) -> list:
        portfolio_df = pd.DataFrame(self.portfolios)
        current_index = portfolio_df[portfolio_df['member_id']==member_id].index[0]
        current_portfolio = portfolio_df[portfolio_df['member_id']==member_id]['portfolio'].iloc[0]
        if len(current_portfolio)>0:
            portfolio = pd.DataFrame(current_portfolio)
            if stock_id in list(portfolio['ticker']):
                new_total_shares = portfolio[portfolio['ticker'] == stock_id]['num_shares'].iloc[0]+body['num_shares']
                new_average_price_per_share = ((portfolio[portfolio['ticker'] == stock_id]['num_shares'].iloc[0]*portfolio[portfolio['ticker'] == 'AAPL']['avg_price'].iloc[0])+(body['num_shares']*body['price_per_share']))/new_total_shares
                portfolio.loc[portfolio['ticker']==stock_id,['num_shares']] = new_total_shares
                portfolio.loc[portfolio['ticker']==stock_id,['avg_price']] = new_average_price_per_share
                # Remember to update this when you connect to the stock microservice
                # portfolio.loc[portfolio['ticker']==stock_id,['current_price']] = requests.get(f"api/stocks/{stock_id}/price")
            else:
                #Remember to update 'current_price':42 to the actual live price when you connect to the stock microservice
                portfolio = pd.concat([portfolio,pd.DataFrame([{'ticker':stock_id,'num_shares':body['num_shares'],'avg_price':body['price_per_share'],'current_price':42}])])
        else:
            #Remember to update 'current_price':42 to the actual live price when you connect to the stock microservice
            portfolio = pd.DataFrame([{'ticker':stock_id,'num_shares':body['num_shares'],'avg_price':body['price_per_share'],'current_price':42}])        
        new_portfolio_value = float((portfolio['num_shares']*portfolio['current_price']).sum())
        new_portfolio = portfolio.to_dict(orient='records')
        portfolio_df.at[current_index,'portfolio'] = new_portfolio
        portfolio_df.at[current_index,'portfolio_value'] = new_portfolio_value
        self.portfolios = portfolio_df.to_dict(orient='records')
        self._save()
        return portfolio_df[portfolio_df['member_id']==member_id].to_dict(orient='records')
    
    def remove_holdings(self, member_id: str, stock_id:str ,body:dict) -> list:
        portfolio_df = pd.DataFrame(self.portfolios)
        current_index = portfolio_df[portfolio_df['member_id']==member_id].index[0]
        portfolio = pd.DataFrame(portfolio_df[portfolio_df['member_id']==member_id]['portfolio'].iloc[0])
        new_total_shares = portfolio[portfolio['ticker'] == stock_id]['num_shares'].iloc[0]-body['num_shares']
        portfolio.loc[portfolio['ticker']==stock_id,['num_shares']] = new_total_shares
        # Remember to update this when you connect to the stock microservice
        # portfolio.loc[portfolio['ticker']==stock_id,['current_price']] = requests.get(f"api/stocks/{stock_id}/price")
        new_portfolio_value = float((portfolio['num_shares']*portfolio['current_price']).sum())
        new_portfolio = portfolio.to_dict(orient='records')
        portfolio_df.at[current_index,'portfolio'] = new_portfolio
        portfolio_df.at[current_index,'portfolio_value'] = new_portfolio_value
        self.portfolios = portfolio_df.to_dict(orient='records')
        self._save()
        return portfolio_df[portfolio_df['member_id']==member_id].to_dict(orient='records')