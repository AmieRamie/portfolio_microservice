
import json
import pandas as pd
import pickle
from portfolio.database import database
from portfolio.portfolio_model import data_returns
import requests
import math
import sys

headers = {"x-api-key": "jHZjspQE0uA9tSL8eWwK5knja7tmnC81ekpOzGF8"}
db = database()

class portfoliosDataService():

    def __init__(self, config: dict): 
        """

        :param config: A dictionary of configuration parameters.
        """
        super().__init__()
        # self.db_connection = database()
        # self.cursor = self.db_connection.cursor
        # self.update_lambda_function_headers = {"x-api-key": "jHZjspQE0uA9tSL8eWwK5knja7tmnC81ekpOzGF8"}
        self.data_dir = config['data_directory']
        self.data_file = config["data_file"]
        self.students = []
    
    def get_portfolio_list(self, current_portfolio_info, current_holdings):
        all_portfolios = []
        print(current_portfolio_info.shape[0])
        for i in range(0,current_portfolio_info.shape[0]):
            row = current_portfolio_info.iloc[i]
            portfolio_dict = current_portfolio_info.iloc[i:i+1].to_dict('records')[0]
            member_id = row['member_id']
            portfolio_dict['portfolio'] = current_holdings.loc[current_holdings['member_id']==member_id,['ticker', 'num_shares', 'avg_price', 'current_price']].to_dict('records')
            all_portfolios.append(portfolio_dict)
        return all_portfolios


    def get_portfolio(self, query: str, limit: int, page:int) -> data_returns:
        if limit>50:
            return {'status_code':500,'text':'limit exceeded. Max number of portfolios per page is 50','body':{}}
        else:
            try:
                query_str = f"SELECT * FROM all_portfolio_info WHERE {query} LIMIT {limit} OFFSET {page*limit}" if pd.notnull(query) else f"SELECT * FROM all_portfolio_info LIMIT {limit} OFFSET {page*limit}"
                current_portfolio_info = pd.read_sql_query(query_str, db.conn)
                if current_portfolio_info.shape[0]==0:
                    return {'status_code':500,'text':'No rows with the given query string, limit, and page number were found','body':{}}
                else:
                    all_member_ids = ','.join(map(str,list(current_portfolio_info['member_id'])))
                    query_str = f"SELECT * FROM all_holdings WHERE member_id IN ({all_member_ids})"
                    current_holdings = pd.read_sql_query(query_str, db.conn)
                    all_portfolios = self.get_portfolio_list(current_portfolio_info, current_holdings)
                    # all_portfolios = []
                    # print(current_portfolio_info.shape[0])
                    # for i in range(0,current_portfolio_info.shape[0]):
                    #     row = current_portfolio_info.iloc[i]
                    #     portfolio_dict = current_portfolio_info.iloc[i:i+1].to_dict('records')[0]
                    #     member_id = row['member_id']
                    #     portfolio_dict['portfolio'] = current_holdings.loc[current_holdings['member_id']==member_id,['ticker', 'num_shares', 'avg_price', 'current_price']].to_dict('records')
                    #     all_portfolios.append(portfolio_dict)
                    db_cursor = db.conn.cursor()
                    row_count_query = f"SELECT count(*) FROM all_portfolio_info WHERE {query}" if pd.notnull(query) else f"SELECT count(*) FROM all_portfolio_info"
                    db_cursor.execute(row_count_query)
                    num_rows = db_cursor.fetchone()[0]
                    db_cursor.close()
                    links = []
                    links.append({'rel':"current",'href':f'/api/portfolios?limit={limit},page={page}'})
                    if page == 0:
                        links.append({'rel':"prev",'href':f''})
                    else:
                        links.append({'rel':"prev",'href':f'/api/portfolios?limit={limit},page={page-1}'})
                    if (page+1)<math.ceil(num_rows/limit):
                        links.append({'rel':"next",'href':f'/api/portfolios?limit={limit},page={page+1}'})
                    else:
                        links.append({'rel':"next",'href':f''})
                    return {'status_code':200,'text':'success!','body':{'data':all_portfolios,'links':links}}
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                print(e, exc_tb.tb_lineno)
                return {'status_code':500,'text': str(e),'body':{}}

    
    def get_leaderboard(self)->data_returns:
        top_10_portfolios_query = "SELECT *, (portfolio_value + cash_balance) AS sum_columns FROM all_portfolio_info ORDER BY sum_columns DESC LIMIT 10"
        top_10_portfolios = pd.read_sql_query(top_10_portfolios_query, db.conn)
        top_10_member_ids = ','.join(map(str,list(top_10_portfolios['member_id'])))
        query = f"SELECT * FROM all_holdings WHERE member_id IN ({top_10_member_ids})"
        top_10_portfolios_holdings = pd.read_sql_query(query, db.conn)
        all_portfolios = self.get_portfolio_list(top_10_portfolios, top_10_portfolios_holdings)
        # all_portfolios = []
        # for i in range(0,top_10_portfolios.shape[0]):
        #     row = top_10_portfolios.iloc[i]
        #     portfolio_dict = top_10_portfolios.iloc[i:i+1].drop(columns = ['sum_columns']).to_dict('records')[0]
        #     member_id = row['member_id']
        #     portfolio_dict['portfolio'] = top_10_portfolios_holdings.loc[top_10_portfolios_holdings['member_id']==member_id,['ticker', 'num_shares', 'avg_price', 'current_price']].to_dict('records')
        #     all_portfolios.append(portfolio_dict)
        return {'status_code':200,'text':'success!','body':{'data':all_portfolios}}
    def update_portfolio_value(self,member_id:int) -> data_returns:
        try:
            query = f"SELECT DISTINCT ticker FROM all_holdings WHERE member_id = {member_id}"
            cursor_val = db.conn.cursor()
            unique_tickers = pd.read_sql_query(query, db.conn)
            query = "UPDATE all_holdings SET current_price = %s WHERE ticker = %s"
            for ticker in unique_tickers['ticker']:
                res = requests.post('https://dph6awgc5h.execute-api.us-east-2.amazonaws.com/default/update_stock_info_containerized',json = {"ticker":ticker},headers = headers)
                current_price = res.json()['current_price']
                cursor_val.execute(query, (current_price, ticker))
            
                
            query = f"SELECT * FROM all_holdings WHERE member_id = {member_id}"
            current_holdings = pd.read_sql_query(query, db.conn)
            current_holdings_value = (current_holdings['current_price']*current_holdings['num_shares']).sum()
            query = "UPDATE all_portfolio_info SET portfolio_value = %s WHERE member_id = %s"
            cursor_val.execute(query, (current_holdings_value, member_id))
            query = f"SELECT * FROM all_portfolio_info WHERE member_id = {member_id}"
            current_portfolio_info = pd.read_sql_query(query, db.conn)
            portfolio_dict = current_portfolio_info.to_dict('records')[0]
            portfolio_dict['portfolio'] = current_holdings.loc[:,['ticker', 'num_shares', 'avg_price', 'current_price']].to_dict('records')
            cursor_val.close()
            return {'status_code':200,'text':'success!','body':{'data':[portfolio_dict]}}
        except Exception as e:
            print({'status_code':500,'text': str(e),'body':[]})
            return {'status_code':500,'text': str(e),'body':{}}


    def add_portfolio(self, member_id:int) -> data_returns:
        try:
            db_cursor = db.conn.cursor()
            check_query = "SELECT EXISTS(SELECT 1 FROM all_portfolio_info WHERE member_id = %s)"
            db_cursor.execute(check_query, (member_id,))
            exists = db_cursor.fetchone()[0]
            if exists:
                return {'status_code':500,'text': "member_id already exists!",'body':{}}
            else:
                data_portfolio_info = {'member_id': member_id,'is_benchmark': 0,'portfolio_value': 0,'cash_balance': 10000}
                columns = ', '.join(data_portfolio_info.keys())
                placeholders = ', '.join(['%s'] * len(data_portfolio_info))
                insert_query = f"INSERT INTO all_portfolio_info ({columns}) VALUES ({placeholders})"
                db_cursor.execute(insert_query, tuple(data_portfolio_info.values()))
                db.conn.commit()
                db_cursor.close()
                data_portfolio_info['portfolio'] = []
                return {'status_code':200,'text':'success!','body':{'data':[data_portfolio_info]}}
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e, exc_tb.tb_lineno)
            return {'status_code':500,'text': str(e),'body':{}}

    def delete_portfolio(self,member_id:str) -> list:
        try:
            query = f"SELECT * FROM all_holdings WHERE member_id = {member_id}"
            current_holdings = pd.read_sql_query(query, db.conn)
            query = f"SELECT * FROM all_portfolio_info WHERE member_id = {member_id}"
            current_portfolio_info = pd.read_sql_query(query, db.conn)
            all_portfolios = self.get_portfolio_list(current_portfolio_info, current_holdings)
            db_cursor = db.conn.cursor()
            delete_query = "DELETE FROM all_holdings WHERE member_id = %s"
            db_cursor.execute(delete_query, (member_id,))
            delete_query = "DELETE FROM all_portfolio_info WHERE member_id = %s"
            db_cursor.execute(delete_query, (member_id,))
            db_cursor.close()
            return {'status_code':200,'text':'success!','body':{'data':all_portfolios}}
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e, exc_tb.tb_lineno)
            db_cursor.close()
            return {'status_code':500,'text': str(e),'body':{}}


    def add_holdings(self, member_id: str, stock_id:str ,body:dict) -> list:
        try:
            current_price = body['price_per_share']
            num_shares = body['num_shares']
            db_cursor = db.conn.cursor()
            check_query = "SELECT EXISTS(SELECT 1 FROM all_holdings WHERE member_id = %s && ticker = %s)"
            db_cursor.execute(check_query, (member_id,stock_id,))
            member_owns_ticker = bool(db_cursor.fetchone()[0])
            print(member_owns_ticker)
            if member_owns_ticker:
                #getting avg. price per share for the previous shares bought and the number of shares bought already in order to calculate the new avg. price per share and the new number of shares
                query = "SELECT * FROM all_holdings WHERE member_id = %s && ticker = %s"
                current_ticker_holdings = pd.read_sql_query(query, db.conn,params = (member_id,stock_id,))
                cost_of_transaction = current_price*num_shares
                #getting old values of the portfolio and cash values
                query = f"SELECT * FROM all_portfolio_info WHERE member_id = {member_id}"
                old_portfolio_info = pd.read_sql_query(query, db.conn)
                portfolio_value = old_portfolio_info['portfolio_value'].iloc[0]
                cash_balance = old_portfolio_info['cash_balance'].iloc[0]
                if cash_balance<cost_of_transaction:
                    db_cursor.close()
                    return {'status_code':500,'text': 'member does not have enough cash in their account','body':{}}
                else:
                    new_num_shares = current_ticker_holdings['num_shares'].iloc[0]+num_shares
                    new_avg_price = (current_ticker_holdings['avg_price'].iloc[0]*current_ticker_holdings['num_shares'].iloc[0] + cost_of_transaction)/new_num_shares
                    #updating the avg. price per share and the number of shares in the SQL table
                    update_query = "UPDATE all_holdings SET num_shares = %s, avg_price = %s, current_price = %s WHERE member_id = %s && ticker = %s"
                    db_cursor.execute(update_query, (new_num_shares, new_avg_price, current_price, member_id,stock_id))       
                    #updating the new value of the portfolio and cash values
                    new_portfolio_value = portfolio_value+cost_of_transaction
                    new_cash_balance = cash_balance - cost_of_transaction
                    update_query = "UPDATE all_portfolio_info SET portfolio_value = %s, cash_balance = %s WHERE member_id = %s"
                    db_cursor.execute(update_query, (new_portfolio_value, new_cash_balance, member_id))     

                    query = f"SELECT * FROM all_holdings WHERE member_id = {member_id}"
                    current_holdings = pd.read_sql_query(query, db.conn)
                    query = f"SELECT * FROM all_portfolio_info WHERE member_id = {member_id}"
                    current_portfolio_info = pd.read_sql_query(query, db.conn)
                    all_portfolios = self.get_portfolio_list(current_portfolio_info, current_holdings)
                    db_cursor.close()
                    return {'status_code':200,'text':'success!','body':{'data':all_portfolios}}
            else:
                cost_of_transaction = current_price*num_shares
                #getting old values of the portfolio and cash values
                query = f"SELECT * FROM all_portfolio_info WHERE member_id = {member_id}"
                old_portfolio_info = pd.read_sql_query(query, db.conn)
                portfolio_value = old_portfolio_info['portfolio_value'].iloc[0]
                cash_balance = old_portfolio_info['cash_balance'].iloc[0]            
                if cash_balance<cost_of_transaction:
                    db_cursor.close()
                    return {'status_code':500,'text': 'member does not have enough cash in their account','body':{}}
                else:
                    new_holdings_data = {'member_id': member_id,'ticker': stock_id,'num_shares': num_shares,'avg_price': current_price,'current_price': current_price}
                    columns = ', '.join(new_holdings_data.keys())
                    placeholders = ', '.join(['%s'] * len(new_holdings_data))
                    insert_query = f"INSERT INTO all_holdings ({columns}) VALUES ({placeholders})"
                    db_cursor.execute(insert_query, tuple(new_holdings_data.values()))
                    db.conn.commit()
                    #updating the new value of the portfolio and cash values
                    new_portfolio_value = portfolio_value+cost_of_transaction
                    new_cash_balance = cash_balance - cost_of_transaction
                    update_query = "UPDATE all_portfolio_info SET portfolio_value = %s, cash_balance = %s WHERE member_id = %s"
                    db_cursor.execute(update_query, (new_portfolio_value, new_cash_balance, member_id))  

                    query = f"SELECT * FROM all_holdings WHERE member_id = {member_id}"
                    current_holdings = pd.read_sql_query(query, db.conn)
                    query = f"SELECT * FROM all_portfolio_info WHERE member_id = {member_id}"
                    current_portfolio_info = pd.read_sql_query(query, db.conn)
                    all_portfolios = self.get_portfolio_list(current_portfolio_info, current_holdings)
                    db_cursor.close()
                    return {'status_code':200,'text':'success!','body':{'data':all_portfolios}}       
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e, exc_tb.tb_lineno)
            db_cursor.close()
            return {'status_code':500,'text': str(e),'body':{}}     

    
    def remove_holdings(self, member_id: str, stock_id:str ,body:dict) -> list:
        try:
            current_price = body['price_per_share']
            num_shares_sold = body['num_shares']
            cost_of_transaction = current_price*num_shares_sold
            db_cursor = db.conn.cursor()
            check_query = "SELECT EXISTS(SELECT 1 FROM all_holdings WHERE member_id = %s && ticker = %s)"
            db_cursor.execute(check_query, (member_id,stock_id,))
            member_owns_ticker = bool(db_cursor.fetchone()[0])
            print(member_owns_ticker)
            if member_owns_ticker==False:
                db_cursor.close()
                return {'status_code':500,'text': 'member does not have stock in their portfolio','body':{}}
            else:
                query = "SELECT * FROM all_holdings WHERE member_id = %s && ticker = %s"
                current_ticker_holdings = pd.read_sql_query(query, db.conn,params = (member_id,stock_id,))
                current_num_shares = current_ticker_holdings['num_shares'].iloc[0]
                print(current_num_shares)
                print(num_shares_sold)
                if current_num_shares<num_shares_sold:
                    db_cursor.close()
                    return {'status_code':500,'text': 'member does not have enough shares in their portfolio','body':{}}
                elif current_num_shares==num_shares_sold:
                    delete_query = "DELETE FROM all_holdings WHERE member_id = %s && ticker = %s"
                    db_cursor.execute(delete_query, (member_id,stock_id,))
                    query = f"SELECT * FROM all_portfolio_info WHERE member_id = {member_id}"
                    old_portfolio_info = pd.read_sql_query(query, db.conn)
                    portfolio_value = old_portfolio_info['portfolio_value'].iloc[0]
                    cash_balance = old_portfolio_info['cash_balance'].iloc[0]
                    new_portfolio_value = portfolio_value - cost_of_transaction
                    new_cash_balance = cash_balance + cost_of_transaction

                    update_query = "UPDATE all_portfolio_info SET portfolio_value = %s, cash_balance = %s WHERE member_id = %s"
                    db_cursor.execute(update_query, (new_portfolio_value, new_cash_balance, member_id))  

                    query = f"SELECT * FROM all_holdings WHERE member_id = {member_id}"
                    current_holdings = pd.read_sql_query(query, db.conn)
                    query = f"SELECT * FROM all_portfolio_info WHERE member_id = {member_id}"
                    current_portfolio_info = pd.read_sql_query(query, db.conn)
                    all_portfolios = self.get_portfolio_list(current_portfolio_info, current_holdings)
                    db_cursor.close()                   
                    return {'status_code':200,'text':'success!','body':{'data':all_portfolios}}
                else:
                    new_num_shares = current_num_shares - num_shares_sold
                    #updating the number of shares in the SQL table
                    update_query = "UPDATE all_holdings SET num_shares = %s, current_price = %s WHERE member_id = %s && ticker = %s"
                    db_cursor.execute(update_query, (new_num_shares, current_price, member_id,stock_id))
                    query = f"SELECT * FROM all_portfolio_info WHERE member_id = {member_id}"
                    old_portfolio_info = pd.read_sql_query(query, db.conn)
                    portfolio_value = old_portfolio_info['portfolio_value'].iloc[0]
                    cash_balance = old_portfolio_info['cash_balance'].iloc[0]
                    new_portfolio_value = portfolio_value - cost_of_transaction
                    new_cash_balance = cash_balance + cost_of_transaction

                    update_query = "UPDATE all_portfolio_info SET portfolio_value = %s, cash_balance = %s WHERE member_id = %s"
                    db_cursor.execute(update_query, (new_portfolio_value, new_cash_balance, member_id))  

                    query = f"SELECT * FROM all_holdings WHERE member_id = {member_id}"
                    current_holdings = pd.read_sql_query(query, db.conn)
                    query = f"SELECT * FROM all_portfolio_info WHERE member_id = {member_id}"
                    current_portfolio_info = pd.read_sql_query(query, db.conn)
                    all_portfolios = self.get_portfolio_list(current_portfolio_info, current_holdings)
                    db_cursor.close()               
                    return {'status_code':200,'text':'success!','body':{'data':all_portfolios}}
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(e, exc_tb.tb_lineno)
            db_cursor.close()
            return {'status_code':500,'text': str(e),'body':{}}     