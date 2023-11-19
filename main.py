from fastapi import FastAPI, Response, HTTPException
from fastapi.responses import RedirectResponse

import uvicorn


from portfolio.portfolio_resource import PortfolioResource
from portfolio.portfolio_data_service import portfoliosDataService
from portfolio.portfolio_model import non_pagination_model, pagination_model, trade_quantity_model, sql_query_str, PortfolioModel
from typing import List

app = FastAPI()


def get_data_service():

    config = {
        "data_directory": "./data",
        "data_file": "portfolios.pkl"
    }

    ds = portfoliosDataService(config)
    return ds
@app.get("/")
async def root():
    return {'message':'This works!'}

def get_portfolio_resource():
    ds = get_data_service()
    config = {
        "data_service": ds
    }
    res = PortfolioResource(config)
    return res

portfolio_resource = get_portfolio_resource()

# @app.get("/api/portfolios", response_model=List[PortfolioModel])
# async def get_portfolios():
#     result = portfolio_resource.get_portfolios()
#     return result

# @app.get("/api/portfolios/get_benchmarks", response_model=List[PortfolioModel])
# async def get_benchmarks():
#     result = portfolio_resource.get_portfolios(is_benchmark = True)
#     return result

@app.get("/api/portfolios/get_leaderboard", response_model=non_pagination_model)
async def get_leaderboard():
    result = portfolio_resource.get_leaderboard()
    if result['status_code']!=200:
        raise HTTPException(status_code=result['status_code'], detail=result['text'])
    else:
        data = result['body']
        return data

@app.get("/api/portfolios/", response_model=pagination_model)
async def get_specific_portfolio(query_str: str = None, limit: int = 25, page: int = 0):
    print(query_str, limit, page)
    result = portfolio_resource.get_portfolios(query_str, limit, page)
    if result['status_code']!=200:
        raise HTTPException(status_code=result['status_code'], detail=result['text'])
    else:
        data = result['body']
        return data

@app.put("/api/portfolios/add_portfolio/{new_member_id}", response_model=non_pagination_model)
async def add_portfolio(new_member_id:int):
    result = portfolio_resource.add_portfolio(new_member_id)
    if result['status_code']!=200:
        raise HTTPException(status_code=result['status_code'], detail=result['text'])
    else:
        data = result['body']
        return data


@app.delete("/api/portfolios/delete_portfolio/{member_id}", response_model=non_pagination_model)
async def delete_portfolio(member_id:int):
    result = portfolio_resource.delete_portfolio(member_id)
    print(result)
    if result['status_code']!=200:
        raise HTTPException(status_code=result['status_code'], detail=result['text'])
    else:
        data = result['body']
        return data

@app.post("/api/portfolios/{member_id}/buy_stock/{stock_id}", response_model=non_pagination_model)
async def buy_stock(member_id:int,stock_id:str, item: trade_quantity_model):
    result = portfolio_resource.add_holdings(member_id,stock_id,item.dict())
    if result['status_code']!=200:
        raise HTTPException(status_code=result['status_code'], detail=result['text'])
    else:
        data = result['body']
        return data


@app.post("/api/portfolios/{member_id}/sell_stock/{stock_id}", response_model=non_pagination_model)
async def sell_stock(member_id:int,stock_id:str, item: trade_quantity_model):
    result = portfolio_resource.remove_holdings(member_id,stock_id,item.dict())
    if result['status_code']!=200:
        raise HTTPException(status_code=result['status_code'], detail=result['text'])
    else:
        data = result['body']
        return data


@app.post("/api/portfolios/update_portfolio_value/{member_id}", response_model=non_pagination_model)
async def update_portfolio_value(member_id:int):
    result = portfolio_resource.update_portfolio_value(member_id)
    if result['status_code']!=200:
        raise HTTPException(status_code=result['status_code'], detail=result['text'])
    else:
        data = result['body']
        return data


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8015)