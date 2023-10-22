from fastapi import FastAPI, Response, HTTPException
from fastapi.responses import RedirectResponse

import uvicorn


from portfolio.portfolio_resource import PortfolioResource
from portfolio.portfolio_data_service import portfoliosDataService
from portfolio.portfolio_model import PortfolioModel, trade_quantity_model
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

@app.get("/api/portfolios", response_model=List[PortfolioModel])
async def get_portfolios():
    result = portfolio_resource.get_portfolios()
    return result

@app.get("/api/portfolios/get_benchmarks", response_model=List[PortfolioModel])
async def get_benchmarks():
    result = portfolio_resource.get_portfolios(is_benchmark = True)
    return result

@app.get("/api/portfolios/get_leaderboard", response_model=List[PortfolioModel])
async def get_leaderboard():
    result = portfolio_resource.get_leaderboard()
    return result

@app.get("/api/portfolios/{member_id}", response_model=List[PortfolioModel])
async def get_specific_portfolio(member_id:int):
    result = portfolio_resource.get_portfolios(member_id = member_id)
    return result

@app.put("/api/portfolios/add_portfolio", response_model=List[PortfolioModel])
async def add_portfolio():
    result = portfolio_resource.add_portfolio()
    return result

@app.post("/api/portfolios/{member_id}/buy_stock/{stock_id}", response_model=List[PortfolioModel])
async def buy_stock(member_id:int,stock_id:str, item: trade_quantity_model):
    result = portfolio_resource.add_holdings(member_id,stock_id,item.dict())
    return result

@app.post("/api/portfolios/{member_id}/sell_stock/{stock_id}", response_model=List[PortfolioModel])
async def sell_stock(member_id:int,stock_id:str, item: trade_quantity_model):
    result = portfolio_resource.remove_holdings(member_id,stock_id,item.dict())
    return result

@app.post("/api/portfolios/update_portfolio_value/{member_id}", response_model=dict)
async def update_portfolio_value(member_id:int):
    #update the prices of every stock in their portfolio and get their portfolio value
    return {"message":"Still working on this endpoint"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8011)