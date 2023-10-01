#!/usr/bin/env python
# coding: utf-8

# In[21]:


from fastapi import FastAPI, Query, HTTPException
from sqlalchemy import create_engine, text
import pandas as pd

app = FastAPI()

# Define an SQLAlchemy database connection
database_uri = "sqlite:///../data/my_database.db"
engine = create_engine(database_uri, pool_pre_ping=True)

@app.get("/average_price")
async def get_average_price(metal: str = Query(..., description="Metal name (e.g., 'Copper', 'Zinc')"),
                            start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
                            end_date: str = Query(..., description="End date (YYYY-MM-DD)")):
    
    # Ensure that start_date is before end_date
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="Invalid date range. Start date must be before end date.")
    
    # Query the database to calculate the average price for the specified metal and date range
    query = text(f"""
        SELECT AVG(Price) AS avg_price
        FROM MetalPrices
        WHERE Metal = :metal
        AND Date >= :start_date
        AND Date <= :end_date
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, metal=metal, start_date=start_date, end_date=end_date)
        avg_price = result.scalar()
    
    if avg_price is None:
        raise HTTPException(status_code=404, detail=f"No data found for {metal} between {start_date} and {end_date}")
    
    return {"metal": metal, "start_date": start_date, "end_date": end_date, "average_price": avg_price}

