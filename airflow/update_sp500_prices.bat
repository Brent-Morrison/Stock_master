set database=%1
set update_to_date=%2
conda activate STOCK_MASTER && python C:\Users\brent\Documents\VS_Code\postgres\postgres\airflow\update_sp500_prices.py %database% %update_to_date%