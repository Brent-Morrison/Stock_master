set database=%1
set batch_size=%2
set update_to_date=%3
set test_data_bool=%4
conda activate STOCK_MASTER && python C:\Users\brent\Documents\VS_Code\postgres\postgres\airflow\update_iex_prices.py %database% %batch_size% %update_to_date% %test_data_bool%