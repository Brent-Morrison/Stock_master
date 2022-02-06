set batch_size=%1
set update_to_date=%2
set test_data_bool=%3
conda activate STOCK_MASTER && python C:\Users\brent\Documents\VS_Code\postgres\postgres\update_iex_prices.py %batch_size% %update_to_date% %test_data_bool%