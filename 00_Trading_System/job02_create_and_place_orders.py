# %%
"""
This job runs around 15:20

1. Load saved price dataset
2. get stock list
3. get current price
4. merge price dataset
5. load models
6. create model recommendation
7. merge and create buy orders
8. update original order sheet
"""
# %%
IS_RUN_ON_DATABUTTON = False

# %%
import pandas as pd
import telebot

if IS_RUN_ON_DATABUTTON:
    from handler_KIS_api import KisApiHandler
    from handler_eBest_api import EBestApiHandler
    from handler_google_sheet import GoogleSheetHandler
    from defs_tools import (
        get_krx_stock_list,
        get_todays_buy_orders,
        get_df_offset_orders,
        get_kospi_kosdaq_close,
    )

    from model_alicia_2023 import Alicia2023
    from model_cate_2023 import Cate2023

    import databutton as db

else:
    from LIBRARIES.handler_KIS_api import KisApiHandler
    from LIBRARIES.handler_eBest_api import EBestApiHandler
    from LIBRARIES.handler_google_sheet import GoogleSheetHandler
    from LIBRARIES.defs_tools import *

    from LIBRARIES.model_alicia_2023 import Alicia2023
    from LIBRARIES.model_cate_2023 import Cate2023
    import json

    from dotenv import load_dotenv
    import os

# %%
try:
    if IS_RUN_ON_DATABUTTON:
        TELEGRAM_BOT_TOKEN = db.secrets.get("telegram_bot_01_token")
    else:
        load_dotenv()
        TELEGRAM_BOT_TOKEN = os.getenv("telegram_bot_01_token")
    tb = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
except Exception as err:
    print(f"telegram bot err : {err}")

try:
    tb.send_message(883866223, "Job02 - Create and Place Orders : Started")
except Exception as err:
    print(f"telegram bot err : {err}")

# %%
kah = KisApiHandler()
eah = EBestApiHandler()
gsh = GoogleSheetHandler()

# %%
# Get Today date
TODAY = pd.Timestamp.today(tz="Asia/Seoul").strftime("%Y-%m-%d")
# TODAY = "2023-12-08"

is_krx_open = kah.is_market_open(TODAY)
if not is_krx_open:
    print("It's holiday")
    assert False

# %%
# 1. Load saved price dataset
if IS_RUN_ON_DATABUTTON:
    df_price_n_days = db.storage.dataframes.get(key="df_price_n_days")
else:
    df_price_n_days = pd.read_pickle("./STORAGE/df_price_n_days.pkl")
print(f"01 : {df_price_n_days.tail()}")
# %%
# 2. get stock list
codes_list = df_price_n_days.code.unique().tolist()

# 3. get current price
df_current_price = eah.get_current_stock_price(codes_list, TODAY)
print(f"01-01 : {df_current_price.tail()}")
# 4. Add Stock Name
code_name_dict = df_current_price.set_index("code")["name"].to_dict()
df_price_n_days["name"] = df_price_n_days["code"].map(code_name_dict)

print(f"02 : {df_price_n_days.tail()}")

# %%
# 5. Merge price dataset
df_price = (
    pd.concat([df_price_n_days.loc[lambda df: df.date < TODAY], df_current_price])
    .sort_values(by=["code", "date"], ascending=[True, True])
    .reset_index(drop=True)
)

print(f"03 : {df_price.tail()}")

# %%
# 3. Get KRX Index mean of close
df_krx = get_kospi_kosdaq_close(100, TODAY)

# %%
# 4. Merge df_price and df_krx
df_price = df_price.merge(df_krx, on="date")

print(f"04 : {df_price.tail()}")


# %%
# 6. Add Market Cap.
df_krx_stock_list = get_krx_stock_list().drop_duplicates(subset="code")
code_marcap_dict = df_krx_stock_list.set_index("code")["marcap"].to_dict()

df_price["marcap"] = df_price["code"].map(code_marcap_dict)

print(f"05 : {df_price.tail()}")

# df_price = (
#     df_price.loc[lambda df: df.code.str[5] == 0]
#     .loc[lambda df: ~df.name.str.contains("스팩")]
#     .loc[lambda df: ~df.name.str.contains("스펙")]
# )
df_price = df_price.query(
    'code.str[5] == "0" and not name.str.contains("스팩") and not name.str.contains("스펙")'
)
print(f"06 : {df_price.tail()}")

try:
    tb.send_message(883866223, "Job02 - price dataset ready")
except Exception as err:
    print(f"telegram bot err : {err}")

# %%
# df_price.to_pickle("./temp.pkl")
# df_price= pd.read_pickle("./temp.pkl")
print("df_price current price added")
print(df_price.tail())

db.storage.dataframes.put("df_temp", df_price)
# df_price = db.storage.dataframes.get(key="df_temp")

try:
    tb.send_message(883866223, "Job02 - start model recommendation")
except Exception as err:
    print(f"telegram bot err : {err}")

# # %%
# 7. Get Model Recommendation
# 7.1. Model Alicia
model_alicia = Alicia2023(TODAY)
df_recommend_alicia = model_alicia.get_model_alicia_recommendation(df_price)

# %%
# 7.2. Model Cate
model_cate = Cate2023(TODAY, df_price)
df_recommend_cate = model_cate.get_model_cate_recommendation()

# %%
# 8. merge and create buy orders
df_recommends = pd.concat([df_recommend_alicia, df_recommend_cate])

# print(f"cate{df_recommend_cate}")

# %%
# 9. update original order sheet
# If recommendations exist
df_original_ord = gsh.get_df_from_sheet("original_order")

# %%
if df_recommends.shape[0] > 0:
    df_buy_orders_today = get_todays_buy_orders(df_recommends, TODAY)

    df_original_order_updated = pd.concat(
        [df_buy_orders_today, df_original_ord]
    ).drop_duplicates(subset=["ord_id", "ord_date"])

else:
    df_original_order_updated = df_original_ord

try:
    tb.send_message(883866223, "Job02 - model recommendation finished")
    tb.send_message(883866223, f"Job02 - total items : {df_recommends.shape[0]}")
except Exception as err:
    print(f"telegram bot err : {err}")

# %%
# 10. Create Offset Order
# Split Orders
df_order_today = df_original_order_updated.loc[lambda df: df.ord_date == TODAY]

df_prev_original_order = df_original_order_updated.loc[lambda df: df.ord_date != TODAY]

# %%
df_today_org_ord_updated, df_offset_orders = get_df_offset_orders(df_order_today, TODAY)

# %%
# update original order sheet / offset_ord_id updated
df_original_order_updated = pd.concat(
    [df_today_org_ord_updated, df_prev_original_order]
)

df_original_order_updated = df_original_order_updated.sort_values(
    by=["ord_id", "ord_date"], ascending=[True, False]
)

res = gsh.replace_sheet_with_dataframe("original_order", df_original_order_updated)

if not res:
    print("Original Order Sheet Update Fail")

# %%
# update offset order sheet with today's offset order
res = gsh.replace_sheet_with_dataframe("offset_order", df_offset_orders)

if not res:
    print("Offset Order Sheet Update Fail")
# %%
# 11. Place Orders & Update Offset Order Sheet
df_offset_ord_updated = eah.place_order_on_ebest(df_offset_orders)

res = gsh.replace_sheet_with_dataframe("offset_order", df_offset_ord_updated)

if not res:
    print("Offset Order Sheet Update Fail")

# %%
try:
    tb.send_message(883866223, "Job02 - Finished")
except Exception as err:
    print(f"telegram bot err : {err}")
