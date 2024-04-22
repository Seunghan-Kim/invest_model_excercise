# %%
"""
This job runs before market close

1. Get KRX Stock List today
2. Get Price dataset n days
3. Save dataset
"""
# %%
IS_RUN_ON_DATABUTTON = False

# %%
import pandas as pd
import telebot

if IS_RUN_ON_DATABUTTON:
    from handler_KIS_api import KisApiHandler
    from defs_tools import get_krx_stock_list, get_kospi_kosdaq_close
    import databutton as db

else:
    from LIBRARIES.handler_KIS_api import KisApiHandler
    from LIBRARIES.defs_tools import *
    from dotenv import load_dotenv
    import os

# %%
kah = KisApiHandler()
# %%
# Get Today date
TODAY = pd.Timestamp.today(tz="Asia/Seoul").strftime("%Y-%m-%d")
# TODAY = "2023-12-08"

is_krx_open = kah.is_market_open(TODAY)
if not is_krx_open:
    print("It's holiday")
    assert False

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
    tb.send_message(883866223, "Job01 - Preparing price dataset : Started")
except Exception as err:
    print(f"telegram bot err : {err}")

# %%
# 1. Get KRX Stock List
df_krx_stock_list = get_krx_stock_list()
codes = df_krx_stock_list.code.unique().tolist()
codes.append("229200")

# %%
# 2. Get Price DataFrame
df_price_n_days = kah.get_df_price_n_days(codes, 90, TODAY)

# %%
# 5. Save Price Dataframe
if IS_RUN_ON_DATABUTTON:
    db.storage.dataframes.put("df_price_n_days", df_price_n_days)
else:
    df_price_n_days.to_pickle("./STORAGE/df_price_n_days.pkl")

# %%
try:
    tb.send_message(883866223, "Job01 - Preparing price dataset : Finished")
except Exception as err:
    print(f"telegram bot err : {err}")

# %%
