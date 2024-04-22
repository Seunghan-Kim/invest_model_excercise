# %%
"""
This Job update trading results by order types
"""

# %%
IS_RUN_ON_DATABUTTON = False
# %%
import pandas as pd
import telebot

if IS_RUN_ON_DATABUTTON:
    from handler_google_sheet import GoogleSheetHandler
    from handler_KIS_api import KisApiHandler

    import databutton as db

else:
    from LIBRARIES.handler_google_sheet import GoogleSheetHandler
    from LIBRARIES.handler_KIS_api import KisApiHandler

    import json
    from dotenv import load_dotenv
    import os


# %%
gsh = GoogleSheetHandler()
kah = KisApiHandler()

# %%
# Get Today date
TODAY = pd.Timestamp.today(tz="Asia/Seoul").strftime("%Y-%m-%d")
# TODAY = "2023-12-15"

is_krx_open = kah.is_market_open(TODAY)
if not is_krx_open:
    print("It's holiday")
    assert False

# %%
# 1. Get Current Original Order Sheet
df_ = gsh.get_df_from_sheet("original_order")
sheet_replace_resp = gsh.replace_sheet_with_dataframe(
    "original_order_daily_backup", df_
)

# %%
# 2. Convert Original Order into merged form

l_of_dict = []
for name, group in df_.groupby("ord_id"):
    # print(name, group)
    model_name = name.split("-")[0]

    buy_qty = 0
    sell_qty = 0

    buy_money = 0
    sell_money = 0

    for idx, row in group.iterrows():
        created_at = row["created_date"]
        stock_code = row["stock_code"]
        stock_name = row["stock_name"]
        ord_id = row["ord_id"]

        if row["ord_type"] == "buy":
            buy_qty += int(row["set_qty"])
            buy_money += int(row["set_qty"]) * int(row["set_price"])
        else:
            sell_qty += int(row["set_qty"])
            sell_money += int(row["set_qty"]) * int(row["set_price"])

        if buy_qty > 0:
            buy_price = int(buy_money / buy_qty)
        else:
            buy_price = "0"

        if sell_qty > 0:
            sell_price = int(sell_money / sell_qty)
        else:
            sell_price = "0"

    l_of_dict.append(
        {
            "model_name": model_name,
            "created_at": created_at,
            "ord_id": ord_id,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "buy_qty": buy_qty,
            "sell_qty": sell_qty,
            "buy_price": buy_price,
            "sell_price": sell_price,
        }
    )

df_updated_by_model = pd.DataFrame(l_of_dict)


# %%
# 3. Get Orders finished
df_done = df_updated_by_model.loc[lambda df: df.buy_qty == df.sell_qty].assign(
    earn_money=lambda df: df.sell_price * df.sell_qty - df.buy_price * df.buy_qty,
    earn_ratio=lambda df: df.earn_money / (df.buy_price * df.buy_qty) * 100,
)

# 4. Split by model names

df_done_alicia = df_done.loc[lambda df: df.model_name.str.contains("alicia")]

df_done_cate_20d = df_done.loc[lambda df: df.model_name.isin(["cate01", "cate02"])]
df_done_cate_ml_345 = df_done.loc[
    lambda df: df.model_name.isin(["cate03", "cate05", "cate06"])
]
df_done_cate_kosdaq = df_done.loc[lambda df: df.model_name.isin(["cate04"])]

# %%
# 5.Drop finished orders from original sheet
all_ord_id = df_.ord_id.to_list()
done_ord_id = df_done.ord_id.to_list()

pending_ord_id = [x for x in all_ord_id if x not in done_ord_id]
# %%

df_orders_pending = df_.loc[lambda df: df.ord_id.isin(pending_ord_id)]

# %%
# Update Original Order Sheet - replace
sheet_replace_resp = gsh.replace_sheet_with_dataframe(
    "original_order", df_orders_pending
)
# %%
# Update model Sheet - add
gsh.append_rows_to_sheet("Alicia_results", df_done_alicia)
gsh.append_rows_to_sheet("Cate_20d_results", df_done_cate_20d)
gsh.append_rows_to_sheet("Cate_ML_345_results", df_done_cate_ml_345)
gsh.append_rows_to_sheet("Cate_Kosdaq_results", df_done_cate_kosdaq)


# %%
