# %%
"""
This job runs after market closed

1. Load order sheets : Original Order / Offset Order
2. Check order execution result
3. Update Offset Order Sheet
4. Update Original Order Sheet
5. Update Model Sheet
"""

IS_RUN_ON_DATABUTTON = False

# %%
# Import Modules
import pandas as pd

if IS_RUN_ON_DATABUTTON:
    from handler_google_sheet import GoogleSheetHandler
    from handler_KIS_api import KisApiHandler
    from handler_eBest_api import EBestApiHandler
    from defs_tools import (
        update_all_done_offset_orders,
        update_all_fail_offset_orders,
        update_partially_sell_offset_orders,
        get_new_sell_order,
        update_partially_buy_offset_orders,
    )

    import databutton as db

else:
    from LIBRARIES.handler_google_sheet import GoogleSheetHandler
    from LIBRARIES.handler_KIS_api import KisApiHandler
    from LIBRARIES.handler_eBest_api import EBestApiHandler
    from LIBRARIES.defs_tools import *

    import json

# %%
# Initialize Instances
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
# 1. Load Order Sheets
df_org_order = gsh.get_df_from_sheet("original_order")
# df_org_order['ord_qty'] = df_org_order['ord_qty'].astype(int)
# df_org_order['set_price'] = df_org_order['set_price'].astype(int)
# df_org_order['set_qty'] = df_org_order['set_qty'].astype(int)

df_org_order_today = df_org_order.loc[lambda df: df.ord_date == TODAY]
df_org_order_left = df_org_order.loc[lambda df: df.ord_date != TODAY]

df_offset_order = gsh.get_df_from_sheet("offset_order")
df_offset_order["ord_qty"] = df_offset_order["ord_qty"].astype(int)
df_offset_order["rjct_qty"] = df_offset_order["rjct_qty"].astype(int)
df_offset_order["set_price"] = df_offset_order["set_price"].astype(int)

df_offset_order = df_offset_order.loc[lambda df: df.ord_date == TODAY]

# %%
# 2. Check order execution result - Update rejected_qty / Set Price
check_order_results = eah.get_order_placed_results()

try:
    order_results = check_order_results["t0425OutBlock1"]
    # this is list of dictionary
except Exception as err:
    print(f"No orders today : {err}")
    assert False

df_offset_column_order = df_offset_order.columns
df_offset_order_temp = df_offset_order.set_index("stock_code")
codes_from_system_trading = [
    x.split("-")[2] for x in df_offset_order.offset_ord_id.to_list()
]

# 주문 결과로 잔여 수량과 체결가 업데이트
for result in order_results:
    if result["expcode"] in codes_from_system_trading:
        df_offset_order_temp.loc[result["expcode"], "rjct_qty"] = int(result["ordrem"])
        df_offset_order_temp.loc[result["expcode"], "set_price"] = int(
            result["cheprice"]
        )

# 주문이 들어가지 않은 것들 처리 : 주문 수량이 0 이었거나, 실패한 것
# 주문 수량이 0인 것은 가격 업데이트 필요
# 주문 실패한 것은 가격 업데이트 불필요

codes_ord_qty_zero = df_offset_order_temp.loc[lambda df: df["ord_qty"] == 0].index

if codes_ord_qty_zero.__len__() > 0:
    df_curr_price_of_zero_qty = eah.get_current_stock_price(codes_ord_qty_zero, TODAY)

    code_close_dict_zero_qty = df_curr_price_of_zero_qty.set_index("code")[
        "close"
    ].to_dict()

    for code, close in code_close_dict_zero_qty.items():
        df_offset_order_temp.loc[code, "set_price"] = close

df_offset_order = df_offset_order_temp.reset_index()[df_offset_column_order]

print(df_offset_order)

# 3. Update offset order sheet
res = gsh.replace_sheet_with_dataframe("offset_order", df_offset_order)
if res:
    pass
else:
    print("Update order check result failed")
    assert False

# %%
# 3. Update Original Order Sheet

l_df_new_sell_order = []
l_df_original_order_updated = []
for idx, row in df_offset_order.iterrows():
    ord_qty = int(row["ord_qty"])
    rjct_qty = int(row["rjct_qty"])
    set_price = int(row["set_price"])
    offset_ord_id = row["offset_ord_id"]

    df_temp = df_org_order_today.loc[lambda df: df["offset_ord_id"] == offset_ord_id]
    df_temp["ord_qty"] = df_temp["ord_qty"].astype(int)

    if rjct_qty == 0:  # all done
        print("All Done")
        df_updated = update_all_done_offset_orders(df_temp, set_price)
        l_df_original_order_updated.append(df_updated)

        df_buys = df_updated.loc[lambda df: df.ord_type == "buy"].loc[
            lambda df: df.ord_qty > 0
        ]

        if df_buys.shape[0] > 0:
            l_df_new_sell_order.append(get_new_sell_order(df_buys))

    elif rjct_qty == ord_qty:  # all fail
        print("Failed")
        print(row)
        print(df_temp)
        df_updated = update_all_fail_offset_orders(df_temp)
        l_df_original_order_updated.append(df_updated)

    else:  # partial
        print("PArtial")
        df_temp_sell = df_temp.loc[lambda df: df.ord_type == "sell"].sort_values(
            by="created_date", ascending=True
        )  # The latest first

        df_temp_buy = df_temp.loc[lambda df: df.ord_type == "buy"]

        if df_temp_sell.shape[0] == 0:  # Only buy orders exist
            # update buy orders - cancel remains
            df_updated = update_partially_buy_offset_orders(
                df_temp_buy, rjct_qty, set_price
            )
            l_df_original_order_updated.append(df_updated)

            l_df_new_sell_order.append(
                get_new_sell_order(df_updated)  # updated buy order
            )

        elif df_temp_buy.shape[0] == 0:  # Only sell orders exist
            # update sell orders
            org_sell_qty = df_temp_sell["ord_qty"].sum()
            executed_qty = org_sell_qty - rjct_qty

            df_updated, _new_sell_order = update_partially_sell_offset_orders(
                df_temp_sell, executed_qty, set_price
            )

            l_df_original_order_updated.append(df_updated)
            l_df_new_sell_order.append(_new_sell_order)

        else:  # Buy and Sell orders exist both
            if row["ord_type"] == "sell":  # All buy orders are to be fully executed
                # update sell orders & re-order failed
                org_sell_qty = df_temp_sell["ord_qty"].sum()
                executed_qty = org_sell_qty - rjct_qty

                df_updated, _new_sell_order = update_partially_sell_offset_orders(
                    df_temp_sell, executed_qty, set_price
                )

                l_df_original_order_updated.append(df_updated)
                l_df_new_sell_order.append(_new_sell_order)

                # update buy order & create new orders
                df_updated = update_all_done_offset_orders(df_temp_buy, set_price)
                l_df_original_order_updated.append(df_updated)

                l_df_new_sell_order.append(get_new_sell_order(df_temp_buy))

            else:  # All sell orders are to be fully executed
                # update sell order
                df_updated = update_all_done_offset_orders(df_temp_sell, set_price)
                l_df_original_order_updated.append(df_updated)

                # update buy orders & create new sell orders
                df_updated = update_partially_buy_offset_orders(
                    df_temp_buy, rjct_qty, set_price
                )
                l_df_original_order_updated.append(df_updated)

                l_df_new_sell_order.append(
                    get_new_sell_order(df_updated)  # updated buy order
                )

df_original_updated_today = pd.concat(l_df_original_order_updated)
df_new_sell_order_today = pd.concat(l_df_new_sell_order)

df_updated_orders = pd.concat(
    [df_original_updated_today, df_new_sell_order_today, df_org_order_left]
).sort_values(by=["ord_date", "ord_id"], ascending=[True, True])

res = gsh.replace_sheet_with_dataframe("original_order", df_updated_orders)
if res:
    pass
else:
    print("Update offset order sheet failed")
    assert False


# %%

# %%
