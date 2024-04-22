IS_RUN_ON_DATABUTTON = False

import pandas as pd
import FinanceDataReader as fdr

if IS_RUN_ON_DATABUTTON:
    from handler_KIS_api import KisApiHandler
else:
    from LIBRARIES.handler_KIS_api import KisApiHandler

kah = KisApiHandler()


# Order Sheet Updater
def update_all_done_offset_orders(df_, price):
    """
    For fully traded offset orders, update the result
    """
    l_df_updated = []
    for idx, row in df_.iterrows():
        row["set_qty"] = row["ord_qty"]
        row["set_price"] = price
        row["status"] = "done"

        l_df_updated.append(row.to_frame().T)
    return pd.concat(l_df_updated)


def update_all_fail_offset_orders(df_):
    """
    For fully failed offset orders, update the result
    """
    l_df_updated = []
    for idx, row in df_.iterrows():
        if row["ord_type"] == "sell":
            org_ord_date = row["ord_date"]
            next_bdate = kah.get_bdate_n_days_later(1, org_ord_date)
            row["ord_date"] = next_bdate
            row["offset_ord_id"] = "na"
        else:
            row["status"] = "cancel"

        l_df_updated.append(row.to_frame().T)
    return pd.concat(l_df_updated)


def update_partially_sell_offset_orders(df_, executed_qty, set_price):
    """
    For patially executed sell offset orders, update the result
    """
    remain = executed_qty
    l_df_new_sell_order = []
    l_df_updated_order = []

    for idx, row in df_.iterrows():
        if remain > 0:
            diff = remain - int(row["ord_qty"])
            if diff >= 0:
                row["set_price"] = set_price
                row["set_qty"] = row["ord_qty"]
                row["status"] = "done"
                remain = remain - int(row["ord_qty"])

            else:
                org_ord_date = row["ord_date"]
                next_bdate = kah.get_bdate_n_days_later(1, org_ord_date)
                remain_sell_qty = int(row["ord_qty"]) - remain

                l_df_new_sell_order.append(
                    pd.DataFrame(
                        [
                            {
                                "ord_date": next_bdate,
                                "created_date": row["created_date"],
                                "ord_id": row[
                                    "ord_id"
                                ],  # ord_id is created when buy order is created
                                "stock_code": row["stock_code"],
                                "stock_name": row["stock_name"],
                                "ord_type": "sell",
                                "ord_qty": str(remain_sell_qty),
                                "set_price": str(0),
                                "set_qty": str(0),
                                "offset_ord_id": "na",
                                "status": "pending",
                            }
                        ]
                    )
                )

                row["set_qty"] = str(remain)
                row["set_price"] = str(set_price)
                row["status"] = "done"
                remain = 0
        else:
            org_ord_date = row["ord_date"]
            next_bdate = kah.get_bdate_n_days_later(1, org_ord_date)
            row["ord_date"] = next_bdate
            row["offset_ord_id"] = "na"

        l_df_updated_order.append(row.to_frame().T)
    return pd.concat(l_df_updated_order), pd.concat(l_df_new_sell_order)


def update_partially_buy_offset_orders(df_, rjct_qty, set_price):
    n_of_orders = df_.shape[0]
    a = rjct_qty // n_of_orders
    b = rjct_qty % n_of_orders

    l_df_updated = []
    for idx, row in df_.iterrows():
        row["set_price"] = set_price
        row["status"] = "done"
        if idx < b:
            set_qty = int(row["ord_qty"]) - a * n_of_orders - 1
        else:
            set_qty = int(row["ord_qty"]) - a * n_of_orders

        row["ord_qty"] = set_qty
        row["set_qty"] = set_qty

        l_df_updated.append(row.to_frame().T)
    return pd.concat(l_df_updated)


def get_new_sell_order(df_buys):
    """
    Create sell order from confirmed buy orders

    Args. :
    df_buys (dataframe) : buy orders dataframe which have the same offset_ord_id

    Returns:
    new_sell_df : new sell order dataframe

    Related Method : get_new_sell_order_from_model
    """

    l_df_sell_order_of_model = []

    for idx, row in df_buys.iterrows():
        l_df_sell_order_of_model.append(get_new_sell_order_from_model(row))

    if l_df_sell_order_of_model.__len__() > 1:
        df_new_sells = pd.concat(l_df_sell_order_of_model)
    else:
        df_new_sells = l_df_sell_order_of_model[0]

    return df_new_sells


def get_new_sell_order_from_model(row):
    """
    Create Specific sell orders by model name

    Args :
    row (dataframe) : specific buy order - one row of dataframe

    Returns :
    df_new_orders : sell order dataframe
    """

    model_name = row["ord_id"].split("-")[0]
    set_qty = int(row["set_qty"])
    date_ref = row["ord_date"]

    if model_name == "alicia":
        first_sell_date = kah.get_bdate_n_days_later(5, date_ref)
        # second_sell_date = kah.get_bdate_n_days_later(5, date_ref)

        first_sell_qty = set_qty  # // 2
        # second_sell_qty = set_qty - first_sell_qty

        first_sell_orders = {
            "ord_date": first_sell_date,
            "created_date": date_ref,
            "ord_id": row["ord_id"],  # ord_id is created when buy order is created
            "stock_code": row["stock_code"],
            "stock_name": row["stock_name"],
            "ord_type": "sell",
            "ord_qty": str(first_sell_qty),
            "set_price": str(0),
            "set_qty": str(0),
            "offset_ord_id": "na",
            "status": "pending",
        }

        # second_sell_orders = {
        #     "ord_date": second_sell_date,
        #     "created_date": date_ref,
        #     "ord_id": row["ord_id"],  # ord_id is created when buy order is created
        #     "stock_code": row["stock_code"],
        #     "stock_name": row["stock_name"],
        #     "ord_type": "sell",
        #     "ord_qty": str(second_sell_qty),
        #     "set_price": str(0),
        #     "set_qty": str(0),
        #     "offset_ord_id": "na",
        #     "status": "pending",
        # }

        return pd.DataFrame(
            [first_sell_orders]
        )  # pd.DataFrame([first_sell_orders, second_sell_orders])

    elif model_name == "brie":
        print("BBB")
        pass
    elif model_name == "cate01" or model_name == "cate02":
        first_sell_date = kah.get_bdate_n_days_later(20, date_ref)

        first_sell_qty = set_qty

        first_sell_orders = {
            "ord_date": first_sell_date,
            "created_date": date_ref,
            "ord_id": row["ord_id"],  # ord_id is created when buy order is created
            "stock_code": row["stock_code"],
            "stock_name": row["stock_name"],
            "ord_type": "sell",
            "ord_qty": str(first_sell_qty),
            "set_price": str(0),
            "set_qty": str(0),
            "offset_ord_id": "na",
            "status": "pending",
        }

        return pd.DataFrame([first_sell_orders])
    elif model_name == "cate03":
        first_sell_date = kah.get_bdate_n_days_later(5, date_ref)

        first_sell_qty = set_qty

        first_sell_orders = {
            "ord_date": first_sell_date,
            "created_date": date_ref,
            "ord_id": row["ord_id"],  # ord_id is created when buy order is created
            "stock_code": row["stock_code"],
            "stock_name": row["stock_name"],
            "ord_type": "sell",
            "ord_qty": str(first_sell_qty),
            "set_price": str(0),
            "set_qty": str(0),
            "offset_ord_id": "na",
            "status": "pending",
        }

        return pd.DataFrame([first_sell_orders])

    elif model_name == "cate05":
        first_sell_date = kah.get_bdate_n_days_later(3, date_ref)

        first_sell_qty = set_qty

        first_sell_orders = {
            "ord_date": first_sell_date,
            "created_date": date_ref,
            "ord_id": row["ord_id"],  # ord_id is created when buy order is created
            "stock_code": row["stock_code"],
            "stock_name": row["stock_name"],
            "ord_type": "sell",
            "ord_qty": str(first_sell_qty),
            "set_price": str(0),
            "set_qty": str(0),
            "offset_ord_id": "na",
            "status": "pending",
        }

        return pd.DataFrame([first_sell_orders])

    elif model_name == "cate06":
        first_sell_date = kah.get_bdate_n_days_later(4, date_ref)

        first_sell_qty = set_qty

        first_sell_orders = {
            "ord_date": first_sell_date,
            "created_date": date_ref,
            "ord_id": row["ord_id"],  # ord_id is created when buy order is created
            "stock_code": row["stock_code"],
            "stock_name": row["stock_name"],
            "ord_type": "sell",
            "ord_qty": str(first_sell_qty),
            "set_price": str(0),
            "set_qty": str(0),
            "offset_ord_id": "na",
            "status": "pending",
        }

        return pd.DataFrame([first_sell_orders])

    elif model_name == "cate04":
        first_sell_date = kah.get_bdate_n_days_later(20, date_ref)

        first_sell_qty = set_qty

        first_sell_orders = {
            "ord_date": first_sell_date,
            "created_date": date_ref,
            "ord_id": row["ord_id"],  # ord_id is created when buy order is created
            "stock_code": row["stock_code"],
            "stock_name": row["stock_name"],
            "ord_type": "sell",
            "ord_qty": str(first_sell_qty),
            "set_price": str(0),
            "set_qty": str(0),
            "offset_ord_id": "na",
            "status": "pending",
        }

        return pd.DataFrame([first_sell_orders])


def get_todays_buy_orders(df, today):
    """
    Get today's buy order from model recommendations

    Args.
    df_recommends
    today (str) : date of today YYYY-mm-dd

    Returns.
    df_buy_orders
    """

    l_todays_buy_order = []
    for _idx, row in df.iterrows():
        ord_id = row["model_name"] + "-" + today.replace("-", "") + "-" + row["code"]
        l_todays_buy_order.append(
            {
                "ord_date": today,
                "created_date": today,
                "ord_id": ord_id,
                "stock_code": row["code"],
                "stock_name": row["name"],
                "ord_type": "buy",
                "ord_qty": str(row["buy_qty"]),
                "set_price": str(0),
                "set_qty": str(0),
                "offset_ord_id": str(0),
                "status": "pending",
            }
        )

    return pd.DataFrame(l_todays_buy_order)


def get_df_offset_orders(df_today, today):
    """
    Create Today's Offset Order Dataframe

    Args.
    df_today (dataframe) : today's sell / buy orders from Original Order sheet
    today (str) : date of today. YYYY-mm-dd

    Returns.
    df_today (dataframe) : offset_ord_id updated original order df
    df_offset_order (dataframe) : Offset Orders
    """

    df_today["ord_qty"] = df_today["ord_qty"].astype(int)
    df_today["signed_qty"] = (
        df_today["ord_type"].apply(lambda x: 1 if x == "buy" else -1)
        * df_today["ord_qty"]
    )

    sr_sigend_qty = df_today.groupby(["stock_code", "stock_name"])["signed_qty"].sum()

    l_offset_order_dict = []
    for (code, name), qty in sr_sigend_qty.items():
        if qty == 0:
            l_offset_order_dict.append(
                {
                    "ord_date": today,
                    "offset_ord_id": "off-" + today.replace("-", "") + "-" + code,
                    "ebest_ord_no": "-",
                    "stock_code": code,
                    "stock_name": name,
                    "ord_type": "0",
                    "ord_qty": str(qty),
                    "rjct_qty": "0",
                    "set_price": "0",
                    "status": "done",
                }
            )
        else:
            ord_type = "buy" if qty > 0 else "sell"
            l_offset_order_dict.append(
                {
                    "ord_date": today,
                    "offset_ord_id": "off-" + today.replace("-", "") + "-" + code,
                    "ebest_ord_no": "-",
                    "stock_code": code,
                    "stock_name": name,
                    "ord_type": ord_type,
                    "ord_qty": str(abs(qty)),
                    "rjct_qty": "0",
                    "set_price": "0",
                    "status": "pending",
                }
            )

    df_offset_order = pd.DataFrame(l_offset_order_dict)

    code_offset_ord_id_dict = df_offset_order.set_index("stock_code")[
        "offset_ord_id"
    ].to_dict()

    df_today["offset_ord_id"] = df_today["stock_code"].map(code_offset_ord_id_dict)

    df_today.drop(columns="signed_qty", inplace=True)

    return df_today, df_offset_order


# Stock Info
def get_krx_stock_list():
    """
    Get Stock KRX Stock List at the present
    : Only normal stocks
    """
    try:
        df_krx = (
            fdr.StockListing("KRX")[
                lambda df: df.Market.isin(["KOSPI", "KOSDAQ GLOBAL", "KOSDAQ"])
            ][lambda df: df.Code.str[5] == "0"]
            .sort_values(by="Marcap", ascending=False)
            .rename(columns=lambda x: x.lower())
        )

    except Exception as err:
        print(f"{err} : plan B Started")

        url = "https://kind.krx.co.kr/corpgeneral/corpList.do"

        kospi_code = pd.read_html(url + "?method=download&marketType=stockMkt")[0]
        kosdaq_code = pd.read_html(url + "?method=download&marketType=kosdaqMkt")[0]

        kospi_code = kospi_code[["회사명", "종목코드"]]
        kosdaq_code = kosdaq_code[["회사명", "종목코드"]]

        df_krx = pd.concat([kospi_code, kosdaq_code])
        df_krx.rename(columns={"종목코드": "code"}, inplace=True)

    return df_krx


def get_kospi_kosdaq_close(n_days, today):
    """
    Get close mean values of KOSPI and KOSDAQ

    Args.
    n_days (int) : Number of how many days ago
    today (str) : Reference date (YYYY-mm-dd)

    Returns.
    df_krx (dataframe) : columns = ['date', 'krx']
      - 'date' is string, YYYY-mm-dd
      - 'krx' is mean of closes of KOSPI and KOSDAQ

    """

    date_n_days_ago = kah.get_bdate_n_days_before(n_days, today)

    df_kospi = (
        fdr.DataReader("KS11", date_n_days_ago, today)
        .reset_index()
        .rename(columns={"Close": "kospi"})[["Date", "kospi"]]
    )

    df_kosdaq = (
        fdr.DataReader("KQ11", date_n_days_ago, today)
        .reset_index()
        .rename(columns={"Close": "kosdaq"})[["Date", "kosdaq"]]
    )

    df_krx = (
        df_kospi.merge(df_kosdaq, on="Date")
        # .set_index('Date')
        # .mean(axis=1)
        # .reset_index()
        # .rename(columns={"Date" : "date", 0 : "krx"})
        .rename(columns={"Date": "date"})
    )

    df_krx["date"] = df_krx["date"].dt.strftime("%Y-%m-%d")

    print("df_krx")
    print(df_krx.tail())

    return df_krx
