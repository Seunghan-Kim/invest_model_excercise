#%%
from handler_KIS_api import KisApiHandler
import FinanceDataReader as fdr
import pandas as pd
import math

#%%
kah = KisApiHandler()

#--------------------------------------------------------------
#%% 1. Functions
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

    return df_krx

# For fixed length train dataset
def get_dates():
    import exchange_calendars as xcals
    krx_cal = xcals.get_calendar("XKRX")

    max_date = '2024-05-30'
    start_date = '2015-01-02'

    finish = False
    dates = []

    len_of_train = 200
    gap_from_last_train_date = 7 # This number depends on the target ( rtn_5 -> 6, rnt_20 -> 21)
    len_of_pred = 20 # The length of pred for 1 model update

    while not finish :
        train_end = krx_cal.sessions_window(start_date, len_of_train)[-1].strftime("%Y-%m-%d")
        oos_start = krx_cal.sessions_window(train_end, gap_from_last_train_date)[-1].strftime("%Y-%m-%d")
        oos_end = krx_cal.sessions_window(oos_start, len_of_pred)[-1].strftime("%Y-%m-%d")

        if oos_end > max_date :
            finish = True
            
        else :
            dates.append(
                (start_date, train_end, oos_start, oos_end)
            )

            start_date = krx_cal.sessions_window(start_date, len_of_pred)[-1].strftime("%Y-%m-%d")
    return dates

#%%
# 2. Get Dates List
dates = get_dates()
dates[-6:]

#%%
# 3. Set Ref Dates Tuple
date_set = ('2023-06-01', '2024-03-25', '2024-04-02', '2024-04-29')

#%%
# 4. Set TODAY & period
TODAY = "2024-04-02"
period = 320

#%%
# 5. Get KRX Stock List
df_krx_stock_list = get_krx_stock_list()
codes = df_krx_stock_list.code.unique().tolist()
code_name_dict = df_krx_stock_list.set_index('code')['name'].to_dict()

#%%
# 6. Get Business days from KIS
df_bdates = kah.get_df_bdate_n_days_before(period, TODAY)
dates_list = df_bdates["bass_dt"].tolist()

#%%
# 7. Check Business day list value
print(dates_list[0])
print(dates_list[-1])

# %%
# 8. Get Price DataFrame
df_price_n_days = kah.get_df_price_n_days_over_100days(codes, dates_list)
print(df_price_n_days.head())
print(df_price_n_days.tail())

#%%
# 9. Save df_price_n_days
df_price_n_days.to_pickle(f"df_price_{dates_list[-1]}_{dates_list[0]}.pkl")
# df_price_n_days = pd.read_pickle(f"df_price_{dates_list[-1]}_{dates_list[0]}.pkl")

#%%
# 10. Get KRX Index value (mean of close)
df_krx = get_kospi_kosdaq_close(period, TODAY)
print(df_krx.head(2))
print(df_krx.tail(2))

#%%
# 11. Merge df_price and df_krx
df_price_n_days = df_price_n_days.merge(
    df_krx,
    on='date'
)
df_price_n_days['name'] = df_price_n_days['code'].map(code_name_dict)

#%%
# 12. Check Merged df_price

print(df_price_n_days.head(2))
print(df_price_n_days.tail(2))

#%%
# 13. Save df_price_n_days For ML Train
df_price_n_days.to_pickle(f"df_price_for_train_{date_set[0]}-{date_set[1]}.pkl")
print(f"df_price_for_train_{date_set[0]}-{date_set[1]}.pkl")


# %%
