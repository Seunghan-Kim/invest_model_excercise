#%%

from handler_KIS_api import KisApiHandler
import FinanceDataReader as fdr
import pandas as pd
import math
# from make_new_features import get_df_with_features

pd.options.plotting.backend = "plotly"

#%%
kah = KisApiHandler()
#%%
TODAY = "2024-01-03"

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
        fdr.DataReader("KS11", date_n_days_ago)
        .reset_index()
        .rename(columns={"Close": "kospi"})[["Date", "kospi"]]
    )

    df_kosdaq = (
        fdr.DataReader("KQ11", date_n_days_ago)
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

    max_date = '2024-02-30'
    start_date = '2015-01-02'

    finish = False
    dates = []

    len_of_train = 300
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
# 1. Get KRX Stock List
df_krx_stock_list = get_krx_stock_list()
codes = df_krx_stock_list.code.unique().tolist()

# %%
#%%
df_bdates = kah.get_df_bdate_n_days_before(90,TODAY)
dates_list = df_bdates["bass_dt"].tolist()

# %%
# 2. Get Price DataFrame
df_price_n_days = kah.get_df_price_n_days_over_100days(codes, dates_list)

#%%
# df_price_n_days.to_pickle(f"df_price_n_days_{TODAY}.pkl")
df_price_n_days = pd.read_pickle(f"df_price_n_days_{TODAY}.pkl")

# %%
# 3. Get KRX Index mean of close
df_krx = get_kospi_kosdaq_close(100, TODAY)

# %%
# 4. Merge df_price and df_krx
df_price_n_days = df_price_n_days.merge(
    df_krx,
    on='date'
)
# %%
SHORT_PERIOD=5
MID_PERIOD=20
LONG_PERIOD=60

date_min = df_price_n_days.date.min()
date_max = df_price_n_days.date.max()

from make_new_features_with_corr import get_df_with_features
df_with_feats = get_df_with_features(
        df_price_n_days, SHORT_PERIOD=SHORT_PERIOD, MID_PERIOD=MID_PERIOD, LONG_PERIOD=LONG_PERIOD)

# %%
feats = [            
           'vol_x_price_sma_short',
    'vol_x_price_sma_mid_to_short',
    'change_high',
    'change_low',
    'change_open',
    'close_std_short', 
    'close_std_mid',
    'close_std_long',
    'open_std_short',
    'open_std_mid',
    'high_std_short',
    'high_std_long',
    'close_change_p_long', 
    'krx_corr_mid',
    'krx_corr_long',
    'krx_change_std_short',
    'krx_change_std_mid',
    'w_price_vol_corr_long',
    'w_price_vol_corr_mid'
]

#%%       
import numpy as np
df_ml = (
    df_with_feats
    .drop(columns=['rtn_3', 'rtn_4', 'rtn_5'])
    .assign(
        vol_x_price_sma_long_to_mid = lambda df : df.vol_x_price_sma_long / df.vol_x_price_sma_mid,
        vol_x_price_sma_mid_to_short = lambda df : df.vol_x_price_sma_mid / df.vol_x_price_sma_short,
    )
    .loc[lambda df : df["vol_x_price_sma_mid"] >  0.75e+08] #2.481560e+08
    .loc[lambda df : df["vol_x_price_sma_mid"] <  3.5e+08] #2.481560e+08
    .loc[lambda df : df["vol_zero_count_mid"] == 0]
    .loc[lambda df : df["change"] < 0.29]
    .loc[lambda df : df["volume"] > 0]  

    .replace([-np.inf, np.inf], np.nan)
    .dropna(subset=feats)
    .reset_index(drop=True)
)


#%%
import xgboost as xgb
import pickle

df_ml_feats = df_ml[feats]
d_feats = xgb.DMatrix(df_ml_feats)

model = xgb.Booster()

with open("model_list_pickle_2023-11-28_rtn_4.pkl", "rb") as f:
    model_list = pickle.load(f)

cols = []
for id, model in enumerate(model_list):
    col = f"pred_{id}"
    cols.append(col)
    df_ml[col] = model.predict(d_feats)
    # print(model.predict(d_feats))

df_ml["proba_mean"] = df_ml[cols].mean(axis=1)
df_ml["proba_max"] = df_ml[cols].max(axis=1)

print("Cate03 Proba distributions")
print(df_ml.loc[lambda df : df.date == TODAY]["proba_mean"].describe())
print(df_ml.loc[lambda df : df.date == TODAY]["proba_max"].describe())

#%%
        
df_after_ml_filter = (
    df_ml
    .assign(
        model_name = 'cate03'
    )
    .drop(columns=['vol_x_price_sma_long_to_mid', 'vol_x_price_sma_mid_to_short'])
    .loc[lambda df : df["proba_mean"] > 0.5] # rtn_5, mean-max
    # .loc[lambda df : df.date == TODAY]
    .sort_values('proba_mean', ascending=False) # rtn_5, mean-max 
    .groupby('date')
    # .sort_values('proba_mean', ascending=False)  
    .head(5)       
    .reset_index()     
)

#%%
df_ml.date.unique().__len__()
#%%
df_ml.loc[lambda df : df.proba_max > 0.5].date.unique().__len__()
# %%
df_group = df_ml.loc[lambda df : df.proba_max > 0.5].groupby('date')
# %%
for name, group in df_after_ml_filter.groupby('date'):
    print(name)
    print(group[['date', 'code']])
    print('\n')
# %%
