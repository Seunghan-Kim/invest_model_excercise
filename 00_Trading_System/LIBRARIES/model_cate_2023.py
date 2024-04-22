import pandas as pd
import numpy as np
import xgboost as xgb
import telebot

IS_RUN_ON_DATABUTTON = False

if IS_RUN_ON_DATABUTTON :
    import databutton as db
else :
    import os
    from dotenv import load_dotenv

try:
    if IS_RUN_ON_DATABUTTON:
        TELEGRAM_BOT_TOKEN = db.secrets.get("telegram_bot_01_token")
    else:        
        load_dotenv()                
        TELEGRAM_BOT_TOKEN = os.getenv("telegram_bot_01_token")
    tb = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
    
except Exception as err:
    print(f"telegram bot err : {err}")

class Cate2023:

    def __init__(self, today, df_price) -> None:

        self.today = today

        self.money_per_day = 2_000_000
        self.money_per_day_ml = 2_000_000
        self.sell_days_after_01 = 20

        self.LONG_PERIOD = 60
        self.MID_PERIOD = 20
        self.SHORT_PERIOD = 5

        self.vol_x_price_upper = 339067168.15

        self.df_feats = self.get_df_feats_of_model_cate(df_price)
        print(self.df_feats.tail())        

    def get_model_cate_recommendation(self):

        # df_filtered_01 = self.filter_cate_01()
        # df_filtered_02 = self.filter_cate_02()
        ## ML
        df_filtered_03 = self.filter_cate_03()
        df_filtered_05 = self.filter_cate_05()
        df_filtered_06 = self.filter_cate_06()
        ## KOSDAQ
        df_filtered_04 = self.filter_cate_04()
        
        # print(df_filtered_01.tail())
        # print(df_filtered_02.tail())
        # print(df_filtered_03.tail())
        # print(df_filtered_04.tail())
        l_recommend_df = []

        # if df_filtered_01.shape[0] > 0:

        #     money_per_stock = self.money_per_day / df_filtered_01.shape[0]
        #     df_filtered_01['buy_qty'] = df_filtered_01['close'].apply(
        #         lambda x : int(money_per_stock / x)
        #     )
        #     l_recommend_df.append(df_filtered_01)

        # if df_filtered_02.shape[0] > 0:

        #     money_per_stock = self.money_per_day / df_filtered_02.shape[0]
        #     df_filtered_02['buy_qty'] = df_filtered_02['close'].apply(
        #         lambda x : int(money_per_stock / x)
        #     )
        #     l_recommend_df.append(df_filtered_02)

        if df_filtered_03.shape[0] > 0:

            money_per_stock = self.money_per_day_ml / df_filtered_03.shape[0]
            df_filtered_03['buy_qty'] = df_filtered_03['close'].apply(
                lambda x : int(money_per_stock / x)
            )
            l_recommend_df.append(df_filtered_03)

        if df_filtered_05.shape[0] > 0:

            money_per_stock = self.money_per_day_ml / df_filtered_05.shape[0]
            df_filtered_05['buy_qty'] = df_filtered_05['close'].apply(
                lambda x : int(money_per_stock / x)
            )
            l_recommend_df.append(df_filtered_05)

        if df_filtered_06.shape[0] > 0:

            money_per_stock = self.money_per_day_ml / df_filtered_06.shape[0]
            df_filtered_06['buy_qty'] = df_filtered_06['close'].apply(
                lambda x : int(money_per_stock / x)
            )
            l_recommend_df.append(df_filtered_06)

        if df_filtered_04.shape[0] > 0:

            money_per_stock = self.money_per_day / df_filtered_04.shape[0]
            df_filtered_04['buy_qty'] = df_filtered_04['close'].apply(
                lambda x : int(money_per_stock / x)
            )
            l_recommend_df.append(df_filtered_04)

        if l_recommend_df.__len__() > 0:
            df_recomnd = pd.concat(l_recommend_df)
            print("Cate Recommendation")
            try:
                print(df_recomnd[['date', 'name', 'model_name']])
            except Exception as err:
                print(err)
            return df_recomnd
        else :
            return pd.DataFrame()

    def filter_cate_01(self):
        """
        Get Recommendation from Cate Filter#1
        Optuna Tunning Objective : Max Cumsum, Min std

        Args. None

        Returns.
        Recomendation Dataframe
        """
        df_with_feats = self.df_feats

        params_max_cumsum_min_std = {
            'close_change_p_mid_upper' : 0.35,
            'close_std_long_fac' : 0.75,
            'vol_x_price_sma_long_fac' : 0.9,
            'vol_x_price_sma_mid_fac' : 0.65,
            'vol_x_price_sma_mid_lower' : 1e+08,
            'vol_x_price_sma_mid_upper' : 5.9e+08,
            'sort_ref' : 'close_change_p_mid',
            'sort_order' : True,
            'top_n' : 5,
            'max_holding_date' : 20
        }

        df_filtered = (
            df_with_feats
            .loc[lambda df : 
                 df["vol_x_price_sma_long"]*params_max_cumsum_min_std['vol_x_price_sma_long_fac'] 
                 > df["vol_x_price_sma_mid"]
                 ]
            .loc[lambda df : 
                 df["vol_x_price_sma_mid"]*params_max_cumsum_min_std['vol_x_price_sma_mid_fac']  
                 > df["vol_x_price_sma_short"]
                 ]

            .loc[lambda df : 
                 df["close_std_short"] 
                 > df["close_std_long"]*params_max_cumsum_min_std['close_std_long_fac']
                 ]

            .loc[lambda df : 
                 df["vol_x_price_sma_mid"] > params_max_cumsum_min_std['vol_x_price_sma_mid_lower']
                 ] 
            .loc[lambda df : 
                 df["vol_x_price_sma_mid"] < params_max_cumsum_min_std['vol_x_price_sma_mid_upper']
                 ] 
            
            .loc[lambda df : df["vol_zero_count_long"] < 1]
            .loc[lambda df : df["change"] < 0.29]
            .loc[lambda df : df["volume"] > 0]

            .loc[lambda df : 
                 df["close_change_p_mid"] < params_max_cumsum_min_std['close_change_p_mid_upper']
                 ]
        )

        return (
            df_filtered
            .loc[lambda df : df.date == self.today]
            .sort_values('close_change_p_mid')  
            .head(5)
            .assign(
                model_name = 'cate01'
            )            
        )
    
    def filter_cate_02(self):
        """
        Get Recommendation from Cate Filter#1
        Optuna Tunning Objective : Max win rate

        Args. None

        Returns.
        Recomendation Dataframe
        """
        df_with_feats = self.df_feats

        params_max_win_rate = {
            'close_change_p_mid_upper' : -0.25,
            'close_std_long_fac' : 0.60,
            'vol_x_price_sma_long_fac' : 1.00,
            'vol_x_price_sma_mid_fac' : 0.70,
            'vol_x_price_sma_mid_lower' : 1.5e+08,
            'vol_x_price_sma_mid_upper' : 26.5e+08,
            'sort_ref' : 'close_change_p_mid',
            'sort_order' : True,
            'top_n' : 5,
            'max_holding_date' : 20
        }

        df_filtered = (
            df_with_feats
            .loc[lambda df : 
                 df["vol_x_price_sma_long"]*params_max_win_rate['vol_x_price_sma_long_fac'] 
                 > df["vol_x_price_sma_mid"]
                 ]
            .loc[lambda df : 
                 df["vol_x_price_sma_mid"]*params_max_win_rate['vol_x_price_sma_mid_fac']  
                 > df["vol_x_price_sma_short"]
                 ]

            .loc[lambda df : 
                 df["close_std_short"] 
                 > df["close_std_long"]*params_max_win_rate['close_std_long_fac']
                 ]

            .loc[lambda df : 
                 df["vol_x_price_sma_mid"] > params_max_win_rate['vol_x_price_sma_mid_lower']
                 ] 
            .loc[lambda df : 
                 df["vol_x_price_sma_mid"] < params_max_win_rate['vol_x_price_sma_mid_upper']
                 ] 
            
            .loc[lambda df : df["vol_zero_count_long"] < 1]
            .loc[lambda df : df["change"] < 0.29]
            .loc[lambda df : df["volume"] > 0]

            .loc[lambda df : 
                 df["close_change_p_mid"] < params_max_win_rate['close_change_p_mid_upper']
                 ]
        )

        return (
            df_filtered
            .loc[lambda df : df.date == self.today]
            .sort_values('close_change_p_mid')  
            .head(5)            
            .assign(
                model_name = 'cate02'
            )
        )
    
    def filter_cate_03(self):
        """
        Apply ML model
        """

        feats = [            
            'vol_x_price_sma_long',
            'vol_x_price_sma_mid',
            'vol_x_price_sma_short',
            # 'vol_x_price_sma_long_to_mid',
            # 'vol_x_price_sma_mid_to_short',
            # 'change_high',
            # 'change_low',
            # 'change_open',
            'close_std_short',
            'close_std_mid',
            'close_std_long',
            # 'open_std_short',
            # 'open_std_mid',
            # 'open_std_long',
            'high_std_short',
            'high_std_mid',
            'high_std_long',
            'low_std_short',
            'low_std_mid',
            'low_std_long',
            # 'weighted_price_change_std_short',
            # 'weighted_price_change_std_mid',
            # 'weighted_price_change_std_long',
            # 'vol_zero_count_short',
            # 'vol_zero_count_mid',
            # 'vol_zero_count_long',
            'close_change_p_short',
            'close_change_p_mid',
            'close_change_p_long',
            # 'weighted_price_change_p_short',
            # 'weighted_price_change_p_mid',
            # 'weighted_price_change_p_long',
            'krx_corr_short',
            'krx_corr_mid',
            'krx_corr_long',
            # 'krx_change_p_short',
            # 'krx_change_p_mid',
            # 'krx_change_p_long',
            'krx_change_std_short',
            'krx_change_std_mid',
            'krx_change_std_long',
        ]

        print(f"Cate03 df_feats latest date : {self.df_feats.date.max()}")
        
        df_ml = (
            self.df_feats
            # .assign(
            #     vol_x_price_sma_long_to_mid = lambda df : df.vol_x_price_sma_long / df.vol_x_price_sma_mid,
            #     vol_x_price_sma_mid_to_short = lambda df : df.vol_x_price_sma_mid / df.vol_x_price_sma_short,
            # )
            .loc[lambda df : df["vol_x_price_sma_mid"] >  0.75e+08] #2.481560e+08
            .loc[lambda df : df["vol_x_price_sma_mid"] <  self.vol_x_price_upper] #3.5e+08
            .loc[lambda df : df["vol_zero_count_mid"] < 1]
            .loc[lambda df : df["change"] < 0.29]
            .loc[lambda df : df["volume"] > 0]  
            .loc[lambda df : df["close"] > 1000]

            .replace([-np.inf, np.inf], np.nan)
            .dropna(subset=feats)
            .reset_index(drop=True)
        )

        print(f"Cate03 df_ml latest date : {df_ml.date.max()}")

        df_ml_feats = df_ml[feats]
        d_feats = xgb.DMatrix(df_ml_feats)

        model = xgb.Booster()

        if IS_RUN_ON_DATABUTTON :
            model_list_pkl = db.storage.binary.get(key="model-list-pickle-2023-09-20-rtn-5-pkl")
            # data = db.storage.binary.get(key="model-list-pickle-2023-09-20-rtn-3-pkl")
            
            import pickle
            
            with open("model_list", "wb") as f:
                f.write(model_list_pkl)
        
            with open("model_list", "rb") as f:
                model_list = pickle.load(f)
            
        else :
            model.load_model("./STORAGE/cate_xgb_model_230930.bin")

        # y_pred = model.predict(d_feats)
        cols = []
        for id, model in enumerate(model_list):
            col = f"pred_{id}"
            cols.append(col)
            df_ml[col] = model.predict(d_feats)
            # print(model.predict(d_feats))

        df_ml["proba_mean"] = df_ml[cols].mean(axis=1)

        print("Cate03 Proba distributions")
        print(df_ml.loc[lambda df : df.date == self.today]["proba_mean"].describe())
        
        return  (
            df_ml
            .assign(
                model_name = 'cate03'
            )
            # .drop(columns=['vol_x_price_sma_long_to_mid', 'vol_x_price_sma_mid_to_short'])
            .loc[lambda df : df["proba_mean"] > 0.5]
            .loc[lambda df : df.date == self.today]
            .sort_values('close_change_p_mid', ascending=True)  
            # .sort_values('proba_mean', ascending=False)  
            .head(5)            
        )

    def filter_cate_04(self):

        df_filtered = self.df_feats

        value_today = (
            df_filtered.loc[lambda df : df.date == self.today]['krx_change_p_mid'].values[0]
        )        

        if value_today < -0.08 or value_today > 0.12 : 
            
            return(
                df_filtered
                .loc[lambda df : df.date == self.today]
                .loc[lambda df : df.code == '229200']
                .assign(
                    model_name = 'cate04'
                )
            )
        else :
            return pd.DataFrame()

    def filter_cate_05(self):
        """
        Apply ML model
        """

        feats = [            
            'vol_x_price_sma_long',
            'vol_x_price_sma_mid',
            'vol_x_price_sma_short',
            # 'vol_x_price_sma_long_to_mid',
            # 'vol_x_price_sma_mid_to_short',
            # 'change_high',
            # 'change_low',
            # 'change_open',
            'close_std_short',
            'close_std_mid',
            'close_std_long',
            # 'open_std_short',
            # 'open_std_mid',
            # 'open_std_long',
            'high_std_short',
            'high_std_mid',
            'high_std_long',
            'low_std_short',
            'low_std_mid',
            'low_std_long',
            # 'weighted_price_change_std_short',
            # 'weighted_price_change_std_mid',
            # 'weighted_price_change_std_long',
            # 'vol_zero_count_short',
            # 'vol_zero_count_mid',
            # 'vol_zero_count_long',
            'close_change_p_short',
            'close_change_p_mid',
            'close_change_p_long',
            # 'weighted_price_change_p_short',
            # 'weighted_price_change_p_mid',
            # 'weighted_price_change_p_long',
            'krx_corr_short',
            'krx_corr_mid',
            'krx_corr_long',
            # 'krx_change_p_short',
            # 'krx_change_p_mid',
            # 'krx_change_p_long',
            'krx_change_std_short',
            'krx_change_std_mid',
            'krx_change_std_long',
        ]

        print(f"Cate05 df_feats latest date : {self.df_feats.date.max()}")
        
        df_ml = (
            self.df_feats
            # .assign(
            #     vol_x_price_sma_long_to_mid = lambda df : df.vol_x_price_sma_long / df.vol_x_price_sma_mid,
            #     vol_x_price_sma_mid_to_short = lambda df : df.vol_x_price_sma_mid / df.vol_x_price_sma_short,
            # )
            .loc[lambda df : df["vol_x_price_sma_mid"] >  0.75e+08] #2.481560e+08
            .loc[lambda df : df["vol_x_price_sma_mid"] <  self.vol_x_price_upper] #2.481560e+08
            .loc[lambda df : df["vol_zero_count_mid"] < 1]
            .loc[lambda df : df["change"] < 0.29]
            .loc[lambda df : df["volume"] > 0]  
            .loc[lambda df : df["close"] > 1000]

            .replace([-np.inf, np.inf], np.nan)
            .dropna(subset=feats)
            .reset_index(drop=True)
        )

        print(f"Cate05 df_ml latest date : {df_ml.date.max()}")

        df_ml_feats = df_ml[feats]
        d_feats = xgb.DMatrix(df_ml_feats)

        model = xgb.Booster()

        if IS_RUN_ON_DATABUTTON :
            model_list_pkl = db.storage.binary.get(key="model-list-pickle-2023-09-20-rtn-3-pkl")
            
            import pickle
            
            with open("model_list", "wb") as f:
                f.write(model_list_pkl)
        
            with open("model_list", "rb") as f:
                model_list = pickle.load(f)
            
        else :
            model.load_model("./STORAGE/cate_xgb_model_230930.bin")

        # y_pred = model.predict(d_feats)
        cols = []
        for id, model in enumerate(model_list):
            col = f"pred_{id}"
            cols.append(col)
            df_ml[col] = model.predict(d_feats)
            # print(model.predict(d_feats))

        df_ml["proba_mean"] = df_ml[cols].mean(axis=1)

        print("Cate03 Proba distributions")
        print(df_ml.loc[lambda df : df.date == self.today]["proba_mean"].describe())
        
        return  (
            df_ml
            .assign(
                model_name = 'cate05'
            )
            # .drop(columns=['vol_x_price_sma_long_to_mid', 'vol_x_price_sma_mid_to_short'])
            .loc[lambda df : df["proba_mean"] > 0.5]
            .loc[lambda df : df.date == self.today]
            .sort_values('close_change_p_mid', ascending=True)  
            # .sort_values('proba_mean', ascending=False)  
            .head(5)            
        )

    def filter_cate_06(self):
        """
        Apply ML model
        """

        feats = [            
            'vol_x_price_sma_long',
            'vol_x_price_sma_mid',
            'vol_x_price_sma_short',
            # 'vol_x_price_sma_long_to_mid',
            # 'vol_x_price_sma_mid_to_short',
            # 'change_high',
            # 'change_low',
            # 'change_open',
            'close_std_short',
            'close_std_mid',
            'close_std_long',
            # 'open_std_short',
            # 'open_std_mid',
            # 'open_std_long',
            'high_std_short',
            'high_std_mid',
            'high_std_long',
            'low_std_short',
            'low_std_mid',
            'low_std_long',
            # 'weighted_price_change_std_short',
            # 'weighted_price_change_std_mid',
            # 'weighted_price_change_std_long',
            # 'vol_zero_count_short',
            # 'vol_zero_count_mid',
            # 'vol_zero_count_long',
            'close_change_p_short',
            'close_change_p_mid',
            'close_change_p_long',
            # 'weighted_price_change_p_short',
            # 'weighted_price_change_p_mid',
            # 'weighted_price_change_p_long',
            'krx_corr_short',
            'krx_corr_mid',
            'krx_corr_long',
            # 'krx_change_p_short',
            # 'krx_change_p_mid',
            # 'krx_change_p_long',
            'krx_change_std_short',
            'krx_change_std_mid',
            'krx_change_std_long',
        ]

        print(f"Cate06 df_feats latest date : {self.df_feats.date.max()}")
        
        df_ml = (
            self.df_feats
            # .assign(
            #     vol_x_price_sma_long_to_mid = lambda df : df.vol_x_price_sma_long / df.vol_x_price_sma_mid,
            #     vol_x_price_sma_mid_to_short = lambda df : df.vol_x_price_sma_mid / df.vol_x_price_sma_short,
            # )
            .loc[lambda df : df["vol_x_price_sma_mid"] >  0.75e+08] #2.481560e+08
            .loc[lambda df : df["vol_x_price_sma_mid"] <  self.vol_x_price_upper] #2.481560e+08
            .loc[lambda df : df["vol_zero_count_mid"] < 1]
            .loc[lambda df : df["change"] < 0.29]
            .loc[lambda df : df["volume"] > 0]  
            .loc[lambda df : df["close"] > 1000]

            .replace([-np.inf, np.inf], np.nan)
            .dropna(subset=feats)
            .reset_index(drop=True)
        )

        print(f"Cate06 df_ml latest date : {df_ml.date.max()}")

        df_ml_feats = df_ml[feats]
        d_feats = xgb.DMatrix(df_ml_feats)

        model = xgb.Booster()

        if IS_RUN_ON_DATABUTTON :
            model_list_pkl = db.storage.binary.get(key="model-list-pickle-2023-09-20-rtn-4-pkl")
            
            import pickle
            
            with open("model_list", "wb") as f:
                f.write(model_list_pkl)
        
            with open("model_list", "rb") as f:
                model_list = pickle.load(f)
            
        else :
            model.load_model("./STORAGE/cate_xgb_model_230930.bin")

        # y_pred = model.predict(d_feats)
        cols = []
        for id, model in enumerate(model_list):
            col = f"pred_{id}"
            cols.append(col)
            df_ml[col] = model.predict(d_feats)
            # print(model.predict(d_feats))

        df_ml["proba_mean"] = df_ml[cols].mean(axis=1)

        print("Cate03 Proba distributions")
        print(df_ml.loc[lambda df : df.date == self.today]["proba_mean"].describe())
        
        return  (
            df_ml
            .assign(
                model_name = 'cate06'
            )
            # .drop(columns=['vol_x_price_sma_long_to_mid', 'vol_x_price_sma_mid_to_short'])
            .loc[lambda df : df["proba_mean"] > 0.5]
            .loc[lambda df : df.date == self.today]
            .sort_values('close_change_p_mid', ascending=True)  
            # .sort_values('proba_mean', ascending=False)  
            .head(5)            
        )

    def get_df_feats_of_model_cate(self,df_price):
        """
        Create and add features for model Cate to the original price dataset

        Args.
        df_price (dataframe) : price dataset with KRX index

        Returns.
        DataFrame with features
        """

        def get_rolling_sma(cols, rolling_window):
            return (
                df_price.set_index("date")
                .groupby('code')[cols]
                .rolling(rolling_window)
                .mean()
                .unstack()
                .T
            )
        
        close_pivot = get_rolling_sma('close', 1)
        open_pivot = get_rolling_sma('open', 1)
        high_pivot = get_rolling_sma('high', 1)
        low_pivot = get_rolling_sma('low', 1)    

        kosdaq_pivot = get_rolling_sma('kosdaq', 1)
        kospi_pivot = get_rolling_sma('kospi', 1)

        weighted_price_pivot = (0.25*close_pivot + 0.25*open_pivot + 0.25*high_pivot + 0.25*low_pivot)

        vol_pivot = get_rolling_sma('volume', 1)
        vol_x_price_pivot = (vol_pivot * weighted_price_pivot)

        vol_x_price_sma_long = vol_x_price_pivot.rolling(self.LONG_PERIOD).mean()
        vol_x_price_sma_mid = vol_x_price_pivot.rolling(self.MID_PERIOD).mean()
        vol_x_price_sma_short = vol_x_price_pivot.rolling(self.SHORT_PERIOD).mean()
        
        change_close_pivot = get_rolling_sma('change', 1)
        change_high_pivot = high_pivot / close_pivot.shift(1) - 1
        change_low_pivot = low_pivot / close_pivot.shift(1) - 1
        change_open_pivot = open_pivot / close_pivot.shift(1) - 1

        close_std_short_pivot = change_close_pivot.rolling(self.SHORT_PERIOD).std()
        close_std_mid_pivot = change_close_pivot.rolling(self.MID_PERIOD).std()
        close_std_long_pivot = change_close_pivot.rolling(self.LONG_PERIOD).std()

        open_std_short_pivot = change_open_pivot.rolling(self.SHORT_PERIOD).std()
        open_std_mid_pivot = change_open_pivot.rolling(self.MID_PERIOD).std()
        open_std_long_pivot = change_open_pivot.rolling(self.LONG_PERIOD).std()

        high_std_short_pivot = change_high_pivot.rolling(self.SHORT_PERIOD).std()
        high_std_mid_pivot = change_high_pivot.rolling(self.MID_PERIOD).std()
        high_std_long_pivot = change_high_pivot.rolling(self.LONG_PERIOD).std()

        low_std_short_pivot = change_low_pivot.rolling(self.SHORT_PERIOD).std()
        low_std_mid_pivot = change_low_pivot.rolling(self.MID_PERIOD).std()
        low_std_long_pivot = change_low_pivot.rolling(self.LONG_PERIOD).std()

        close_change_p_short = close_pivot.pct_change(self.SHORT_PERIOD)
        close_change_p_mid = close_pivot.pct_change(self.MID_PERIOD)
        close_change_p_long = close_pivot.pct_change(self.LONG_PERIOD)

        weighted_price_change_p_short = weighted_price_pivot.pct_change(self.SHORT_PERIOD)
        weighted_price_change_p_mid = weighted_price_pivot.pct_change(self.MID_PERIOD)
        weighted_price_change_p_long = weighted_price_pivot.pct_change(self.LONG_PERIOD)

        kospi_corr_short = close_pivot.rolling(self.SHORT_PERIOD).corr(kospi_pivot)
        kospi_corr_mid = close_pivot.rolling(self.MID_PERIOD).corr(kospi_pivot)
        kospi_corr_long = close_pivot.rolling(self.LONG_PERIOD).corr(kospi_pivot)

        kosdaq_corr_short = close_pivot.rolling(self.SHORT_PERIOD).corr(kosdaq_pivot)
        kosdaq_corr_mid = close_pivot.rolling(self.MID_PERIOD).corr(kosdaq_pivot)
        kosdaq_corr_long = close_pivot.rolling(self.LONG_PERIOD).corr(kosdaq_pivot)

        krx_corr_short = (kospi_corr_short + kosdaq_corr_short)/2
        krx_corr_mid = (kospi_corr_mid + kosdaq_corr_mid)/2
        krx_corr_long = (kospi_corr_long + kosdaq_corr_long)/2

        kospi_change_p_short = kospi_pivot.pct_change(self.SHORT_PERIOD)
        kospi_change_p_mid = kospi_pivot.pct_change(self.MID_PERIOD)
        kospi_change_p_long = kospi_pivot.pct_change(self.LONG_PERIOD)

        kosdaq_change_p_short = kosdaq_pivot.pct_change(self.SHORT_PERIOD)
        kosdaq_change_p_mid = kosdaq_pivot.pct_change(self.MID_PERIOD)
        kosdaq_change_p_long = kosdaq_pivot.pct_change(self.LONG_PERIOD)

        krx_change_p_short = (kospi_change_p_short + kosdaq_change_p_short)/2
        krx_change_p_mid = (kospi_change_p_mid + kosdaq_change_p_mid)/2
        krx_change_p_long = (kospi_change_p_long + kosdaq_change_p_long)/2

        kospi_change_pivot = kospi_pivot.pct_change(1)
        kosdaq_change_pivot = kosdaq_pivot.pct_change(1)

        kospi_change_std_short = kospi_change_pivot.rolling(self.SHORT_PERIOD).std()
        kospi_change_std_mid = kospi_change_pivot.rolling(self.MID_PERIOD).std()
        kospi_change_std_long = kospi_change_pivot.rolling(self.LONG_PERIOD).std()

        kosdaq_change_std_short = kosdaq_change_pivot.rolling(self.SHORT_PERIOD).std()
        kosdaq_change_std_mid = kosdaq_change_pivot.rolling(self.MID_PERIOD).std()
        kosdaq_change_std_long = kosdaq_change_pivot.rolling(self.LONG_PERIOD).std()

        krx_change_std_short = (kospi_change_std_short + kosdaq_change_std_short)/2
        krx_change_std_mid = (kospi_change_std_mid + kosdaq_change_std_mid)/2
        krx_change_std_long = (kospi_change_std_long + kosdaq_change_std_long)/2

        def combined_price_change_std(period):
            mean = (
                0.25*change_close_pivot.rolling(period).mean()
                + 0.25*change_high_pivot.rolling(period).mean()
                + 0.25*change_low_pivot.rolling(period).mean()
                + 0.25*change_open_pivot.rolling(period).mean()
            )

            var = (
                (change_close_pivot - mean)**2
                + (change_open_pivot - mean)**2
                + (change_high_pivot - mean)**2
                + (change_low_pivot - mean)**2
            )/4

            return np.sqrt(var)
        
        weighted_price_change_std_pivot_short = \
            combined_price_change_std(self.SHORT_PERIOD)
        weighted_price_change_std_pivot_mid = \
            combined_price_change_std(self.MID_PERIOD)
        weighted_price_change_std_pivot_long = \
            combined_price_change_std(self.LONG_PERIOD)
        
        def replace_zero_volume_to_one(x):
            return 1 if x == 0 else 0    

        def get_volume_zero_count(period):
            return (
                    vol_pivot.map(replace_zero_volume_to_one)
                    .rolling(period)
                    .sum()
                    .fillna(0)
                )
        
        vol_zero_count_short = get_volume_zero_count(self.SHORT_PERIOD)
        vol_zero_count_mid = get_volume_zero_count(self.MID_PERIOD)
        vol_zero_count_long = get_volume_zero_count(self.LONG_PERIOD)

        def get_stacked_df(df_pivot, col_name):
            return (
                df_pivot
                .stack(level=-1, dropna=False)
                .reset_index()
                .rename(columns={0:col_name})
            )

        new_features_concatenated = pd.concat(
            [                
                get_stacked_df(weighted_price_pivot, 'weighted_price'),

                get_stacked_df(vol_x_price_pivot, 'vol_x_price'),

                get_stacked_df(vol_x_price_sma_long, 'vol_x_price_sma_long'),
                get_stacked_df(vol_x_price_sma_mid, 'vol_x_price_sma_mid'),
                get_stacked_df(vol_x_price_sma_short, 'vol_x_price_sma_short'),

                get_stacked_df(change_high_pivot, 'change_high'),
                get_stacked_df(change_low_pivot, 'change_low'),
                get_stacked_df(change_open_pivot, 'change_open'),

                get_stacked_df(close_std_short_pivot, 'close_std_short'),
                get_stacked_df(close_std_mid_pivot, 'close_std_mid'),
                get_stacked_df(close_std_long_pivot, 'close_std_long'),

                get_stacked_df(open_std_short_pivot, 'open_std_short'),
                get_stacked_df(open_std_mid_pivot, 'open_std_mid'),
                get_stacked_df(open_std_long_pivot, 'open_std_long'),

                get_stacked_df(high_std_short_pivot, 'high_std_short'),
                get_stacked_df(high_std_mid_pivot, 'high_std_mid'),
                get_stacked_df(high_std_long_pivot, 'high_std_long'),

                get_stacked_df(low_std_short_pivot, 'low_std_short'),
                get_stacked_df(low_std_mid_pivot, 'low_std_mid'),
                get_stacked_df(low_std_long_pivot, 'low_std_long'),

                get_stacked_df(weighted_price_change_std_pivot_short, 'weighted_price_change_std_short'),
                get_stacked_df(weighted_price_change_std_pivot_mid, 'weighted_price_change_std_mid'),
                get_stacked_df(weighted_price_change_std_pivot_long, 'weighted_price_change_std_long'),

                get_stacked_df(vol_zero_count_short, 'vol_zero_count_short'),
                get_stacked_df(vol_zero_count_mid, 'vol_zero_count_mid'),
                get_stacked_df(vol_zero_count_long, 'vol_zero_count_long'),

                get_stacked_df(close_change_p_short, 'close_change_p_short'),
                get_stacked_df(close_change_p_mid, 'close_change_p_mid'),
                get_stacked_df(close_change_p_long, 'close_change_p_long'),

                get_stacked_df(weighted_price_change_p_short, 'weighted_price_change_p_short'),
                get_stacked_df(weighted_price_change_p_mid, 'weighted_price_change_p_mid'),
                get_stacked_df(weighted_price_change_p_long, 'weighted_price_change_p_long'),
                
                get_stacked_df(krx_corr_short, 'krx_corr_short'),
                get_stacked_df(krx_corr_mid, 'krx_corr_mid'),
                get_stacked_df(krx_corr_long, 'krx_corr_long'),

                get_stacked_df(krx_change_p_short, 'krx_change_p_short'),
                get_stacked_df(krx_change_p_mid, 'krx_change_p_mid'),
                get_stacked_df(krx_change_p_long, 'krx_change_p_long'),

                get_stacked_df(krx_change_std_short, 'krx_change_std_short'),
                get_stacked_df(krx_change_std_mid, 'krx_change_std_mid'),
                get_stacked_df(krx_change_std_long, 'krx_change_std_long'),

            ]
            ,axis=1
        )

        new_features = \
            new_features_concatenated.loc[:, ~new_features_concatenated.columns.duplicated()]


        return (
            df_price.merge(
                new_features,
                left_on=['date', 'code'],
                right_on=['date', 'code'],
                how='inner'
            )
        )
        