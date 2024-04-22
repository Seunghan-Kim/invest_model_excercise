import pandas as pd
import numpy as np

class Alicia2023:

    def __init__(self, today) -> None:
        
        self.today = today

        self.money_per_day = 4_200_000
        self.sell_days_after_01 = 3
        self.sell_days_after_02 = 5
        
        self.params_up_231003 = {
            'market_cap_min': 800_00_000_000,
            'sharpe': 3.354724270328286,
            'threshold_up_change_n_lower': 0.3726163635401358,
            'threshold_up_change_upper': 0.049109267782217095,
            'threshold_up_corr_sma_upper': -0.8638508533655092,
        }

        self.params_down_231003 = {
            'market_cap_min': 300_00_000_000,
            'sharpe': 9.123920699592363,
            'threshold_down_change_lower': -0.22081465374985326,
            'threshold_down_change_n_lower': -0.45707312123709265,
            'threshold_down_change_n_upper': -0.1387482853650226,
            'threshold_down_change_upper': 1.3152503566314026e-05,
            'threshold_down_corr_sma_upper': -0.7830000805869163
        }

        self.params_center_231003 = {
            'market_cap_min': 500_00_000_000,
            'sharpe': 2.2984759541245254,
            'threshold_center_change_lower': -0.23560421854894492,
            'threshold_center_change_n_lower': -0.4529180389675452,
            'threshold_center_change_n_upper': -0.3096086668501585,
            'threshold_center_corr_sma_lower': -0.45535355674392897,
            'threshold_center_corr_sma_upper': 0.055294435577721575
        }

        self.basic_param = {
            'top_n_ref' : "change",
            'n_stock_daily' : 5,  # 매일 5개씩 매수

            # Fixed params
            'sma_period' : 3,
            'corr_n' : 6,
            'change_n_factor' : 1.9321155707294224,

            'fac_sma_close' : 0.25528644188708627,  # close 이동평균 가중치
            'fac_sma_high' : 0.1618121415309444,  # high 이동평균 가중치
            'fac_sma_low' : 0.125,  # low 이동평균 가중치
            'fac_sma_open' : 0.1951124798427969,  # open 이동평균 가중치
        }

        self.param_231218 = {
            'market_cap_min': 500e+08,  
            'market_cap_max': 3000e+08,
        
            'corr_sma_upper': -0.616,
            'corr_sma_lower': -0.5,
            'change_sma_upper': 0.362,
            'change_sma_lower': -0.272,
        
            'sma_period': 4,
            'corr_n': 5,
            'change_n_factor': 1.1,
        
            'fac_sma_close': 1.0,  # close 이동평균 가중치
            'fac_sma_high': 0.895,  # high 이동평균 가중치
            'fac_sma_low': 0.8,
            'fac_sma_open': 0.98,  # open 이동평균 가중치
        
            'top_n_ref' : 'close_change_sma',
            'top_n_ref_order' : True,    
        
            'price_x_volume_sma_10_upper' : 14_00_000_000,
            'change_std_upper' : 0.057999999999999996,
            'change_std_lower' : 0.028,
        
            'sharpe_lower' : 3.398,
            'sharpe_upper' : 5.818,
        }

    def get_model_alicia_recommendation(self, df_price):
        print(f" latest date of df_price in Alicia : {df_price['date'].max()}")
        df_feats = self.get_df_feats_of_model_alicia(df_price)
        df_filtered = self.get_filtered_df(df_feats)
        
        print(f"alicia size before today filter : {df_filtered.shape[0]}")
        
        df_filtered_today = (
            df_filtered
            .loc[lambda df : df.date == self.today]
            .assign(
                model_name = 'alicia'
            )
            .reset_index(drop=True)
        )

        n_of_stocks = df_filtered_today.shape[0]    
        print(f"alicia size after today filter: {n_of_stocks}")        

        if n_of_stocks > 0:
            print("Alicia Recommends")
            print(df_filtered_today[['date','name']])
            
            money_per_stock = self.money_per_day / n_of_stocks
            
            df_filtered_today['buy_qty'] = df_filtered_today['close'].apply(
                lambda x : int(money_per_stock / x)
            )

            return df_filtered_today

        else :
            return pd.DataFrame()

    def get_df_feats_of_model_alicia(self, df_price):
        """
        Add features for model Alicia

        Args.
        df_price (dataframe) : date, code, name and ohclv

        Returns
        df_price_feats (dataframe)
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
        change_pivot = get_rolling_sma('change', 1)
    
        weighted_price_pivot = (
            self.param_231218["fac_sma_open"] * open_pivot
            + self.param_231218["fac_sma_close"] * close_pivot
            + self.param_231218["fac_sma_high"] * high_pivot
            + self.param_231218["fac_sma_low"] * low_pivot
        ) / (self.param_231218["fac_sma_open"] + self.param_231218["fac_sma_close"] + self.param_231218["fac_sma_high"] + self.param_231218["fac_sma_low"])
        
        vol_pivot = get_rolling_sma('volume', 1)    
    
        price_x_volume_pivot = weighted_price_pivot * vol_pivot
        price_x_volume_sma_5 = price_x_volume_pivot.rolling(5).mean()
        price_x_volume_sma_10 = price_x_volume_pivot.rolling(10).mean()
    
        close_sma_pivot = get_rolling_sma('close', self.param_231218["sma_period"])
        open_sma_pivot = get_rolling_sma('open', self.param_231218["sma_period"])
        high_sma_pivot = get_rolling_sma('high', self.param_231218["sma_period"])
        low_sma_pivot = get_rolling_sma('low', self.param_231218["sma_period"])   
        vol_sma_pivot = get_rolling_sma('volume', self.param_231218["sma_period"])
    
        weighted_price_sma = (
             self.param_231218["fac_sma_open"] * close_sma_pivot
            + self.param_231218["fac_sma_close"] * open_sma_pivot
            + self.param_231218["fac_sma_high"] * high_sma_pivot
            + self.param_231218["fac_sma_low"] * low_sma_pivot
        ) / (self.param_231218["fac_sma_open"] + self.param_231218["fac_sma_close"] + self.param_231218["fac_sma_high"] + self.param_231218["fac_sma_low"])
    
    
        w_price_vol_corr = weighted_price_sma.rolling(self.param_231218["corr_n"]).corr(vol_sma_pivot)
    
        change_n = int(self.param_231218["change_n_factor"] * self.param_231218["corr_n"])
        change_n = max(change_n, 1) 
    
        close_change_sma = close_pivot.pct_change(change_n)
        weighted_price_change_sma = weighted_price_pivot.pct_change(change_n)
    
        change_std_sma = change_pivot.rolling(change_n).std()
        sharpe = abs(close_change_sma/change_std_sma)
    
        def replace_zero_volume_to_one(x):
            return 1 if x == 0 else 0    
    
        def get_volume_zero_count(period):
            return (
                    vol_pivot.map(replace_zero_volume_to_one)
                    .rolling(period)
                    .sum()
                    .fillna(0)
                )
        
        vol_zero_count_12 = get_volume_zero_count(12)
    
        def get_stacked_df(df_pivot, col_name):
            return (
                df_pivot
                .stack(level=-1, dropna=False)
                .reset_index()
                .rename(columns={0:col_name})
            )
        
        def get_future_rtn(p):
            return(
                close_pivot.pct_change(p).shift(-p)
            )
    
        new_features_concatenated = pd.concat(
            [
                
                get_stacked_df(w_price_vol_corr, 'w_price_vol_corr'),
                get_stacked_df(close_change_sma, 'close_change_sma'),
                get_stacked_df(weighted_price_change_sma, 'weighted_price_change_sma'),
    
                get_stacked_df(price_x_volume_sma_5, 'price_x_volume_sma_5'),
                get_stacked_df(price_x_volume_sma_10, 'price_x_volume_sma_10'),
                get_stacked_df(vol_zero_count_12, 'vol_zero_count_12'),
    
                get_stacked_df(change_std_sma, 'change_std_sma'),
                get_stacked_df(sharpe, 'sharpe'),
                
                # get_stacked_df(
                #     get_future_rtn(5), 'rtn_5'
                # ),
    
                # get_stacked_df(
                #     get_future_rtn(3), 'rtn_3'
                # ),
                #  get_stacked_df(
                #     get_future_rtn(4), 'rtn_4'
                # )
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
        # corr_sma
        # change_sma
        # sharpe
    
    def get_filtered_df(self, df):
        """
        Apply filter / ML model / DL model etc.

        Args.
        dataframe with features

        Returns.
        filtered dataframe
        """
        df_filtered = (
            df
            .loc[lambda df : df.vol_zero_count_12 == 0]
            .loc[lambda df : df.change < 0.29]
            .replace([-np.inf, np.inf], np.nan)
            .dropna()
            
            .loc[lambda df : df["price_x_volume_sma_10"] > 0.5e+08]
            .loc[lambda df : df["marcap"] > self.param_231218["market_cap_min"]]
            .loc[lambda df : df["marcap"] < self.param_231218["market_cap_max"]]
            # .loc[lambda df : df["w_price_vol_corr"] > conf.corr_sma_lower]
            .loc[lambda df : df["w_price_vol_corr"] < self.param_231218["corr_sma_upper"]]
            .loc[lambda df : df["weighted_price_change_sma"] > self.param_231218["change_sma_lower"]]
            .loc[lambda df : df["weighted_price_change_sma"] < self.param_231218["change_sma_upper"]]
            .loc[lambda df : df["change_std_sma"] > self.param_231218["change_std_lower"]]
            .loc[lambda df : df["change_std_sma"] < self.param_231218["change_std_upper"]]
            .loc[lambda df : df["sharpe"] > self.param_231218["sharpe_lower"]]
            .loc[lambda df : df["sharpe"] < self.param_231218["sharpe_upper"]]
            .loc[lambda df : df["price_x_volume_sma_10"] < self.param_231218["price_x_volume_sma_10_upper"]]
        )

        return (
            df_filtered
            .sort_values(by=self.param_231218["top_n_ref"], ascending=self.param_231218["top_n_ref_order"])
            .groupby('date')
            .head()
            .reset_index()
        )

    def filter_out_downs(self, df, conf_dict):
        """
        Get filteration result

        Args.
        dataframe : dataframe with features
        config dictionary

        Returns.
        dataframe
        """

        df = (
            df
            .loc[lambda df : df['marcap'] > conf_dict['market_cap_min']]
            # .loc[lambda df : df['corr_sma'] > conf_dict['threshold_down_corr_sma_lower']]
            .loc[lambda df : df['corr_sma'] < conf_dict['threshold_down_corr_sma_upper']]
            .loc[lambda df : df['change_sma'] > conf_dict['threshold_down_change_n_lower']]
            .loc[lambda df : df['change_sma'] <  conf_dict['threshold_down_change_n_upper']]
            # .loc[lambda df : df['change'] > conf_dict['threshold_down_change_lower']]
            .loc[lambda df : df['change'] < conf_dict['threshold_down_change_upper']]
            .loc[lambda df : df['sharpe'] > conf_dict['sharpe']]
            .loc[lambda df : df.change < 0.29]
            .assign(src="하락")
        )

        return df

    def filter_out_ups(self, df, conf_dict):
        df = (
            df
            .loc[lambda df : df['marcap'] > conf_dict['market_cap_min']]
            # .loc[lambda df : df['marcap'] < conf.market_cap_max]
            # .loc[lambda df : df['corr_sma'] > conf_dict['threshold_up_corr_sma_lower']]
            .loc[lambda df : df['corr_sma'] < conf_dict['threshold_up_corr_sma_upper']]
            .loc[lambda df : df['change_sma'] > conf_dict['threshold_up_change_n_lower']]
            # .loc[lambda df : df['change_sma'] < conf_dict['threshold_up_change_n_upper']]
            # .loc[lambda df : df['change'] > conf_dict['threshold_up_change_lower']]
            .loc[lambda df : df['change'] < conf_dict['threshold_up_change_upper']]
            .loc[lambda df : df['sharpe'] > conf_dict['sharpe']]
            .loc[lambda df : df.change < 0.29]
            .assign(src="상승")           
        )
        return df

    def filter_out_centers(self, df, conf_dict):
        df = (
            df
            .loc[lambda df : df['marcap'] > conf_dict['market_cap_min']]
            # .loc[lambda df : df['marcap'] < conf.market_cap_max]
            .loc[lambda df : df['corr_sma'] > conf_dict['threshold_center_corr_sma_lower']]
            .loc[lambda df : df['corr_sma'] < conf_dict['threshold_center_corr_sma_upper']]
            .loc[lambda df : df['change_sma'] > conf_dict['threshold_center_change_n_lower']]
            .loc[lambda df : df['change_sma'] < conf_dict['threshold_center_change_n_upper']]
            .loc[lambda df : df['change'] > conf_dict['threshold_center_change_lower']]
            # .loc[lambda df : df['change'] < conf_dict['threshold_center_change_upper']]
            .loc[lambda df : df['sharpe'] > conf_dict['sharpe']]
            .loc[lambda df : df.change < 0.29]
            .assign(src="중앙")
        )
        return df
    
   