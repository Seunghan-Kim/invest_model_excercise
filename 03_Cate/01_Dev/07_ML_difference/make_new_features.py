import pandas as pd
import numpy as np

def get_df_with_features(
        df_price, SHORT_PERIOD=5, MID_PERIOD=20, LONG_PERIOD=60
        ):
    
    DF_PRICE = df_price
    LONG_PERIOD = LONG_PERIOD
    MID_PERIOD = MID_PERIOD
    SHORT_PERIOD = SHORT_PERIOD

    def get_pivotted_df(df, col):
        return (
            df.pivot(
                index='date',
                columns='code',
                values=col
            )
        )
    
    df_open = get_pivotted_df(DF_PRICE, 'open')
    df_high = get_pivotted_df(DF_PRICE, 'high')
    df_low = get_pivotted_df(DF_PRICE, 'low')
    df_close = get_pivotted_df(DF_PRICE, 'close')
    df_volume = get_pivotted_df(DF_PRICE, 'volume')

    df_open_diff = df_open.pct_change()
    df_high_diff = df_high.pct_change()
    df_low_diff = df_low.pct_change()
    df_close_diff = df_close.pct_change()
    df_vol_diff = np.log(df_volume).diff()

    df_open_diff_std_short = df_open_diff.rolling(SHORT_PERIOD).std()
    df_high_diff_std_short = df_high_diff.rolling(SHORT_PERIOD).std()
    df_low_diff_std_short = df_low_diff.rolling(SHORT_PERIOD).std()
    df_close_diff_std_short = df_close_diff.rolling(SHORT_PERIOD).std()
    df_vol_diff_std_short = df_vol_diff.rolling(SHORT_PERIOD).std()

    df_open_diff_std_mid = df_open_diff.rolling(MID_PERIOD).std()
    df_high_diff_std_mid = df_high_diff.rolling(MID_PERIOD).std()
    df_low_diff_std_mid = df_low_diff.rolling(MID_PERIOD).std()
    df_close_diff_std_mid = df_close_diff.rolling(MID_PERIOD).std()
    df_vol_diff_std_mid = df_vol_diff.rolling(MID_PERIOD).std()

    df_open_diff_std_short_L1 = df_open_diff_std_short.shift(SHORT_PERIOD)
    df_high_diff_std_short_L1 = df_high_diff_std_short.shift(SHORT_PERIOD)
    df_low_diff_std_short_L1 = df_low_diff_std_short.shift(SHORT_PERIOD)
    df_close_diff_std_short_L1 = df_close_diff_std_short.shift(SHORT_PERIOD)
    df_vol_diff_std_short_L1 = df_vol_diff_std_short.shift(SHORT_PERIOD)

    df_open_diff_std_short_L2 = df_open_diff_std_short.shift(SHORT_PERIOD*2)
    df_high_diff_std_short_L2 = df_high_diff_std_short.shift(SHORT_PERIOD*2)
    df_low_diff_std_short_L2 = df_low_diff_std_short.shift(SHORT_PERIOD*2)
    df_close_diff_std_short_L2 = df_close_diff_std_short.shift(SHORT_PERIOD*2)
    df_vol_diff_std_short_L2 = df_vol_diff_std_short.shift(SHORT_PERIOD*2)

    df_open_diff_std_mid_L1 = df_open_diff_std_mid.shift(MID_PERIOD)
    df_high_diff_std_mid_L1 = df_high_diff_std_mid.shift(MID_PERIOD)
    df_low_diff_std_mid_L1 = df_low_diff_std_mid.shift(MID_PERIOD)
    df_close_diff_std_mid_L1 = df_close_diff_std_mid.shift(MID_PERIOD)
    df_vol_diff_std_mid_L1 = df_vol_diff_std_mid.shift(MID_PERIOD)

    df_open_diff_std_mid_L2 = df_open_diff_std_mid.shift(MID_PERIOD*2)
    df_high_diff_std_mid_L2 = df_high_diff_std_mid.shift(MID_PERIOD*2)
    df_low_diff_std_mid_L2 = df_low_diff_std_mid.shift(MID_PERIOD*2)
    df_close_diff_std_mid_L2 = df_close_diff_std_mid.shift(MID_PERIOD*2)
    df_vol_diff_std_mid_L2 = df_vol_diff_std_mid.shift(MID_PERIOD*2)


    df_open_diff_mean_short = df_open_diff.rolling(SHORT_PERIOD).mean()
    df_high_diff_mean_short = df_high_diff.rolling(SHORT_PERIOD).mean()
    df_low_diff_mean_short = df_low_diff.rolling(SHORT_PERIOD).mean()
    df_close_diff_mean_short = df_close_diff.rolling(SHORT_PERIOD).mean()
    df_vol_diff_mean_short = df_vol_diff.rolling(SHORT_PERIOD).mean()

    df_open_diff_mean_mid = df_open_diff.rolling(MID_PERIOD).mean()
    df_high_diff_mean_mid = df_high_diff.rolling(MID_PERIOD).mean()
    df_low_diff_mean_mid = df_low_diff.rolling(MID_PERIOD).mean()
    df_close_diff_mean_mid = df_close_diff.rolling(MID_PERIOD).mean()
    df_vol_diff_mean_mid = df_vol_diff.rolling(MID_PERIOD).mean()

    df_open_diff_mean_short_L1 = df_open_diff_mean_short.shift(SHORT_PERIOD)
    df_high_diff_mean_short_L1 = df_high_diff_mean_short.shift(SHORT_PERIOD)
    df_low_diff_mean_short_L1 = df_low_diff_mean_short.shift(SHORT_PERIOD)
    df_close_diff_mean_short_L1 = df_close_diff_mean_short.shift(SHORT_PERIOD)
    df_vol_diff_mean_short_L1 = df_vol_diff_mean_short.shift(SHORT_PERIOD)

    df_open_diff_mean_short_L2 = df_open_diff_mean_short.shift(SHORT_PERIOD*2)
    df_high_diff_mean_short_L2 = df_high_diff_mean_short.shift(SHORT_PERIOD*2)
    df_low_diff_mean_short_L2 = df_low_diff_mean_short.shift(SHORT_PERIOD*2)
    df_close_diff_mean_short_L2 = df_close_diff_mean_short.shift(SHORT_PERIOD*2)
    df_vol_diff_mean_short_L2 = df_vol_diff_mean_short.shift(SHORT_PERIOD*2)

    df_open_diff_mean_mid_L1 = df_open_diff_mean_mid.shift(MID_PERIOD)
    df_high_diff_mean_mid_L1 = df_high_diff_mean_mid.shift(MID_PERIOD)
    df_low_diff_mean_mid_L1 = df_low_diff_mean_mid.shift(MID_PERIOD)
    df_close_diff_mean_mid_L1 = df_close_diff_mean_mid.shift(MID_PERIOD)
    df_vol_diff_mean_mid_L1 = df_vol_diff_mean_mid.shift(MID_PERIOD)

    df_open_diff_mean_mid_L2 = df_open_diff_mean_mid.shift(MID_PERIOD*2)
    df_high_diff_mean_mid_L2 = df_high_diff_mean_mid.shift(MID_PERIOD*2)
    df_low_diff_mean_mid_L2 = df_low_diff_mean_mid.shift(MID_PERIOD*2)
    df_close_diff_mean_mid_L2 = df_close_diff_mean_mid.shift(MID_PERIOD*2)
    df_vol_diff_mean_mid_L2 = df_vol_diff_mean_mid.shift(MID_PERIOD*2)

    weighted_price_pivot = (0.25*df_close + 0.25*df_open + 0.25*df_high + 0.25*df_low)
    vol_x_price_pivot = (df_volume * weighted_price_pivot)

    vol_x_price_sma_long = vol_x_price_pivot.rolling(LONG_PERIOD).mean()
    vol_x_price_sma_mid = vol_x_price_pivot.rolling(MID_PERIOD).mean()
    vol_x_price_sma_short = vol_x_price_pivot.rolling(SHORT_PERIOD).mean()

    w_price_vol_corr_long = weighted_price_pivot.rolling(LONG_PERIOD).corr(df_volume)
    w_price_vol_corr_mid = weighted_price_pivot.rolling(MID_PERIOD).corr(df_volume)
    w_price_vol_corr_short = weighted_price_pivot.rolling(SHORT_PERIOD).corr(df_volume)

    
    def replace_zero_volume_to_one(x):
        return 1 if x == 0 else 0    

    def get_volume_zero_count(period):
        return (
                df_volume.map(replace_zero_volume_to_one)
                .rolling(period)
                .sum()
                .fillna(0)
            )
    
    vol_zero_count_short = get_volume_zero_count(SHORT_PERIOD)
    vol_zero_count_mid = get_volume_zero_count(MID_PERIOD)
    vol_zero_count_long = get_volume_zero_count(LONG_PERIOD)

    def get_stacked_df(df_pivot, col_name):
        return (
            df_pivot
            .stack(level=-1, dropna=False)
            .reset_index()
            .rename(columns={0:col_name})
        )
    
    def get_future_rtn(p):
        return(
            df_close.pct_change(p).shift(-p)
        )
    
    new_features_concatenated = pd.concat(
        [
            
            get_stacked_df(df_open_diff_std_short, 'open_diff_std_short'),
            get_stacked_df(df_high_diff_std_short, 'high_diff_std_short'),
            get_stacked_df(df_low_diff_std_short, 'low_diff_std_short'),
            get_stacked_df(df_close_diff_std_short, 'close_diff_std_short'),
            get_stacked_df(df_vol_diff_std_short, 'vol_diff_std_short'),

            get_stacked_df(df_open_diff_std_mid, 'open_diff_std_mid'),
            get_stacked_df(df_high_diff_std_mid, 'high_diff_std_mid'),
            get_stacked_df(df_low_diff_std_mid, 'low_diff_std_mid'),
            get_stacked_df(df_close_diff_std_mid, 'close_diff_std_mid'),
            get_stacked_df(df_vol_diff_std_mid, 'vol_diff_std_mid'),

            get_stacked_df(df_open_diff_std_short_L1, 'open_diff_std_short_L1'),
            get_stacked_df(df_high_diff_std_short_L1, 'high_diff_std_short_L1'),
            get_stacked_df(df_low_diff_std_short_L1, 'low_diff_std_short_L1'),
            get_stacked_df(df_close_diff_std_short_L1, 'close_diff_std_short_L1'),
            get_stacked_df(df_vol_diff_std_short_L1, 'vol_diff_std_short_L1'),

            get_stacked_df(df_open_diff_std_short_L2, 'open_diff_std_short_L2'),
            get_stacked_df(df_high_diff_std_short_L2, 'high_diff_std_short_L2'),
            get_stacked_df(df_low_diff_std_short_L2, 'low_diff_std_short_L2'),
            get_stacked_df(df_close_diff_std_short_L2, 'close_diff_std_short_L2'),
            get_stacked_df(df_vol_diff_std_short_L2, 'vol_diff_std_short_L2'),

            get_stacked_df(df_open_diff_std_mid_L1, 'open_diff_std_mid_L1'),
            get_stacked_df(df_high_diff_std_mid_L1, 'high_diff_std_mid_L1'),
            get_stacked_df(df_low_diff_std_mid_L1, 'low_diff_std_mid_L1'),
            get_stacked_df(df_close_diff_std_mid_L1, 'close_diff_std_mid_L1'),
            get_stacked_df(df_vol_diff_std_mid_L1, 'vol_diff_std_mid_L1'),

            get_stacked_df(df_open_diff_std_mid_L2, 'open_diff_std_mid_L2'),
            get_stacked_df(df_high_diff_std_mid_L2, 'high_diff_std_mid_L2'),
            get_stacked_df(df_low_diff_std_mid_L2, 'low_diff_std_mid_L2'),
            get_stacked_df(df_close_diff_std_mid_L2, 'close_diff_std_mid_L2'),
            get_stacked_df(df_vol_diff_std_mid_L2, 'vol_diff_std_mid_L2'),

            get_stacked_df(df_open_diff_mean_short, 'open_diff_mean_short'),
            get_stacked_df(df_high_diff_mean_short, 'high_diff_mean_short'),
            get_stacked_df(df_low_diff_mean_short, 'low_diff_mean_short'),
            get_stacked_df(df_close_diff_mean_short, 'close_diff_mean_short'),
            get_stacked_df(df_vol_diff_mean_short, 'vol_diff_mean_short'),

            get_stacked_df(df_open_diff_mean_mid, 'open_diff_mean_mid'),
            get_stacked_df(df_high_diff_mean_mid, 'high_diff_mean_mid'),
            get_stacked_df(df_low_diff_mean_mid, 'low_diff_mean_mid'),
            get_stacked_df(df_close_diff_mean_mid, 'close_diff_mean_mid'),
            get_stacked_df(df_vol_diff_mean_mid, 'vol_diff_mean_mid'),

            get_stacked_df(df_open_diff_mean_short_L1, 'open_diff_mean_short_L1'),
            get_stacked_df(df_high_diff_mean_short_L1, 'high_diff_mean_short_L1'),
            get_stacked_df(df_low_diff_mean_short_L1, 'low_diff_mean_short_L1'),
            get_stacked_df(df_close_diff_mean_short_L1, 'close_diff_mean_short_L1'),
            get_stacked_df(df_vol_diff_mean_short_L1, 'vol_diff_mean_short_L1'),

            get_stacked_df(df_open_diff_mean_short_L2, 'open_diff_mean_short_L2'),
            get_stacked_df(df_high_diff_mean_short_L2, 'high_diff_mean_short_L2'),
            get_stacked_df(df_low_diff_mean_short_L2, 'low_diff_mean_short_L2'),
            get_stacked_df(df_close_diff_mean_short_L2, 'close_diff_mean_short_L2'),
            get_stacked_df(df_vol_diff_mean_short_L2, 'vol_diff_mean_short_L2'),

            get_stacked_df(df_open_diff_mean_mid_L1, 'open_diff_mean_mid_L1'),
            get_stacked_df(df_high_diff_mean_mid_L1, 'high_diff_mean_mid_L1'),
            get_stacked_df(df_low_diff_mean_mid_L1, 'low_diff_mean_mid_L1'),
            get_stacked_df(df_close_diff_mean_mid_L1, 'close_diff_mean_mid_L1'),
            get_stacked_df(df_vol_diff_mean_mid_L1, 'vol_diff_mean_mid_L1'),

            get_stacked_df(df_open_diff_mean_mid_L2, 'open_diff_mean_mid_L2'),
            get_stacked_df(df_high_diff_mean_mid_L2, 'high_diff_mean_mid_L2'),
            get_stacked_df(df_low_diff_mean_mid_L2, 'low_diff_mean_mid_L2'),
            get_stacked_df(df_close_diff_mean_mid_L2, 'close_diff_mean_mid_L2'),
            get_stacked_df(df_vol_diff_mean_mid_L2, 'vol_diff_mean_mid_L2'),

            get_stacked_df(vol_zero_count_short, 'vol_zero_count_short'),
            get_stacked_df(vol_zero_count_mid, 'vol_zero_count_mid'),
            get_stacked_df(vol_zero_count_long, 'vol_zero_count_long'),

            get_stacked_df(vol_x_price_sma_long, 'vol_x_price_sma_long'),
            get_stacked_df(vol_x_price_sma_mid, 'vol_x_price_sma_mid'),
            get_stacked_df(vol_x_price_sma_short, 'vol_x_price_sma_short'),

            get_stacked_df(w_price_vol_corr_long, 'w_price_vol_corr_long'),
            get_stacked_df(w_price_vol_corr_mid, 'w_price_vol_corr_mid'),
            get_stacked_df(w_price_vol_corr_short, 'w_price_vol_corr_short'),

            get_stacked_df(
                get_future_rtn(3), 'rtn_3'
            ),

            get_stacked_df(
                get_future_rtn(2), 'rtn_2'
            ),

            get_stacked_df(
                get_future_rtn(1), 'rtn_1'                
            ),

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


    

