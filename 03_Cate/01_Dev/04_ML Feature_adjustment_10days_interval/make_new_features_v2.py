import pandas as pd
import numpy as np

def get_df_with_features(
        df_price, SHORT_PERIOD=5, MID_PERIOD=20, LONG_PERIOD=60
        ):
    
    LONG_PERIOD = LONG_PERIOD
    MID_PERIOD = MID_PERIOD
    SHORT_PERIOD = SHORT_PERIOD

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
    vol_pivot = get_rolling_sma('volume',1)

    w_price_pivot = (0.4*close_pivot + 0.1*open_pivot + 0.4*high_pivot + 0.1*low_pivot)

    ratio_pivot_dict = {}
    for i in range(1, 10):
        ratio_pivot_dict[f"ratio_close_{i}d_ago"] = (
            (close_pivot.shift(i) / close_pivot - 1)*100
        ).round(0)

        ratio_pivot_dict[f"ratio_open_{i}d_ago"] = (
            (open_pivot.shift(i) / close_pivot - 1)*100
        ).round(0)

        ratio_pivot_dict[f"ratio_high_{i}d_ago"] = (
            (high_pivot.shift(i) / close_pivot -1)*100
        ).round(0)

        ratio_pivot_dict[f"ratio_low_{i}d_ago"] = (
            (low_pivot.shift(i) / close_pivot - 1)*100
        ).round(0)

        ratio_pivot_dict[f"ratio_vol_{i}d_ago"] = (
            (vol_pivot.shift(i) / vol_pivot - 1)*100
        ).round(0)

    w_price_sma_short_pivot = w_price_pivot.rolling(SHORT_PERIOD).mean()
    w_price_sma_mid_pivot = w_price_pivot.rolling(MID_PERIOD).mean()
    w_price_sma_long_pivot = w_price_pivot.rolling(LONG_PERIOD).mean()

    delta_w_price_sma_short_pivot = (
        (w_price_sma_short_pivot - w_price_sma_short_pivot.shift(1))/ w_price_sma_short_pivot.shift(1)*100
        ).round(0)
    delta_w_price_sma_mid_pivot = (
        (w_price_sma_mid_pivot - w_price_sma_mid_pivot.shift(1))/ w_price_sma_mid_pivot.shift(1)*100
        ).round(0)
    delta_w_price_sma_long_pivot = (
        (w_price_sma_long_pivot - w_price_sma_long_pivot.shift(1))/ w_price_sma_long_pivot.shift(1)*100
        ).round(0)

    sma_mid_by_short = ((w_price_sma_mid_pivot / w_price_sma_short_pivot - 1)*100).round(0)
    sma_long_by_short = ((w_price_sma_long_pivot / w_price_sma_short_pivot -1)*100).round(0)
    
    def replace_zero_volume_to_one(x):
        return 1 if x == 0 else 0    

    def get_volume_zero_count(period):
        return (
                vol_pivot.map(replace_zero_volume_to_one)
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
            close_pivot.pct_change(p).shift(-p)
        )
    
    def get_future_rtn_open(p):
        return(
            open_pivot.shift(-p) / close_pivot - 1
        )
    
    l_of_feats = []
    for k, v in ratio_pivot_dict.items():
        l_of_feats.append(
            get_stacked_df(v, k)
        )

    l_of_feats.append(
        get_stacked_df(delta_w_price_sma_short_pivot, 'delta_w_price_sma_short_pivot')
    )

    l_of_feats.append(
        get_stacked_df(delta_w_price_sma_mid_pivot, 'delta_w_price_mid_short_pivot')
    )

    l_of_feats.append(
        get_stacked_df(delta_w_price_sma_long_pivot, 'delta_w_price_long_short_pivot')
    )

    l_of_feats.append(
        get_stacked_df(sma_mid_by_short, 'sma_mid_by_short')
    )

    l_of_feats.append(
        get_stacked_df(sma_long_by_short, 'sma_long_by_short')
    )

    l_of_feats.append(
        get_stacked_df(vol_zero_count_short, 'vol_zero_count_short')
    )

    l_of_feats.append(
        get_stacked_df(vol_zero_count_mid, 'vol_zero_count_mid')
    )

    l_of_feats.append(
        get_stacked_df(vol_zero_count_long, 'vol_zero_count_long')
    )
    

    new_features_concatenated = pd.concat(
        l_of_feats +
        [   
            
            get_stacked_df(
                get_future_rtn(3), 'rtn_3'
            ),

            get_stacked_df(
                get_future_rtn(4), 'rtn_4'
            ),

            get_stacked_df(
                get_future_rtn(5), 'rtn_5'                
            ),

            get_stacked_df(
                get_future_rtn(1), 'rtn_1'
            ),

            get_stacked_df(
                get_future_rtn(2), 'rtn_2'
            ),

            get_stacked_df(
                get_future_rtn_open(1), 'rtn_open_1'
            ),

            get_stacked_df(
                get_future_rtn_open(2), 'rtn_open_2'
            )
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


    

