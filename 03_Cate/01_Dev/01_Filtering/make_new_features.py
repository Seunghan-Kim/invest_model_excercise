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
    # krx_pivot = get_rolling_sma('krx',1)

    kosdaq_pivot = get_rolling_sma('kosdaq', 1)
    kospi_pivot = get_rolling_sma('kospi', 1)

    weighted_price_pivot = (0.25*close_pivot + 0.25*open_pivot + 0.25*high_pivot + 0.25*low_pivot)

    vol_pivot = get_rolling_sma('volume', 1)
    vol_x_price_pivot = (vol_pivot * weighted_price_pivot)

    vol_x_price_sma_long = vol_x_price_pivot.rolling(LONG_PERIOD).mean()
    vol_x_price_sma_mid = vol_x_price_pivot.rolling(MID_PERIOD).mean()
    vol_x_price_sma_short = vol_x_price_pivot.rolling(SHORT_PERIOD).mean()
    
    change_close_pivot = get_rolling_sma('change', 1)
    change_high_pivot = high_pivot / close_pivot.shift(1) - 1
    change_low_pivot = low_pivot / close_pivot.shift(1) - 1
    change_open_pivot = open_pivot / close_pivot.shift(1) - 1

    close_std_short_pivot = change_close_pivot.rolling(SHORT_PERIOD).std()
    close_std_mid_pivot = change_close_pivot.rolling(MID_PERIOD).std()
    close_std_long_pivot = change_close_pivot.rolling(LONG_PERIOD).std()

    open_std_short_pivot = change_open_pivot.rolling(SHORT_PERIOD).std()
    open_std_mid_pivot = change_open_pivot.rolling(MID_PERIOD).std()
    open_std_long_pivot = change_open_pivot.rolling(LONG_PERIOD).std()

    high_std_short_pivot = change_high_pivot.rolling(SHORT_PERIOD).std()
    high_std_mid_pivot = change_high_pivot.rolling(MID_PERIOD).std()
    high_std_long_pivot = change_high_pivot.rolling(LONG_PERIOD).std()

    low_std_short_pivot = change_low_pivot.rolling(SHORT_PERIOD).std()
    low_std_mid_pivot = change_low_pivot.rolling(MID_PERIOD).std()
    low_std_long_pivot = change_low_pivot.rolling(LONG_PERIOD).std()

    close_change_p_short = close_pivot.pct_change(SHORT_PERIOD)
    close_change_p_mid = close_pivot.pct_change(MID_PERIOD)
    close_change_p_long = close_pivot.pct_change(LONG_PERIOD)

    weighted_price_change_p_short = weighted_price_pivot.pct_change(SHORT_PERIOD)
    weighted_price_change_p_mid = weighted_price_pivot.pct_change(MID_PERIOD)
    weighted_price_change_p_long = weighted_price_pivot.pct_change(LONG_PERIOD)
    
    # krx_corr_short = close_pivot.rolling(SHORT_PERIOD).corr(krx_pivot)
    # krx_corr_mid = close_pivot.rolling(MID_PERIOD).corr(krx_pivot)
    # krx_corr_long = close_pivot.rolling(LONG_PERIOD).corr(krx_pivot)

    # krx_change_p_short = krx_pivot.pct_change(SHORT_PERIOD)
    # krx_change_p_mid = krx_pivot.pct_change(MID_PERIOD)
    # krx_change_p_long = krx_pivot.pct_change(LONG_PERIOD)

    kospi_corr_short = close_pivot.rolling(SHORT_PERIOD).corr(kospi_pivot)
    kospi_corr_mid = close_pivot.rolling(MID_PERIOD).corr(kospi_pivot)
    kospi_corr_long = close_pivot.rolling(LONG_PERIOD).corr(kospi_pivot)

    kosdaq_corr_short = close_pivot.rolling(SHORT_PERIOD).corr(kosdaq_pivot)
    kosdaq_corr_mid = close_pivot.rolling(MID_PERIOD).corr(kosdaq_pivot)
    kosdaq_corr_long = close_pivot.rolling(LONG_PERIOD).corr(kosdaq_pivot)

    krx_corr_short = (kospi_corr_short + kosdaq_corr_short)/2
    krx_corr_mid = (kospi_corr_mid + kosdaq_corr_mid)/2
    krx_corr_long = (kospi_corr_long + kosdaq_corr_long)/2

    kospi_change_p_short = kospi_pivot.pct_change(SHORT_PERIOD)
    kospi_change_p_mid = kospi_pivot.pct_change(MID_PERIOD)
    kospi_change_p_long = kospi_pivot.pct_change(LONG_PERIOD)

    kosdaq_change_p_short = kosdaq_pivot.pct_change(SHORT_PERIOD)
    kosdaq_change_p_mid = kosdaq_pivot.pct_change(MID_PERIOD)
    kosdaq_change_p_long = kosdaq_pivot.pct_change(LONG_PERIOD)

    krx_change_p_short = (kospi_change_p_short + kosdaq_change_p_short)/2
    krx_change_p_mid = (kospi_change_p_mid + kosdaq_change_p_mid)/2
    krx_change_p_long = (kospi_change_p_long + kosdaq_change_p_long)/2

    kospi_change_pivot = kospi_pivot.pct_change(1)
    kosdaq_change_pivot = kosdaq_pivot.pct_change(1)

    kospi_change_std_short = kospi_change_pivot.rolling(SHORT_PERIOD).std()
    kospi_change_std_mid = kospi_change_pivot.rolling(MID_PERIOD).std()
    kospi_change_std_long = kospi_change_pivot.rolling(LONG_PERIOD).std()

    kosdaq_change_std_short = kosdaq_change_pivot.rolling(SHORT_PERIOD).std()
    kosdaq_change_std_mid = kosdaq_change_pivot.rolling(MID_PERIOD).std()
    kosdaq_change_std_long = kosdaq_change_pivot.rolling(LONG_PERIOD).std()

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
        combined_price_change_std(SHORT_PERIOD)
    weighted_price_change_std_pivot_mid = \
        combined_price_change_std(MID_PERIOD)
    weighted_price_change_std_pivot_long = \
        combined_price_change_std(LONG_PERIOD)
    
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
    
    def get_future_rtn_krx(p):
        return(
            # krx_pivot.pct_change(p).shift(-p)
            kospi_pivot.pct_change(p).shift(-p)*0.5
            + kosdaq_pivot.pct_change(p).shift(-p)*0.5
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

            get_stacked_df(
                get_future_rtn(5), 'rtn_5'
            ),
            get_stacked_df(
                get_future_rtn(10), 'rtn_10'
            ),
            get_stacked_df(
                get_future_rtn(20), 'rtn_20'
            ),
            get_stacked_df(
                get_future_rtn(30), 'rtn_30'
            ),
            get_stacked_df(
                get_future_rtn_krx(20), 'rtn_20_krx'
            ),
            get_stacked_df(
                get_future_rtn_krx(30), 'rtn_30_krx'
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


    

