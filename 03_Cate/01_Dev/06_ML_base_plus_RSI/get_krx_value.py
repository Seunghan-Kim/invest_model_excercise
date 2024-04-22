import FinanceDataReader as fdr

def get_krx_mean():

    df_kospi = (
        fdr.DataReader("KS11", '2013-01-01')
        .reset_index()
        .rename(columns={"Close" : "kospi"})
        [['Date', 'kospi']]
    )

    df_kosdaq = (
        fdr.DataReader("KQ11", '2013-01-01')
        .reset_index()
        .rename(columns={"Close" : "kosdaq"})
        [['Date', 'kosdaq']]
    )

    df_krx = (
        df_kospi
        .merge(
            df_kosdaq,
            on='Date'
        )
        # .set_index('Date')
        # .mean(axis=1)
        # .reset_index()
        # .rename(columns={"Date" : "date", 0 : "krx"})
        .rename(columns={"Date" : "date"})
    )

    print(df_krx.columns)
    
    df_krx['date'] = df_krx['date'].dt.strftime("%Y-%m-%d")

    return df_krx