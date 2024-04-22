#%%
from LIBRARIES.handler_google_sheet import GoogleSheetHandler
from LIBRARIES.handler_eBest_api import EBestApiHandler
# %%
gsh = GoogleSheetHandler()
eah = EBestApiHandler()

# %%
df_original_order = gsh.get_df_from_sheet('original_order')

#%%
df_holding = (
    df_original_order
    .loc[lambda df : df.ord_type == 'buy']
)

#%% 
df_current_price = (
    eah.get_current_stock_price(
        df_holding['stock_code'].unique().tolist(),
        '2024-03-08'
    )
    .rename(columns={'code':'stock_code'})
    [['stock_code', 'close']]
)

#%%
df_current_rtn = (
    df_holding
    .merge(
        df_current_price,
        on='stock_code',
    )
    .assign(
        model_name = lambda df : df.ord_id.apply(lambda x : x.split('-')[0]),
        rtn_now = lambda df : (df.close.astype(int)/df.set_price.astype(int) - 1)*100
    )
    .groupby(['created_date', 'model_name'])
    ['rtn_now']
    .mean()
    .reset_index()
    # .loc[lambda df : df.rtn_now > 0.05]
)


# %%
for id, row in df_current_rtn.iterrows():
    print(f"{row['created_date']} / {row['model_name']} / {round(row['rtn_now'],1)}%")
# %%
