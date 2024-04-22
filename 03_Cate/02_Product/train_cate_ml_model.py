#%%
# 1. Import Libraries

import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
import numpy as np
import pickle

# from make_new_features import get_df_with_features
from make_new_features_with_corr import get_df_with_features

#%%
# 2. Get df_price & Add features

df_price = pd.read_pickle("df_price_for_train_2023-06-01-2024-03-25.pkl")

SHORT_PERIOD=5
MID_PERIOD=20
LONG_PERIOD=60

date_min = df_price.date.min()
date_max = df_price.date.max()

df_with_feats = get_df_with_features(
        df_price, SHORT_PERIOD=SHORT_PERIOD, MID_PERIOD=MID_PERIOD, LONG_PERIOD=LONG_PERIOD)
df_with_feats.to_pickle(f"df_with_feats_f_{date_min}_t_{date_max}.pkl")

#%%
# 3. Ref Dates Set
date_set = ('2023-06-01', '2024-03-25', '2024-04-02', '2024-04-29')

#%%
# 4. Preparing Train Dataset

df_with_feats_ml = (
    df_with_feats
    .loc[lambda df : df.date >= date_set[0]]
    .loc[lambda df : df.date < date_set[1]]
    .loc[lambda df : df.code.str[5] == '0']
    .loc[lambda df : ~df.name.str.contains("스펙")]
    .loc[lambda df : ~df.name.str.contains("스팩")]
    .assign(
        vol_x_price_sma_long_to_mid = lambda df : df.vol_x_price_sma_long / df.vol_x_price_sma_mid,
        vol_x_price_sma_mid_to_short = lambda df : df.vol_x_price_sma_mid / df.vol_x_price_sma_short,
    )
    .loc[lambda df : df["vol_x_price_sma_mid"] >  0.75e+08]  # About 10 percentile
    .loc[lambda df : df["vol_x_price_sma_mid"] <  3.5e+08] #3.5e+08]  # About 30 percentile
    .loc[lambda df : df["vol_zero_count_mid"] == 0]
    .loc[lambda df : df["change"] < 0.29]
    .loc[lambda df : df["volume"] > 0]
    # .loc[lambda df : df["close"] > 1000]
)

#%%
# 5. Check Train Dataset dates

print(f"start date : {df_with_feats_ml.date.max()}")
print(f"end date : {df_with_feats_ml.date.min()}")
# df_with_feats_ml.head()
# df_with_feats_ml.tail()

#%%
# 6. Set features and Target

# This features are used both dev and production
# Be careful to modify!
# 

feats_rtn_3 = [
    'w_price_vol_corr_long',
    'vol_x_price_sma_mid_to_short',
    'w_price_vol_corr_short',
    'vol_x_price_sma_short',
    'w_price_vol_corr_mid',
    'close_change_p_long',
    'high_std_long',
    'krx_corr_mid',
    'krx_corr_long',
    'change_low',
    'open_std_short',
    'close_std_long',
    'high_std_short',
    'open_std_mid',
    'close_std_short',
    'change_high',
    'krx_change_std_short',
    'krx_change_std_mid',
    'close_std_mid',

    'close_mean_short', 'close_mean_mid',
    'close_mean_long', 'close_min_ratio_short', 'close_min_ratio_mid',
    'close_min_ratio_long', 'close_max_ratio_short', 'close_max_ratio_mid',
    'close_max_ratio_long',
]

feats_rtn_4 = [
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
    'w_price_vol_corr_mid',

    'close_mean_short', 'close_mean_mid',
    'close_mean_long', 'close_min_ratio_short', 'close_min_ratio_mid',
    'close_min_ratio_long', 'close_max_ratio_short', 'close_max_ratio_mid',
    'close_max_ratio_long',
]

feats_rtn_5 = [
    'vol_x_price_sma_short',
    'close_std_mid',
    'close_std_long',
    'open_std_short',
    'open_std_mid',
    'open_std_long',
    'high_std_short',
    'high_std_mid',
    'low_std_short',
    'close_change_p_long',
    'krx_corr_mid',
    'krx_corr_long',
    'krx_change_std_short',
    'krx_change_std_mid',
    'krx_change_std_long',
    'w_price_vol_corr_long',
    'w_price_vol_corr_mid',

    'close_mean_short', 'close_mean_mid',
    'close_mean_long', 'close_min_ratio_short', 'close_min_ratio_mid',
    'close_min_ratio_long', 'close_max_ratio_short', 'close_max_ratio_mid',
    'close_max_ratio_long',
]

feats_dict = {
    'rtn_3' : feats_rtn_3,
    'rtn_4' : feats_rtn_4,
    'rtn_5' : feats_rtn_5,
}

target_type = {
    'rtn_3' : True,
    'rtn_4' : True,
    'rtn_5' : False
}

#%%
# 7. Get Trained Model
for target in ["rtn_3", "rtn_4", "rtn_5"]:

    feats = feats_dict[target]
    use_fixed_target = target_type[target]

    print(f"{target} / use fixed target? : {use_fixed_target}")

    df_train_set = df_with_feats_ml

    df_train_set_ = (
        df_train_set
        .replace([-np.inf, np.inf], np.nan)
        .dropna(subset=feats)
        .reset_index(drop=True)
    )

    if use_fixed_target :
        THRES_OF_TRUE = 0.03 # check point
        surfix = 'fixed'
    else :
        THRES_OF_TRUE = df_train_set_[target].quantile(0.75)
        surfix = 'float'

    print(THRES_OF_TRUE)

    df_train_set_["target"] = (
        df_train_set_[target].apply(lambda x : 1 if x > THRES_OF_TRUE else 0)
    )

    feature_df = df_train_set_[feats]
    target_df = df_train_set_["target"]

    l_of_models = []
    for rnd_seed in [6, 13, 5, 2]:
        X_train, X_valid, y_train, y_valid = train_test_split(
            feature_df, target_df, test_size=0.3, random_state=rnd_seed
        )

        # Create DMatrix for training and validation data
        dtrain = xgb.DMatrix(X_train, label=y_train)
        dvalid = xgb.DMatrix(X_valid, label=y_valid)

        evals = [(dtrain, 'train'), (dvalid, 'eval')]

        params = {
            'max_depth': 5, # Adjust based on your dataset : originally 5 --> try 8
            'eta': 0.05,     # Learning rate : originally 0.05
            'objective': 'binary:logistic',
            'eval_metric': 'logloss',  # Or use 'auc', 'error', etc. based on your problem
            'random_state': 42,
        }

        model = xgb.train(
            params, dtrain,
            1000,
            evals=evals,
            early_stopping_rounds=50,
            verbose_eval=False
        )

        l_of_models.append(model)

    with open(f"cate_model_list_ksh_{date_max}_{target}_{surfix}.pkl", "wb") as f:
        pickle.dump(l_of_models, f)

    print(f"{target}")

# %%
