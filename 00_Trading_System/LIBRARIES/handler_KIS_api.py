IS_RUN_ON_DATABUTTON = False

import json
import requests
import pandas as pd
from functools import partial
import time
import multiprocessing as mp
import math

if IS_RUN_ON_DATABUTTON:
    import databutton as db

else :
    from dotenv import load_dotenv
    import os

class KisApiHandler:

    def __init__(self):

        if IS_RUN_ON_DATABUTTON:
            self.key = db.secrets.get("axolotls_app_key")  # "발급받은 API KEY"
            self.secret = db.secrets.get("axolotls_app_secret")  # "발급받은 API SECRET"
            self.acc_no = db.secrets.get("axolotls_account_no")  # 한투 계좌

        else :    
            load_dotenv()
            self.key = os.getenv('axolotls_app_key')
            self.secret = os.getenv('axolotls_app_secret')
            self.acc_no = os.getenv('axolotls_account_no')

        self.TOKEN = self.get_kis_access_token()

    def get_kis_access_token(self):
        """
        Get KIS API Acess Token

        Arg. : None
        Returns : token string
        """
        
        try:            
            if IS_RUN_ON_DATABUTTON:
                resp_data = db.storage.json.get("KIS_TOKEN")
            else :
                with open('./STORAGE/KIS_TOKEN.json') as f:
                    resp_data = json.load(f)

        except Exception as Err:
            print(f"TOKEN ISSUE ERR : {Err}")

            base_url = "https://openapi.koreainvestment.com:9443"
            path = "oauth2/tokenP"
            url = f"{base_url}/{path}"
            headers = {"content-type": "application/json"}
            data = {
                "grant_type": "client_credentials",
                "appkey": self.key,
                "appsecret": self.secret,
            }

            resp = requests.post(url, headers=headers, data=json.dumps(data))
            resp_data = resp.json()

            if IS_RUN_ON_DATABUTTON:
                db.storage.json.put("KIS_TOKEN", resp_data)
            else :
                with open('./STORAGE/KIS_TOKEN.json', 'w') as f:
                    json.dump(resp_data, f)
        
        return f'Bearer {resp_data["access_token"]}'
    
    def get_trading_cal_df(self, date_ref):
        """
        Get KRX Business days about 20 days from today

        Args :
        date_ref (str) : Reference Date. "YYYY-MM-DD"
        
        Returns :
        dataframe : The dataframe that has date and businessday infos

        """
        base_url = "https://openapi.koreainvestment.com:9443"
        path = "uapi/domestic-stock/v1/quotations/chk-holiday"
        url = f"{base_url}/{path}"

        params = {
            "BASS_DT": date_ref.replace("-", ''),
            "CTX_AREA_NK": "",
            "CTX_AREA_FK": ""
        }

        headers = {
            "Content-Type": "application/json",
            "authorization": self.TOKEN,
            "appKey": self.key,
            "appSecret": self.secret,
            "tr_id": "CTCA0903R",
        }

        resp = requests.get(url, headers=headers, params=params)
        res = resp.json()

        df_ = pd.DataFrame.from_dict(res["output"])

        return df_
    
    def get_bdate_n_days_later(self, n_days, date_ref):
        """
        Get business date n days later from date_ref

        Args.
        n_days (int), date_ref (str, "YYYY-mm-dd)

        Returns
        date (str) : "YYYY-mm-dd"
        """        
        l_df = []
        for i in range(2):
            df_trading_cal = self.get_trading_cal_df(date_ref)

            df_market_on_date_ = (
                df_trading_cal
                .loc[lambda df : df.opnd_yn == "Y"]
                .reset_index(drop=True)
            )

            l_df.append(df_market_on_date_)

            date_ref = df_market_on_date_['bass_dt'].iloc[-1]

            time.sleep(0.5)

        df_market_on_date = (
            pd.concat(l_df)
            .drop_duplicates(subset='bass_dt')
            .sort_values(by='bass_dt', ascending=True)
            .reset_index(drop=True)
            )
        
        date_str = df_market_on_date.loc[n_days, 'bass_dt']
        
        return (
            date_str[:4] + '-' + date_str[4:6] + '-' + date_str[6:]
        )
    
    def get_bdate_n_days_before(self, n_days, date_ref):
        """
        Get business date n days prior to the date_ref

        Args.
        n_days (int), date_ref (str, "YYYY-mm-dd)
    
        Returns
        date (str) : "YYYY-mm-dd"
        """

        l_bdates = []
        current_date = date_ref
        stop_loop = False
        while not stop_loop:
            df_bdates = self.get_trading_cal_df(current_date)
            l_bdates.append(df_bdates)            
            
            df_bdates = (
                pd.concat(l_bdates)
                .drop_duplicates()
                .sort_values('bass_dt', ascending=False)
                .loc[lambda df : df.bass_dt <= date_ref.replace('-','')]
                .loc[lambda df : df.opnd_yn == 'Y']
                .reset_index(drop=True)
            )

            if df_bdates.index[-1] > n_days:
                stop_loop = True
            else :
                date_10_days_ago =  pd.to_datetime(current_date) - pd.Timedelta(days=10)
                current_date = date_10_days_ago.strftime("%Y-%m-%d")
                time.sleep(0.5)

            # print(f"B dates df : {df_bdates.tail(1)}")

        date_str = df_bdates.loc[n_days, 'bass_dt']
        
        return (
            date_str[:4] + '-' + date_str[4:6] + '-' + date_str[6:]
        )
    
    def is_market_open(self, date_ref):
        """
        Check is the KRX market open

        Args.
        date_ref (str) : "YYYY-mm-dd"

        Returns.
        Boolean : True / False
        """
        df_trading_cal = self.get_trading_cal_df(date_ref)

        if df_trading_cal.iloc[0]["opnd_yn"] == "Y":
            return True
        else :
            return False

    def request_stock_price_in_specific_period(self, code, start_date, end_date):
        """
        Get Stock price in the specific period

        Args.
        code (str) : stock code 
        start_date (str): date of start "YYYYmmdd"
        end_date (str): date of end "YYYYmmdd" 

        Returns.
        df_stock_price (dataframe) : price of single stock
        """       

        base_url = "https://openapi.koreainvestment.com:9443"
        path = "uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        url = f"{base_url}/{path}"

        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": code,
            "FID_INPUT_DATE_1" : start_date,
            "FID_INPUT_DATE_2" : end_date,
            "fid_org_adj_prc": "0000000000",
            "fid_period_div_code": "D",
        }

        headers = {
            "Content-Type": "application/json",
            "authorization": self.TOKEN,
            "appKey": self.key,
            "appSecret": self.secret,
            "tr_id": "FHKST03010100",
        }

        resp = requests.get(url, headers=headers, params=params)
        res = resp.json()

        df_ = (
            pd.DataFrame.from_dict(res["output2"])
            .assign(code=code)
            .sort_values(by="stck_bsop_date", ascending=True)            
        )

        return df_

    def get_df_price_n_days(self, codes_list, n_days, date_ref):
        """
        Get price and volume datframe of given stock code list

        Arg.
        codes_list (list of str), n_days (int), date_ref (str) : "YYYY-mm-dd"

        Returns.
        dataframe of price
        """
        LEN_OF_CODES = codes_list.__len__()

        end_date = date_ref.replace('-','')
        start_date = self.get_bdate_n_days_before(n_days, date_ref).replace('-','')

        if IS_RUN_ON_DATABUTTON :
            N = 50
            RANGE = int(LEN_OF_CODES / N)
            # RANGE = 0

            l_df = []
            for i in range(0, RANGE + 1):
                codes_partial = codes_list[i * N : (i + 1) * N]
                with mp.Pool(mp.cpu_count()) as pool:
                    fetch_calc_data_partial = partial(
                        # get_price_from_KIS
                        self.request_stock_price_in_specific_period,
                        start_date = start_date,
                        end_date = end_date
                    )

                    result = pool.map(fetch_calc_data_partial, list(codes_partial))
                    df_temp = pd.concat(result).reset_index(drop=True)  # "df_raw" is here
                l_df.append(df_temp)
                time.sleep(1)
        else :
            l_df = []
            start_idx = 0
            get_price_done = False

            while not get_price_done:
                try :
                    for i in range(start_idx, LEN_OF_CODES):                    
                        l_df.append(
                            self.request_stock_price_in_specific_period(
                                codes_list[i],  start_date, end_date
                            )
                        )
                        if i%200 == 0 and i > 0:
                            print(f"{i}___sleeping")
                            time.sleep(5)
                        if i == LEN_OF_CODES -1 :
                            get_price_done = True
                except :
                    print("Error occured drugin get price")
                    start_idx = i
                    time.sleep(10)

        df_ = pd.concat(l_df)
        df_ = self.reform_df_price(df_)

        return df_

    def get_df_price_n_days_over_100days(self, codes_list, dates_list):
        """
        Get price and volume datframe of given stock code list

        Arg.
        codes_list (list of str), n_days (int), dates_list (str) : "YYYY-mm-dd"

        Returns.
        dataframe of price
        """
        LEN_OF_CODES = codes_list.__len__()

        itr_range = math.ceil(dates_list.__len__() / 100)

        l_query_dates = []
        for i in range(itr_range):
            list_seg = dates_list[i*100 : (i+1)*100 ]
            
            l_query_dates.append(
                (list_seg[-1], list_seg[0])
            )
        l_query_dates.sort(key = lambda x : x[0])

        l_df = []
        for dates in l_query_dates:
            start_date = dates[0]
            end_date = dates[-1]

            print(f"start : {start_date} / end : {end_date}")
            if IS_RUN_ON_DATABUTTON :
                N = 50
                RANGE = int(LEN_OF_CODES / N)
                # RANGE = 0

                for i in range(0, RANGE + 1):
                    codes_partial = codes_list[i * N : (i + 1) * N]
                    with mp.Pool(mp.cpu_count()) as pool:
                        fetch_calc_data_partial = partial(
                            # get_price_from_KIS
                            self.request_stock_price_in_specific_period,
                            start_date = start_date,
                            end_date = end_date
                        )

                        result = pool.map(fetch_calc_data_partial, list(codes_partial))
                        df_temp = pd.concat(result).reset_index(drop=True)  # "df_raw" is here
                    l_df.append(df_temp)
                    time.sleep(1)
            else :
            
                start_idx = 0
                get_price_done = False

                while not get_price_done:
                    try :
                        for i in range(start_idx, LEN_OF_CODES):                    
                            l_df.append(
                                self.request_stock_price_in_specific_period(
                                    codes_list[i],  start_date, end_date
                                )
                            )
                            if i%200 == 0 and i > 0:
                                print(f"{i}___sleeping")
                                time.sleep(5)
                            if i == LEN_OF_CODES -1 :
                                get_price_done = True
                    except :
                        print("Error occured drugin get price")
                        start_idx = i
                        time.sleep(10)

                time.sleep(10)

        df_ = pd.concat(l_df)
        df_ = self.reform_df_price(df_)

        return df_
    
    def reform_df_price(self, df_):
        """
        Reform Price DataFrame into Axolotls Format
        For KIS Price Data        
        """

        df_ = (
            df_.rename(
                columns={
                    "stck_bsop_date": "date",
                    "stck_oprc": "open",
                    "stck_hgpr": "high",
                    "stck_lwpr": "low",
                    "stck_clpr": "close",
                    # "prdy_ctrt": "change",
                    "acml_vol": "volume",
                }
            )
            .dropna()
            .drop_duplicates(subset=["date", "code"])
            .sort_values(by=["code", "date"], ascending=[True, True])
            .reset_index(drop=True)[
                ["date", "code", "open", "high", "low", "close", "volume"]
            ]
        )

        df_["open"] = df_["open"].astype(int)
        df_["high"] = df_["high"].astype(int)
        df_["low"] = df_["low"].astype(int)
        df_["close"] = df_["close"].astype(int)
        df_["volume"] = df_["volume"].astype(int)
        df_["date"] = df_["date"].apply(
            lambda date_str : date_str[:4] + '-' + date_str[4:6] + '-' + date_str[6:]
            )

        close_pivot = df_.pivot(index='date', columns='code', values='close')
        change_pivot = close_pivot.pct_change(1)

        change_stacked = (
            change_pivot
            .stack(level=-1, dropna=False)
            .reset_index()
            .rename(columns={0:"change"})
        )

        df_ = df_.merge(
            change_stacked,
            left_on = ['date', 'code'],
            right_on = ['date', 'code'],
            how='inner'
        )

        df_ = df_.dropna().reset_index(drop=True)        
        
        return df_