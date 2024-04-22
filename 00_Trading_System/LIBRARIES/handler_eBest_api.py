IS_RUN_ON_DATABUTTON = False

import json
import requests
import pandas as pd
import time

if IS_RUN_ON_DATABUTTON:
    import databutton as db

else :
    from dotenv import load_dotenv
    import os

class EBestApiHandler:

    def __init__(self):

        if IS_RUN_ON_DATABUTTON:
            self.key = db.secrets.get("eBEST_APP_KEY") 
            self.secret = db.secrets.get("eBEST_SECRET") 

        else :         
            load_dotenv()
            self.key = os.getenv('eBEST_APP_KEY')
            self.secret = os.getenv('eBEST_SECRET')

        self.TOKEN = self.get_ebest_access_token()

    def get_ebest_access_token(self):
        """
        Get eBEST API Acess Token

        Arg. : None
        Returns : token string
        """
        try:
            if IS_RUN_ON_DATABUTTON:
                resp_data = db.storage.json.get("eBEST_TOKEN")
            else :
                with open('./STORAGE/eBEST_TOKEN.json') as f:
                    resp_data = json.load(f)

        except Exception as Err:
            print(f"TOKEN ISSUE ERR eBest: {Err}")
            
            base_url = "https://openapi.ebestsec.co.kr:8080"
            path = "/oauth2/token"
            url = f"{base_url}{path}"

            headers = {"content-type": "application/x-www-form-urlencoded"}

            data = {
                "grant_type": "client_credentials",
                "appkey": self.key,
                "appsecretkey": self.secret,
                "scope": "oob",
            }

            resp = requests.post(url, headers=headers, data=data)
            resp_data = resp.json()

            if IS_RUN_ON_DATABUTTON:
                db.storage.json.put("eBEST_TOKEN", resp_data)
            else :
                with open('./STORAGE/eBEST_TOKEN.json', 'w') as f:
                    json.dump(resp_data, f)

        return f"Bearer {resp_data['access_token']}"
    
    def get_order_placed_results(self):
        """
        Check the order processing results
        of the day.

        Args.:
        None

        Returns:
        check_result (dict)
        """
        base_url = "https://openapi.ebestsec.co.kr:8080"
        path = "/stock/accno"
        url = f"{base_url}{path}"

        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": self.TOKEN,
            "tr_cd": "t0425",
            "tr_cont": "N",
            # "tr_cont_key" :
        }

        data = {
            "t0425InBlock": {
                # "expcode": "005930",
                "chegb": "0",
                "medosu": "0",
                "sortgb": "2",
                "cts_ordno": " ",
            }
        }

        resp = requests.post(url, headers=headers, data=json.dumps(data))
        res = resp.json()
        return res
    
    def current_stock_price_request(self, nrec, codes_str):
        """
        Request for getting current prices of stocks
        Max 50 stock at once.

        Args.
        nrec (int) : number of stocks requested
        codes_str (str) : continuously added stock codes (ex. 000020000030 ...)

        Returns.
        dataframe : price dataframe
        """
        base_url = "https://openapi.ebestsec.co.kr:8080"
        path = "/stock/market-data"
        url = f"{base_url}{path}"

        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": self.TOKEN,
            "tr_cd": "t8407",
            "tr_cont": "N",
        }

        data = {"t8407InBlock": {"nrec": nrec, "shcode": codes_str}}

        resp = requests.post(url, headers=headers, data=json.dumps(data))
        res = resp.json()

        df_ = pd.DataFrame(res["t8407OutBlock1"])

        return df_

    def get_current_stock_price(self, codes_list, today):
        """
        Get dataframe of current prices of the given stock in the list

        Args.
        codes_list (list of str) : The List of stock codes
        today (str) : the date of today YYYY-mm-dd

        Returns.
        price dataframe
        """
        num_of_codes = len(codes_list)
        l_df = []
        for i in range(0, num_of_codes, 50):
            partial_codes = codes_list[i : i + 50]
            len_partial_codes = len(partial_codes)
            codes_str = "".join(partial_codes)

            df_ = self.current_stock_price_request(len_partial_codes, codes_str)
            l_df.append(df_)

            time.sleep(0.6)

        df_ = pd.concat(l_df)
        df_ = self.reform_df_price(df_, today)

        return df_
    
    def reform_df_price(self, df_, today):
        df_ = (
            df_
            .drop_duplicates(subset=['shcode'])
            .assign(date=today)
            .rename(
                columns={
                    "shcode": "code",
                    "price": "close",
                    "diff": "change",
                    "change": "change_money",
                    "hname": "name",
                }
            )[
                [
                    "date",
                    "code",
                    "open",
                    "high",
                    "low",
                    "close",
                    "change",
                    "volume",
                    "name",
                ]
            ]
        )

        df_["open"] = df_["open"].astype(int)
        df_["high"] = df_["high"].astype(int)
        df_["low"] = df_["low"].astype(int)
        df_["close"] = df_["close"].astype(int)
        df_["volume"] = df_["volume"].astype(int)
        df_["change"] = df_["change"].astype(float) / 100

        return df_
    
    def request_order(self, code, qty, BnsTpCode):
        """
        Placing Order Request

        Args.
        code (str) : stock code
        qty (int) : number of stocks
        BnsTpCode (str) : sell - '1' / buy - '2'

        Returns.
        res (dict) : json response for the request
        """

        base_url = "https://openapi.ebestsec.co.kr:8080"
        path = "/stock/order"
        url = f"{base_url}{path}"

        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": self.TOKEN,
            "tr_cd": "CSPAT00601",
            "tr_cont": "N",
            # "tr_cont_key" :
        }

        data = {
            "CSPAT00601InBlock1": {
                "IsuNo": code,
                "OrdQty": qty,
                "OrdPrc": 0,
                "BnsTpCode": BnsTpCode,
                "OrdprcPtnCode": "03",  # 시장가
                "MgntrnCode": "000",
                "LoanDt": "",
                "OrdCndiTpCode": "0",
            }
        }

        resp = requests.post(url, headers=headers, data=json.dumps(data))
        res = resp.json()

        return res
    
    def place_order_on_ebest(self, df_ord):
        """
        Placing Orders

        Args.
        df_ord (dataframe) : offset orders dataframe

        Returns.
        df_ord_updated (dataframe) : dataframe order result updated (order no.)
        """

        l_df_ord_result_updated = []
        for _, row in df_ord.iterrows():

            qty = int(row["ord_qty"])
            code = row["stock_code"]
            ord_type = row["ord_type"]
            name = row["stock_name"]

            if ord_type == 'buy':
                BnsTpCode = '2'
            elif ord_type == 'sell':
                BnsTpCode = '1'
            else :
                l_df_ord_result_updated.append(
                    row.to_frame().T
                )  
                # In this case,
                # rjct_qty processed when offset order created
                continue
            
            order_done = False
            n_iter = 0
            while not order_done and n_iter < 3:
                order_rsp = self.request_order(code, qty, BnsTpCode)
                print(order_rsp)

                if order_rsp["rsp_cd"] in ["00039", "00040"]:
                    row['ebest_ord_no'] = order_rsp["CSPAT00601OutBlock2"]["OrdNo"]
                    row['status'] = "wip"
                    order_done = True

                n_iter += 1
                time.sleep(0.5)

            if not order_done:
                print(f"{name}_종목_주문_실패")
                row['ebest_ord_no'] = "-"
                row['rjct_qty'] = row['ord_qty']
                row['status'] = "ordering failed"
               
            else:
                print(f"{name}_종목_주문_성공")      

            l_df_ord_result_updated.append(
                row.to_frame().T
            )        

            time.sleep(1)
        
        return pd.concat(l_df_ord_result_updated)
