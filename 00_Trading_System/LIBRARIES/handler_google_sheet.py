import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json

IS_RUN_ON_DATABUTTON = False

if IS_RUN_ON_DATABUTTON:
    import databutton as db

else :
    from dotenv import load_dotenv
    import os

# %%
class GoogleSheetHandler:

    def __init__(self):
        """
        A Class to handle Google Spread Sheet

        Methods:

        get_df_from_sheet() : create dataframe from the sheet

        replace_sheet_with_df() : replace whole contents of the
                                  sheet with new dataframe
        
        """

        if IS_RUN_ON_DATABUTTON:            
            self.SHEET_ID = db.secrets.get("google_sheet_id")  # 
            self.json_key = db.storage.json.get(key="dots-stock-b4b1d559c811-json")

        else :         
            load_dotenv()
            
            self.SHEET_ID = os.getenv('google_sheet_id')
            with open("./STORAGE/dots-stock-b4b1d559c811.json") as f:
                self.json_key = json.load(f)

        
        # self.SHEET_ID = sheet_id
        self.credentials = service_account.Credentials.from_service_account_info(
            self.json_key,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
            ],
        )
        

    def get_df_from_sheet(self, sheet_name:str):
        """
        create dataframe from google spreadsheet

        Args. :
        sheet_name (str) : The name of sheet

        Returns:
        dataframe : The dataframe with the contents of the sheet

        """
        try:
            service = build("sheets", "v4", credentials=self.credentials)
            sheet = service.spreadsheets()
            result = (
                sheet.values()
                .get(spreadsheetId=self.SHEET_ID, range=f"{sheet_name}!A:ZZ")
                .execute()
            )

            df_ = pd.DataFrame(
                columns=result["values"][0],
                data=result["values"][1:],
            )

            return df_
        
        except Exception as error:
            print(f"get df from '{sheet_name}' failed : {error}")

    def replace_sheet_with_dataframe(self, sheet_name, df_):
        """
        Replace whole contents with given dataframe
        Previous contents are deleted

        Arg.:
        sheet_name (str) : The name of target sheet
        df_ (dataframe) : The dataframe with contents

        Returns:
        N/A
        """

        self.clear_sheet(sheet_name)
        
        try:
            service = build("sheets", "v4", credentials=self.credentials)
            response = (
                service.spreadsheets()
                .values()
                .update(
                    spreadsheetId=self.SHEET_ID,
                    valueInputOption="RAW",
                    range=f"{sheet_name}!A:ZZ",
                    body=dict(
                        majorDimension="ROWS",
                        values=df_.T.reset_index().T.values.tolist(),
                    ),
                )
                .execute()
            )
            print("Sheet successfully Updated")
            return True
        except Exception as error:
            print(f"gsheet writing error : {error}")
            return False

    def clear_sheet(self, sheet_name):
        """
        Clear all contents in the sheet

        Args.
        sheet_name (str) : The name of target sheet
        """

        try:
            service = build("sheets", "v4", credentials=self.credentials)

            clear_sheet = (
                service.spreadsheets()
                .values()
                .clear(
                    spreadsheetId=self.SHEET_ID,
                    range=f"{sheet_name}!A:ZZ",
                    body={},
                )
                .execute()
            )

            print("Sheet successfully Cleared")

        except Exception as error:
            print(f"gsheet clearing error : {error}")

    def append_rows_to_sheet(self, sheet_name, df_):
        """
        Add rows to the sheed with given dataframe

        Arg.:
        sheet_name (str) : The name of target sheet
        df_ (dataframe) : The dataframe with contents

        Returns:
        N/A
        """
        try:
            service = build("sheets", "v4", credentials=self.credentials)
            response_date = (
                service.spreadsheets()
                .values()
                .append(
                    spreadsheetId=self.SHEET_ID,
                    valueInputOption="RAW",
                    range=f"{sheet_name}!A:ZZ",
                    body=dict(
                        majorDimension="ROWS",
                        values=df_.values.tolist(),
                    ),
                )
                .execute()
            )
            print("Rows Successfully Added")
        except Exception as error:
            print(f"Failed to add rows to {sheet_name}")

        