import logging
import pysurveycto
import pandas as pd 
from io import StringIO
from datetime import datetime
import pytz

import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient



def extract(server_name, username, password):
    scto = pysurveycto.SurveyCTOObject(server_name, username, password)
    data = scto.get_form_data("retailer_survey_chemtex_eth", format = 'csv')
    df = pd.read_csv(StringIO(data))
    return df

def load(df, account_name, account_key):
 
    datalake_service_client = DataLakeServiceClient("https://{}.dfs.core.windows.net".format(account_name), credential=account_key)

    filesystem_name = "data/raw"

    file_path = datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y/%m/%d")

    file_client = datalake_service_client.get_file_client(filesystem_name, file_path)

    raw_df = df.to_csv(index=False)

    file_client.upload_data(data=raw_df,overwrite=True, length=len(raw_df))

    file_client.flush_data(len(raw_df))

    logging.info("Updated data")

def main(mytimer: func.TimerRequest) -> None:

    if mytimer.past_due:
        logging.info('The timer is past due!')

    #TODO: This should be set in the secret 
    server_name = "ifcafrica"
    username = "squiroga@ifc.org"
    password = "IFCMAS2021!"
    account_name='ctogen2store'
    account_key='1H4OGV4IhzJ/NFGKdvvNGPIAP0oLD3KRaZ5j5YE+srIuIWpD0I4VX7sSzGPpfHDuet31ElmdV7kR+AStoea43g=='

    df = extract(server_name, username, password)

    load(df, account_name, account_key)


