import os
import logging
import requests
import numpy as np
import pandas as pd
from credentials import *


logging.basicConfig(level=logging.INFO, filename=os.path.join(os.getcwd(), "crime", "logs", "status"), filemode="w", format="%(asctime)s - %(levelname)s - %(message)s")


def extract():
  """
  Extracts incidents of crime data in the City of Los Angeles dating back to 2020

  https://catalog.data.gov/dataset/crime-data-from-2020-to-present

  """
  URL = "https://data.lacity.org/api/views/2nrs-mtv8/rows.json?accessType=DOWNLOAD"

  try:
    response = requests.get(URL, timeout=(2, 10))
    
    if response.status_code == requests.codes.ok:
      logging.info("data extraction completed successfully")
  
  except Exception as err:
    logging.exception("error encountered", exc_info=err)

  else:
    response.raise_for_status()

  json_data = response.json()

  return json_data


def transform(json_data: dict) -> pd.DataFrame:
  """
  Transforms json data to Pandas DataFrame

  """
  column_list = np.arange(len(json_data["meta"]["view"]["columns"]))
  column_names = [json_data["meta"]["view"]["columns"][idx]["name"] for idx in column_list][8:]
  
  df_pd = pd.DataFrame(json_data["data"]).iloc[:, 8:]
  df_pd.columns = column_names
  df_pd.columns = [col.replace(" ", "_") for col in df_pd.columns]
  
  df_trf = df_pd.astype({"Date_Rptd": "datetime64[ns]", "DATE_OCC": "datetime64[ns]"})

  logging.info("data transformation completed successfully")

  return df_trf


def load(df_trf: pd.DataFrame):
  """
  Load transformed data to AWS S3 Bucket

  """
  s3_bucket_name = "datagov-01"
  file_name = "crime.parquet"
  aws_credentials = {"key": AWS_ACCESS_KEY_ID, "secret": AWS_SECRET_ACCESS_KEY}

  return df_trf.to_parquet(f"s3://{s3_bucket_name}/crime-data/{file_name}", compression="gzip", storage_options=aws_credentials)


if __name__ == "__main__":
  raw_data = extract()
  transformed_data = transform(raw_data)
  load(transformed_data)
  logging.info("ETL process completed successfully")