import os
import logging
import requests
import pandas as pd
from credentials import *
from credentials import open_weather_api_key


logging.basicConfig(level=logging.INFO, filename=os.path.join(os.getcwd(), "logs", "daily"), filemode="w", format="%(asctime)s - %(levelname)s - %(message)s")


def extract(lat: float, lon: float, exclude: str, appid: str):
      """
      Extracts Current and Forecasts weather data For A Specified Location

      https://openweathermap.org/api/one-call-3#current

      Parameters
      ----------
      lat: float, (-90; 90)
        Latitude 

      lon: float, (-180; 180) 
        Longitude

      exclude: str
        exclude some parts of the weather data from the API response

      appid: str
        unique API key

      Returns:
        Json response

      """
      URL = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={exclude}&appid={appid}"

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


def transform(response: dict) -> pd.DataFrame:
      """
      Transforms JSON to Pandas DataFrame

      Parameters
      ----------
      response: dict
        JSON response 

      Returns:
        Pandas DataFrame

      """
      df_a = pd.json_normalize(response["daily"])[["dt", "pressure", "humidity", "wind_speed", "wind_deg", "clouds", "pop", "uvi", "temp.day", "summary"]]\
               .rename(columns={"dt": "date", "temp.day": "temp_day"})

      df_b = pd.json_normalize(response["daily"], record_path="weather").drop(columns=["id", "icon"])\
               .rename(columns={"main": "weather_group", "description": "weather_description"})

      df_trf = df_a.merge(df_b, left_index=True, right_index=True)

      df_trf["date"] = pd.to_datetime(df_trf["date"], unit="s")

      logging.info("data transformation completed successfully")

      return df_trf


def load(df_trf: pd.DataFrame):
      """
      Write a DataFrame to the binary parquet format

      Parameters
      ----------
      df_trf: pd.DataFrame
        Transformed data

      Returns:
        None or str
        If path_or_buf is None, returns the resulting csv format as a string. Otherwise returns None.

      """
      return df_trf.to_csv(os.path.join("outputs", "daily", f'{pd.Timestamp.now().strftime("%Y-%m-%d")}_Germany.csv'), index=False)

if __name__ == "__main__":
    response = extract(51.1657, 10.4515, "alerts", open_weather_api_key)
    transformed_data = transform(response)
    load(transformed_data)
    logging.info("ETL process completed successfully")