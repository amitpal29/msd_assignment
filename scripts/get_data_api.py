import requests
import os
import json
import datetime
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
from urllib.parse import quote_plus
import argparse


def save_text_response(api_url,data_dir,batch_dt):
    try:
        # Step 1: Create the full file path
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        folder_path = os.path.join(data_dir, batch_dt)
        print(folder_path)
        os.makedirs(folder_path, exist_ok=True)

        # Step 2: Make a GET request to the API
        batch_date = datetime.strptime(batch_dt, "%Y-%m-%d")
        previous_month = (batch_date - relativedelta(months=1)).strftime("%Y-%m")
        print(previous_month)
        api_url_with_date = f"{api_url}&yearMonth={previous_month}"
        print(api_url_with_date)
        response = requests.get(api_url_with_date)
        response.raise_for_status()  # Raise an exception for bad status codes

        json_filename = 'currency_rates_' + previous_month + '.json'
        file_path = os.path.join(folder_path, json_filename)


            # Step 3: Open the file in write mode and save the response text
        with open(file_path, 'w', encoding='utf-8') as f:
                f.write(response.text)

        print(f"Response successfully saved to '{file_path}'")
        return folder_path

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return False
    except IOError as e:
        print(f"Error saving file: {e}")
        return False


# Load the JSON data
def load_data_to_table(folder_path, db_user, db_password, db_host, db_name):
    # Step 1: Read the JSON file content
    dfs = []
    json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
    for json_filename in json_files:
        with open(os.path.join(folder_path, json_filename), 'r') as f:
            json_string = f.read()
            data = json.loads(json_string)
            df = pd.DataFrame(data['rates'])
            # print(df)
            dfs.append(df)
    final_df = pd.concat(dfs, ignore_index=True)
   
    # Database connection and DataFrame to SQL table

    CONNECTION_STRING = 'mysql+pymysql://{0}:{1}@{2}:3306/{3}'.format(db_user, db_password, db_host, db_name)
    print(CONNECTION_STRING)
    engine = create_engine(CONNECTION_STRING)

    today = date.today()
    final_df["load_date"] = today.strftime("%Y-%m-%d")


    # Step 2: Write the DataFrame to the database
    final_df.to_sql('stage_fx_rates', con=engine, if_exists='append', index=False)

    # Print the DataFrame to display the tabular data
    print("Successfully wrote DataFrame to table stage_fx_rates")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script load data in raw table from API"
    )
    parser.add_argument("--DB_USER", required=True, type=str)
    parser.add_argument("--DB_PASSWORD", required=True, type=str)
    parser.add_argument("--DB_HOST", required=True, type=str)
    parser.add_argument("--DB_NAME", required=True, type=str)
    parser.add_argument("--api_url", required=True, type=str)
    parser.add_argument("--data_dir", required=True, type=str)
    parser.add_argument("--batch_dt", required=True, type=str)
    args = parser.parse_args()

    db_user = args.DB_USER
    db_password = args.DB_PASSWORD
    db_host = args.DB_HOST
    db_name = args.DB_NAME
    api_url = args.api_url
    data_dir = args.data_dir
    batch_dt = args.batch_dt

folder_path = save_text_response(api_url, data_dir,batch_dt)
load_data_to_table(folder_path, db_user, db_password, db_host, db_name)
