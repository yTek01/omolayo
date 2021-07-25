import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker

import requests 
import json
from datetime import datetime 
import datetime 
import sqlite3  

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = ""
TOKEN = ""


def check_if_valid_data(df: pd.DataFrame) => bool:
    
    if df.empty:
        print("No song downloaded . Finishing Execution")
        
        return False
    
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key chech is Violated")
    
    
    if df.isnull().values.any():
        raise Exception("Null Values found") 
    
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    timestamps = df['timestamp'].tolist()
    
    for timestamp in timestamps:
        
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception('At least on of the returned does not have a yesterdays timestamp')
        
    return True

if __name__ == "__main__" :
    
    
    header = {
        "Accept" : "application/json", 
        "Content-Type": "application/json", 
        "Authorization": "Bearer {token}".format(token=TOKEN)    
    }
    
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    
