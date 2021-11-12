from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os
import logging
import time

if "ENVIRONMENT" not in os.environ:
    DB_HOST = "localhost"
    DB_NAME = "loraguard_db"
    DB_USERNAME = "postgres"
    DB_PASSWORD = "postgres" 
    DB_PORT = 5432
    os.environ["ENVIRONMENT"] = "DEV"
else:
    DB_HOST = os.environ["DB_HOST"] 
    DB_NAME = os.environ["DB_NAME"] 
    DB_USERNAME = os.environ["DB_USERNAME"] 
    DB_PASSWORD = os.environ["DB_PASSWORD"] 
    DB_PORT = os.environ["DB_PORT"] 

engine = create_engine('postgresql+psycopg2://{user}:{pw}@{url}:{port}/{db}'.format(user=DB_USERNAME, pw=DB_PASSWORD, url=DB_HOST, port= DB_PORT, db=DB_NAME))
Base = declarative_base()
sessionBuilder = sessionmaker()
sessionBuilder.configure(bind=engine)
session = sessionBuilder()

while True:
    try:
        engine.connect()
        break
    except Exception as exc:
        logging.warning("Couldn't connect with postgres. Retrying connection.")
        time.sleep(1)