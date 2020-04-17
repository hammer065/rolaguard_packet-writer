from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os

# ---> Comment these lines
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
# If you'd like to use sqlite <---


# Uncomment these lines if you want to work with sqlite instead of postgres
# engine = create_engine('sqlite:///orm_in_detail.sqlite')
# os.environ["ENVIRONMENT"] = "DEV"

Base = declarative_base()
sessionBuilder = sessionmaker()
sessionBuilder.configure(bind=engine)
session = sessionBuilder()

from auditing.db.Models import AlertType, RowProcessed, rollback
import logging

if os.environ["ENVIRONMENT"] == "DEV":
    logging.getLogger().setLevel(logging.DEBUG)
else:
    logging.getLogger().setLevel(logging.INFO)

try:
    if AlertType.count() == 0:
        AlertType(code= 'LAF-001', name= 'DevNonce repeated', risk= 'LOW', description= 'DevNonces for each device should be random enough to not collide. This may be due to join message replay.').save()
        AlertType(code= 'LAF-002', name= 'DevEUIs sharing the same DevAddr', risk= 'INFO', description= 'Two different devices might be using the same DevAddr.').save()
        AlertType(code= 'LAF-003', name= 'Join replay', risk= 'MEDIUM', description= 'A duplicated JoinRequest message was detected. The LoRaWAN network may be under a join replay attack.').save()
        AlertType(code= 'LAF-004', name= 'Uplink data packets replay', risk= 'MEDIUM', description= 'A duplicated uplink packet was detected.  The LoRaWAN network may be under a replay attack.').save()
        AlertType(code= 'LAF-005', name= 'Downlink data packets replay', risk= 'HIGH', description= 'A duplicated downlink packet was detected.  The server is responding to a replay attack or is generating an atypical traffic to devices.').save()
        AlertType(code= 'LAF-006', name= 'Counter reset and same DevAddr', risk= 'HIGH', description= "If the counter was reset (came back to 0) and the DevAddr is kept the same, this may imply that the device/server aren't regenerating the session keys. It could be also that session keys are regenerated but the server assigns the same DevAddr to the device (false positive).").save() 
        AlertType(code= 'LAF-007', name= 'Received smaller counter for DevAddr  (distinct from 0)', risk= 'Info', description= "This may doesn't represent an attack, but it's something not likely to happen. It may be that a device is malfunctioning.").save() 
        AlertType(code= 'LAF-009', name= 'Password cracked', risk= 'HIGH', description= 'It was possible to decrypt Join (Request and Accept) messages using a either a known or guessed AppKey.').save() 
        AlertType(code= 'LAF-010', name= 'Gateway changed location', risk= 'MEDIUM', description= 'If the gateway is not supposed to change its location, it may have been stolen.').save()

    if RowProcessed.count() == 0:
        RowProcessed(last_row= 0, analyzer= 'bruteforcer').save_and_flush()
        RowProcessed(last_row= 0, analyzer= 'packet_analyzer').save_and_flush()
        RowProcessed(last_row= 0, analyzer= 'printer').save_and_flush()

except Exception as exc:
    logging.error('Error at commit when initializing:', exc)
    logging.info('Rolling back the session')
    rollback()