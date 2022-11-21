import imp
import logging
import uuid
import connexion
from connexion import NoContent
import json
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import datetime
import mysql.connector
import pymysql
import swagger_ui_bundle
import connexion
from connexion import NoContent
import requests
import yaml
import uuid
import logging.config
from flask_cors import CORS, cross_origin

with open('app_conf.yml', 'r') as f:
    storage_config = yaml.safe_load(f.read())
STORAGE_SETTING = storage_config['datastore']
# print(STORAGE_SETTING)


# # print(
# # f"mysql+pymysql://{STORAGE_SETTING['user']}:{STORAGE_SETTING['password']}@{STORAGE_SETTING['hostname']}:{STORAGE_SETTING['port']}/{STORAGE_SETTING['db']}")
# engine = create_engine(
#     f"mysql+pymysql://{STORAGE_SETTING['user']}:{STORAGE_SETTING['password']}@{STORAGE_SETTING['hostname']}:{STORAGE_SETTING['port']}/{STORAGE_SETTING['db']}")
# Base.metadata.bind = engine

# DBSession = sessionmaker(bind=engine)
# session = DBSession()
# DB_ENGINE = create_engine(
#     f"mysql+pymysql://{STORAGE_SETTING['user']}:{STORAGE_SETTING['password']}@{STORAGE_SETTING['hostname']}:{STORAGE_SETTING['port']}/{STORAGE_SETTING['db']}")
# Base.metadata.bind = DB_ENGINE
# DB_SESSION = sessionmaker(bind=DB_ENGINE)

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

#Add an INFO log message to your Storage Service that displays the hostname and port of your
# MySQL database. This will help you verify that your Storage Service is connecting to the
# correct database.
logger.info(f"Storage Service connected to MySQL on hostname:{STORAGE_SETTING['hostname']} and port:{STORAGE_SETTING['port']}")

# def add_to_drive_database(body, table_name):
#     session = DB_SESSION()
#     print(body)
#     print(f'adding {table_name} data to table {table_name}')
#     drive = Drive(body['speed'],
#                   body['timestamp'],
#                   body['lat'],
#                   body['long'],
#                   body['date_created'],
#                   body['trace_id'])
#     print(drive.to_dict())
#     session.add(drive)

#     session.commit()
#     session.close()


# def add_to_fly_database(body, table_name):
#     session = DB_SESSION()

#     print(f'adding {table_name} data to table {table_name}')
#     fly = Fly(body['altitute'],
#               body['air_pressure'],
#               body['city'],
#               body['weight'],
#               body['date_created'],
#               body['trace_id'])

#     session.add(fly)

#     session.commit()
#     session.close()


def getAuditDrive(index):
    """ Receives a drive reading """
    hostname = "%s:%d" % (app_config["events"]["hostname"],
    app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    message_dict = {}
    start = 1 
    # Here we reset the offset on start so that we retrieve
    # messages at the beginning of the message queue.
    # To prevent the for loop from blocking, we set the timeout to
    # 100ms. There is a risk that this loop never stops if the
    # index is large and messages are constantly being received!
    consumer = topic.get_simple_consumer(reset_offset_on_start=True,
    consumer_timeout_ms=1000)
    logger.info("Retrieving BP at index %d" % index)
    try:
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)
            if msg['type'] == 'drive':
                message_dict[start] = msg
                start += 1
            # Find the event at the index you want and
            # return code 200
            # i.e., return event, 200
        return message_dict[index], 200
    except:
        logger.error("No more messages found")
    logger.error("Could not find BP at index %d" % index)
    return { "message": "Not Found"}, 404


def getAuditFly(index):
    hostname = "%s:%d" % (app_config["events"]["hostname"],
    app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    message_dict = {}
    start = 1 
    # Here we reset the offset on start so that we retrieve
    # messages at the beginning of the message queue.
    # To prevent the for loop from blocking, we set the timeout to
    # 100ms. There is a risk that this loop never stops if the
    # index is large and messages are constantly being received!
    consumer = topic.get_simple_consumer(reset_offset_on_start=True,
    consumer_timeout_ms=1000)
    logger.info("Retrieving BP at index %d" % index)
    try:
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)
            if msg['type'] == 'fly':
                message_dict[start] = msg
                start += 1
            # Find the event at the index you want and
            # return code 200
            # i.e., return event, 200
        return message_dict[index], 200
    except:
        logger.error("No more messages found")
    logger.error("Could not find BP at index %d" % index)
    return { "message": "Not Found"}, 404

def health():
    return {"status": "ok"}, 200

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content-Type'

if __name__ == "__main__":

    app.run(port=8110, debug=True)
