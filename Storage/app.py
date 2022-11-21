import logging
import connexion
from connexion import NoContent
import json
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from fly import Fly
from drive import Drive
import datetime
import connexion
from connexion import NoContent
import yaml
import logging.config

with open('app_conf.yml', 'r') as f:
    storage_config = yaml.safe_load(f.read())
STORAGE_SETTING = storage_config['datastore']
# print(STORAGE_SETTING)
KAFKA_CONNECTION_RETRY = 5 

# print(
# f"mysql+pymysql://{STORAGE_SETTING['user']}:{STORAGE_SETTING['password']}@{STORAGE_SETTING['hostname']}:{STORAGE_SETTING['port']}/{STORAGE_SETTING['db']}")
# engine = create_engine(
#     f"mysql+pymysql://{STORAGE_SETTING['user']}:{STORAGE_SETTING['password']}@{STORAGE_SETTING['hostname']}:{STORAGE_SETTING['port']}/{STORAGE_SETTING['db']}")
# Base.metadata.bind = engine

# DBSession = sessionmaker(bind=engine)
# session = DBSession()
DB_ENGINE = create_engine(
    f"mysql+pymysql://{STORAGE_SETTING['user']}:{STORAGE_SETTING['password']}@{STORAGE_SETTING['hostname']}:{STORAGE_SETTING['port']}/{STORAGE_SETTING['db']}")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

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


def driveEvent(body):
    """ Receives a drive reading """
    print(body)
    session = DB_SESSION()
    bp = Drive(body['id'],
               body['speed'],
               body['timestamp'],
               body['lat'],
               body['long'],
               body['date_created'],
               body['trace_id'])

    session.add(bp)

    session.commit()
    session.close()
    logger.info(
        f'drive database : trace_id: {body["trace_id"]} write to database-> flve, table->drive')
    print(bp)

    return NoContent, 201


def flyEvent(body):
    print(body)
    """ Receives a fly reading """
    session = DB_SESSION()
    hr = Fly(body['id'],
             body['altitute'],
             body['air_pressure'],
             body['city'],
             body['weight'],
             body['date_created'],
             body['trace_id'])

    session.add(hr)

    session.commit()
    session.close()

    logger.info(
        f'fly database : trace_id: {body["trace_id"]} write to database-> flve, table->fly')

    return NoContent, 201


def get_drive_stats(starttime, endtime):
    """ Gets drive readings after the timestamp """
    session = DB_SESSION()
    start_time = datetime.datetime.strptime(
        starttime, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.datetime.strptime(
        endtime, "%Y-%m-%d %H:%M:%S")
    readings = session.query(Drive).filter(
        Drive.date_created >= start_time, Drive.date_created <= end_time).all()
    'select * from drive where date_created >= start_time and date_created <= end_time'
    results_list = []
    for reading in readings:
        results_list.append(reading.to_dict())
    session.close()
    logger.info("Query for drive event readings after %s and before %s returns %d results" % (
        str(start_time),str(end_time) , len(results_list)))
    return results_list, 200


def get_fly_stats(starttime, endtime):
    """ Gets fly readings after the timestamp """
    session = DB_SESSION()
    start_time = datetime.datetime.strptime(
        starttime, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.datetime.strptime(
        endtime, "%Y-%m-%d %H:%M:%S")

    readings = session.query(Fly).filter(
        Fly.date_created >= start_time, Fly.date_created <= end_time).all()
    results_list = []
    for reading in readings:
        results_list.append(reading.to_dict())
    session.close()
    logger.info("Query for fly event readings after %s and before %s returns %d results" % (
    str(start_time),str(end_time) , len(results_list)))

    return results_list, 200

def process_messages():
    """ Process event messages """
    hostname = "%s:%d" % (app_config["events"]["hostname"],
    app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    # Create a consume on a consumer group, that only reads new messages
    # (uncommitted messages) when the service re-starts (i.e., it doesn't
    # read all the old messages from the history in the message queue).
    consumer = topic.get_simple_consumer(consumer_group=b'event_group',
    reset_offset_on_start=False,
    auto_offset_reset=OffsetType.LATEST)
    # This is blocking - it will wait for a new message
    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)
        payload = msg["payload"]
        print(msg)
        try:
            if msg["type"] == "drive": # Change this to your event type
                # Store the event1 (i.e., the payload) to the DB
                driveEvent(payload)

            elif msg["type"] == "fly": # Change this to your event type
                # Store the event2 (i.e., the payload) to the DB
                flyEvent(payload)
        except:
            print('duplicate entry, ignored')
        # Commit the new message as being read
        consumer.commit_offsets()
def kafka_connection_retry():
    hostname = "%s:%d" % (app_config["events"]["hostname"],app_config["events"]["port"])
    current_retry = 0 # for retrying kafka connection
    while current_retry < KAFKA_CONNECTION_RETRY:
        print('trying to connect to kafka')
        try:
            client = KafkaClient(hosts=hostname)
            topic = client.topics[app_config["events"]["topic"]]
            consumer = topic.get_simple_consumer(consumer_group=b'event_group',
                auto_offset_reset=OffsetType.LATEST,
                reset_offset_on_start=True,
                consumer_timeout_ms=100)
            break
        except Exception as e:
            logger.error("Error connecting to kafka %s" % e)
            current_retry += 1
    if current_retry == KAFKA_CONNECTION_RETRY:
        logger.error("Failed to connect to kafka")
        exit(1)
    else:
        logger.info("Connected to kafka !!!")

def health():
    return {"status": "ok"}, 200
    
app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)


if __name__ == "__main__":
    kafka_connection_retry()
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    app.run(port=8090, debug=True)
