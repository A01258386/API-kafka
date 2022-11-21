import datetime
import json
import logging
from webbrowser import get
from wsgiref import headers
import swagger_ui_bundle
import connexion
from connexion import NoContent
import requests
import yaml
import uuid
import logging.config
import datetime
from pykafka import KafkaClient
from pykafka.common import OffsetType


data = []
# timestamp
# timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# trace = str(uuid.uuid4)
KAFKA_CONNECTION_RETRY = 5 
with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')
# logging
# logging.basicConfig(filename='app.log',
#                     format="Received event drive request with a trace id of " + trace)

# logger = logging.getLogger('basicLogger')
# logger.setLevel(logging.DEBUG)


def driveEvent(body):

    trace = str(uuid.uuid4())
    body['date_created'] = str(
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    body['trace_id'] = trace
    headers = {'Content-Type': 'application/json'}
    print(body)
    client = KafkaClient(hosts='kafka.westus3.cloudapp.azure.com:9092')
    topic = client.topics[str.encode('events')]
    producer = topic.get_sync_producer()
    print(producer)
    msg = {"type":"drive",
    "datetime":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "payload":body}
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    # res = requests.post(app_config['eventstore2']['url'],
    #                     json=body, headers=headers)
    # # logging
    # # Log the receipt of the event request in a form :
    # # Received event <event name> request with a trace id of <trace_id>
    # logger.info("Received event drive request with a trace id of " + trace)

    # # Log the return of the event request in a form :
    # # Returned event <event name> response with a trace id of <trace_id>
    # logger.info("Returned event drive response with a trace id of " + trace)

    # return res.text, res.status_code
    return msg, 201


def flyEvent(body):
    headers = {'Content-Type': 'application/json'}
    # create uuid
    trace = str(uuid.uuid4())
    body['trace_id'] = trace
    body['date_created'] = str(
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # print(body)
    client = KafkaClient(hosts='kafka.westus3.cloudapp.azure.com:9092')
    topic = client.topics[str.encode('events')]
    producer = topic.get_sync_producer()
    print(producer)
    msg = {"type":"fly",
    "datetime":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "payload":body}
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    # res = requests.post(app_config['eventstore1']['url'],
    #                     json=body, headers=headers)

    # # logging
    # # received event
    # logger.info("Received event drive request with a trace id of " + trace)
    # # Returned event
    # logger.info("Returned event drive response with a trace id of " + trace)

    # return res.text, res.status_code
    return msg, 201
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

app = connexion.App(__name__, specification_dir='')
app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)


if __name__ == '__main__':
    kafka_connection_retry()
    app.run(port=8080, debug=True)
