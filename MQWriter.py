import atexit

import pika, os, logging, json, signal
import dateutil.parser as dp

from S3CollectorMessagesManager import S3CollectorMessagesManager
from auditing.db import engine, session
from auditing.db.Models import Packet

if os.environ["ENVIRONMENT"] == "DEV":
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("s3transfer").setLevel(logging.WARNING)
else:
    logging.getLogger().setLevel(logging.INFO)

CollectorMessageManager = None


def save_messages(messages, data_collector_id, packet_id):
    global CollectorMessageManager
    if CollectorMessageManager:
        # Save the message/s
        for message in messages:
            # In case a packet was instantiated, relate it with the message
            # if not, packet_id will be null. That's ok but we still want the property in the json
            message['packet_id'] = packet_id
        CollectorMessageManager.save_collector_messages(data_collector_id, messages)

BATCH_LENGHT = 64
DATA_MAX_LEN = 300
WRITE_TIMEOUT = 10
write_queue = []


def timeout_writer(signum, frame):
    global write_queue
    if len(write_queue) != 0:
        engine.execute(Packet.__table__.insert(), write_queue)
        write_queue = []
        session.commit()


def callback(ch, method, properties, body):
    global write_queue
    try:
        message = body.decode("utf-8")
        # Parse the JSON into a dict
        data = json.loads(message)
        
        # This packet is a JSON object
        packet = data.get('packet')
        messages = data.get('messages')

        if packet:
            packet = dict(
                date=dp.parse(packet.get('date', None)),
                topic=packet.get('topic', None),
                data_collector_id=packet.get('data_collector_id', None),
                organization_id=packet.get('organization_id', None),
                gateway=packet.get('gateway', None),
                tmst=packet.get('tmst', None),
                chan=packet.get('chan', None),
                rfch=packet.get('rfch', None),
                freq=packet.get('freq', None),
                stat=packet.get('stat', None),
                modu=packet.get('modu', None),
                datr=packet.get('datr', None),
                codr=packet.get('codr', None),
                lsnr=packet.get('lsnr', None),
                rssi=packet.get('rssi', None),
                size=packet.get('size', None),
                data=packet['data'][0:DATA_MAX_LEN] if 'data' in packet else None,
                m_type=packet.get('m_type', None),
                major=packet.get('major', None),
                mic=packet.get('mic', None),
                join_eui=packet.get('join_eui', None),
                dev_eui=packet.get('dev_eui', None),
                dev_nonce=packet.get('dev_nonce', None),
                dev_addr=packet.get('dev_addr', None),
                adr=packet.get('adr', None),
                ack=packet.get('ack', None),
                adr_ack_req=packet.get('adr_ack_req', None),
                f_pending=packet.get('f_pending', None),
                class_b=packet.get('class_b', None),
                f_count=packet.get('f_count', None),
                f_opts=packet.get('f_opts', None),
                f_port=packet.get('f_port', None),
                error=packet['error'][0:DATA_MAX_LEN] if 'error' in packet else None,
                latitude=packet.get('latitude', None),
                longitude=packet.get('longitude', None),
                altitude=packet.get('altitude', None),
                app_name=packet.get('app_name', None),
                dev_name=packet.get('dev_name', None),
                gw_name=packet.get('gw_name', None)
                )
            write_queue.append(packet)
            signal.signal(signal.SIGALRM, timeout_writer)
            signal.alarm(WRITE_TIMEOUT)
            
        if len(write_queue) >= BATCH_LENGHT:
            signal.alarm(0)
            engine.execute(Packet.__table__.insert(), write_queue)
            write_queue = []
            session.commit()
        
        if messages and len(messages) > 0:
            save_messages(messages, messages[0].get('data_collector_id'), None)
    except Exception as e:
        logging.error(f"There was an error writing messages:\n{e}")
        write_queue = []
        session.rollback()

    try:
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error("There was an error ACK-ing the packet: %s. Exception: %s" % (body, e))


def exit_handler():
    logging.info('exit_handler: Flushing messages to s3')
    global CollectorMessageManager
    if CollectorMessageManager:
        CollectorMessageManager.flush_all()







try:
    print("Starting PacketWriter")
    atexit.register(exit_handler)
    print("Initializing s3 manager")
    if(
        'AWS_ACCESS_KEY_ID' in os.environ and len(os.environ['AWS_ACCESS_KEY_ID'])>0 and
        'AWS_SECRET_ACCESS_KEY' in os.environ and len(os.environ['AWS_SECRET_ACCESS_KEY'])>0
    ):
        CollectorMessageManager = S3CollectorMessagesManager(aws_access_key=os.environ["AWS_ACCESS_KEY_ID"],
                                            aws_secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                                            bucket_name=os.environ["AWS_COLLECTOR_MSGS_BUCKET"],
                                            logger=logging.getLogger())
    # else:
    #     CollectorMessageManager = LogCollectorMessagesManager(logger=logging.getLogger())


    print("Initializing rabbit connection")
    rabbit_credentials = pika.PlainCredentials(os.environ["RABBITMQ_DEFAULT_USER"], os.environ["RABBITMQ_DEFAULT_PASS"])
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ["RABBITMQ_HOST"],
                                  port=int(os.environ["RABBITMQ_PORT"]),
                                  credentials=rabbit_credentials)
    )
    channel = connection.channel()
    channel.queue_declare(queue='collectors_queue', durable=True)
    channel.exchange_declare(exchange=os.environ["ENVIRONMENT"], exchange_type='direct')
    channel.queue_bind(exchange=os.environ["ENVIRONMENT"], queue='collectors_queue')
    channel.basic_consume(queue='collectors_queue', on_message_callback=callback)
    logging.info("consuming messages on queue collectors_queue")
    channel.start_consuming()
except Exception as e:
    logging.error(f'There was an error initializing PacketWriter: {e}')
finally:
    logging.info('Flushing messages to s3')
    if CollectorMessageManager:
        CollectorMessageManager.flush_all()
    else:
        logging.info('No collector message manager available. Messages will not be saved')
