import atexit

import boto3
import pika, os, logging, gzip, io

from S3CollectorMessagesManager import S3CollectorMessagesManager
from LogCollectorMessagesManager import LogCollectorMessagesManager
from auditing.db.Models import commit, rollback, Packet
import datetime, json
import dateutil.parser as dp

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
    logging.debug("Saving messages to s3")
    global CollectorMessageManager
    if CollectorMessageManager:
        # Save the message/s
        for message in messages:
            # In case a packet was instantiated, relate it with the message
            # if not, packet_id will be null. That's ok but we still want the property in the json
            message['packet_id'] = packet_id
        CollectorMessageManager.save_collector_messages(data_collector_id, messages)
    else:
        logging.info('No collector message manager available. Messages will not be saved')


def save_packet(packet_dict):
    logging.debug("Parsing packet")
    # If a packet was received, persist it
    new_packet = Packet(
        date=dp.parse(packet_dict.get('date', None)),
        topic=packet_dict.get('topic', None),
        data_collector_id=packet_dict.get('data_collector_id', None),
        organization_id=packet_dict.get('organization_id', None),
        gateway=packet_dict.get('gateway', None),
        tmst=packet_dict.get('tmst', None),
        chan=packet_dict.get('chan', None),
        rfch=packet_dict.get('rfch', None),
        freq=packet_dict.get('freq', None),
        stat=packet_dict.get('stat', None),
        modu=packet_dict.get('modu', None),
        datr=packet_dict.get('datr', None),
        codr=packet_dict.get('codr', None),
        lsnr=packet_dict.get('lsnr', None),
        rssi=packet_dict.get('rssi', None),
        size=packet_dict.get('size', None),
        data=packet_dict.get('data', None),
        m_type=packet_dict.get('m_type', None),
        major=packet_dict.get('major', None),
        mic=packet_dict.get('mic', None),
        join_eui=packet_dict.get('join_eui', None),
        dev_eui=packet_dict.get('dev_eui', None),
        dev_nonce=packet_dict.get('dev_nonce', None),
        dev_addr=packet_dict.get('dev_addr', None),
        adr=packet_dict.get('adr', None),
        ack=packet_dict.get('ack', None),
        adr_ack_req=packet_dict.get('adr_ack_req', None),
        f_pending=packet_dict.get('f_pending', None),
        class_b=packet_dict.get('class_b', None),
        f_count=packet_dict.get('f_count', None),
        f_opts=packet_dict.get('f_opts', None),
        f_port=packet_dict.get('f_port', None),
        error=packet_dict.get('error', None),
        latitude=packet_dict.get('latitude', None),
        longitude=packet_dict.get('longitude', None),
        altitude=packet_dict.get('altitude', None),
        app_name=packet_dict.get('app_name', None),
        dev_name=packet_dict.get('dev_name', None),
        gw_name=packet_dict.get('gw_name', None)
    )
    new_packet.save_to_db()
    logging.debug(f'Saving packet ID {new_packet.id}')
    return new_packet


def callback(ch, method, properties, body):
    logging.debug("Received message on queue collectors_queue")
    try:
        message = body.decode("utf-8")
        # Parse the JSON into a dict
        data = json.loads(message)
        
        # This packet is a JSON object
        packet = data.get('packet')
        messages = data.get('messages')
        
        if packet:
            # Now, this packet is a DB object
            packet= save_packet(packet)
        else:
            logging.debug("no packet data present")
        
        if messages and len(messages) > 0:
            save_messages(messages, messages[0].get('data_collector_id'), packet.id if packet else None)
        else:
            logging.debug("no messages present")
    except Exception as e:
        logging.error(f"There was an error processing packet {body}. Exception: {e}")

    try:
        logging.debug("Acknowledging the message")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error("There was an error ACK-ing the packet: %s. Exception: %s" % (body, e))

    try:
        logging.debug("Committing changes to db")
        commit()
    except Exception as exc:
        logging.error(f'Error at commit: {exc}')
        logging.info('Rolling back the session')
        rollback()

    logging.debug("Saved message: %s" % (message))


def initialize_rabbit():
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


def exit_handler():
    logging.info('exit_handler: Flushing messages to s3')
    global CollectorMessageManager
    if CollectorMessageManager:
        CollectorMessageManager.flush_all()

try:
    print("Starting PacketWriter")
    atexit.register(exit_handler)
    print("Initializing s3 manager")
    if 'AWS_ACCESS_KEY_ID' in os.environ and len(os.environ['AWS_ACCESS_KEY_ID'])>0\
         and 'AWS_SECRET_ACCESS_KEY' in os.environ and len(os.environ['AWS_SECRET_ACCESS_KEY'])>0:
        CollectorMessageManager = S3CollectorMessagesManager(aws_access_key=os.environ["AWS_ACCESS_KEY_ID"],
                                            aws_secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                                            bucket_name=os.environ["AWS_COLLECTOR_MSGS_BUCKET"],
                                            logger=logging.getLogger())
    else:
        CollectorMessageManager = LogCollectorMessagesManager(logger=logging.getLogger())
    print("Initializing rabbit connection")
    initialize_rabbit()
except Exception as e:
    logging.error(f'There was an error initializing PacketWriter: {e}')
finally:
    logging.info('Flushing messages to s3')
    if CollectorMessageManager:
        CollectorMessageManager.flush_all()


# while(True):
#     time.sleep(5)
#     try:
#         commit()
#         logging.debug('Commit done!')
#     except Exception as exc:
#         logging.error('Error at commit:', exc)
#         logging.info('Rolling back the session')
#         rollback()
