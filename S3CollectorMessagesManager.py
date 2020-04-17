import datetime
import gzip
import io
import json
import logging
from collections import defaultdict

import boto3


class S3CollectorMessagesManager:

    def __init__(self, aws_access_key, aws_secret_key, bucket_name, maximum_msgs_per_collector=500, logger=None):
        """
        Initializes the instance
        :param aws_access_key: public aws api access key
        :param aws_secret_key: pricate aws api access key
        :param bucket_name: name of the base bucket to use
        :param maximum_msgs_per_collector: maximum number of messages to keep in memory before flushing to s3
        :param logger: logger instance (logging library) to use
        """
        self.logger = logger
        self.MAX_MSGS_PER_COLLECTOR = maximum_msgs_per_collector
        self.bucket_messages = self.get_bucket(aws_access_key, aws_secret_key, bucket_name)
        # dictionary with key=dc_id, value=list of messages
        self.messages_per_collector = defaultdict(list)

    def log(self, level, message):
        """
        proxy to filter log messages if logger is not initialized
        :param level: level of the message (logging.INFO, logging.DEBUG, etc)
        :param message: string to log
        :return: nothing. Message gets logged if the logger is defined
        """
        if self.logger:
            self.logger.log(level, message)

    def get_bucket(self, aws_access_key, aws_secret_key, bucket_name):
        """
        Gets a Bucket instance from AWS
        :param aws_access_key: public api access key
        :param aws_secret_key: private api access key
        :param bucket_name: name of the bucket
        :return: boto3.Bucket instance for the desired bucket
        """
        self.log(logging.DEBUG, 'get s3 bucket')
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        s3 = session.resource('s3')
        return s3.Bucket(bucket_name)

    def save_collector_messages(self, data_collector_id, messages):
        """
        Add messages to the internal dictionary. If the resultant number of messages is greater than MAX_MSGS_PER_COLLECTOR,
        the existing ones are sent to s3 first and the new ones will be saved to the dictionary only
        :param data_collector_id: id of the collector from which the messages came from
        :param messages: list of messages
        :return: nothing. Messages are saved to internal dictionary, potentially triggering a sending to s3
        """
        self.log(logging.DEBUG, f'Adding {len(messages)} messages to collector {data_collector_id}')
        actual_msgs = self.get_messages_for_collector(data_collector_id)
        self.log(logging.DEBUG, f'Actual messages: {len(actual_msgs)}')
        if (len(actual_msgs) + len(messages) > self.MAX_MSGS_PER_COLLECTOR):
            self.log(logging.DEBUG, 'Sending actual message list to s3 first')
            self.send_messages_to_s3(data_collector_id, dt=datetime.datetime.now())

        self.log(logging.DEBUG, 'extending actual messages list')
        actual_msgs.extend(messages)
        self.log(logging.DEBUG, f'data collector {data_collector_id} now has {len(actual_msgs)} messages in the buffer')

    def send_messages_to_s3(self, data_collector_id, dt):
        """
        Send messages for collector to s3
        Messages are compacted using gzip and saved with a prefix created from datetime in 'dt' parameter
        :param data_collector_id: id of the data collector to save
        :param dt: packet datetime. It may contain messages from different days
        :return: nothing. Messages for given data collector are cleared from the internal dictionary
        """
        messages = self.get_messages_for_collector(data_collector_id)
        if len(messages) == 0:
            self.log(logging.INFO, f'There are no messages for collector {data_collector_id}')
            return

        self.log(logging.DEBUG, f'sending {len(messages)} messages for collector {data_collector_id} to s3')
        filename = self.get_filename(data_collector_id, dt)

        # zips the data to a memory file, then copy the file to s3 bucket
        f = io.BytesIO()
        with gzip.GzipFile(fileobj=f, mode='wb') as gz:
            for msg in messages:
                # write one json per line so it can be consumed by spark easily
                json_str = json.dumps(msg)
                gz.write(bytes(json_str + '\n', encoding='utf-8'))
        f.seek(0)
        self.bucket_messages.upload_fileobj(f, filename)
        f.close()
        self.clear_collector_messages(data_collector_id)

    def get_messages_for_collector(self, data_collector_id):
        """
        Returns list of messages for given data collector, from internal dictionary
        :param data_collector_id: id of the collector
        :return: list of messages
        """
        return self.messages_per_collector[data_collector_id]

    def clear_collector_messages(self, data_collector_id):
        """
        Clear collector messages from internal dictionary
        :param data_collector_id: id of the collector
        :return: nothing. Message list for the collector is cleared in dictionary
        """
        self.log(logging.DEBUG, f'Clearing messages list for collector {data_collector_id}')
        self.messages_per_collector[data_collector_id].clear()

    def flush_all(self):
        """
        Flush the whole dictionary to s3
        :return: nothing. Data for all collectors is send to s3
        """
        self.log(logging.DEBUG, 'Sending all remaining messages to s3')
        for id in self.messages_per_collector:
            self.send_messages_to_s3(id, dt=datetime.datetime.now())

    def get_filename(self, data_collector_id, dt):
        """
        Constructs the filename to use for a messages packet to send to s3
        :param data_collector_id: id of the collector
        :param dt: datetime to use for the packet
        :return: string with the complete name of the file (prefix+filename+ext)
        """
        return f'year={dt.year:04}/month={dt.year:04}{dt.month:02}/day={dt.year:04}{dt.month:02}{dt.day:02}/collector={data_collector_id}/messages_collector_{data_collector_id}_{dt.strftime("%Y-%m-%d %H:%M:%S")}.json.gz'
