import datetime
import gzip
import io
import json
import logging
from collections import defaultdict


class LogCollectorMessagesManager:

    def __init__(self, maximum_msgs_per_collector=500, logger=None):
        """
        Initializes the instance
        :param aws_access_key: public aws api access key
        :param aws_secret_key: pricate aws api access key
        :param bucket_name: name of the base bucket to use
        :param maximum_msgs_per_collector: maximum number of messages to keep in memory before flushing to log
        :param logger: logger instance (logging library) to use
        """
        self.logger = logger
        self.MAX_MSGS_PER_COLLECTOR = maximum_msgs_per_collector
        # dictionary with key=dc_id, value=list of messages
        self.messages = defaultdict(list)

    def log(self, level, message):
        """
        proxy to filter log messages if logger is not initialized
        :param level: level of the message (logging.INFO, logging.DEBUG, etc)
        :param message: string to log
        :return: nothing. Message gets logged if the logger is defined
        """
        if self.logger:
            self.logger.log(level, message)


    def save_collector_messages(self, data_collector_id, messages):
        """
        Add messages to the internal dictionary. If the resultant number of messages is greater than MAX_MSGS_PER_COLLECTOR,
        the existing ones are sent to log first and the new ones will be saved to the dictionary only
        :param data_collector_id: id of the collector from which the messages came from
        :param messages: list of messages
        :return: nothing. Messages are saved to internal dictionary, potentially triggering a sending to log
        """
        self.log(logging.DEBUG, f'Adding {len(messages)} messages to collector {data_collector_id}')
        actual_msgs = self.messages[data_collector_id]
        self.log(logging.DEBUG, f'Actual messages: {len(actual_msgs)}')
        if (len(actual_msgs) + len(messages) > self.MAX_MSGS_PER_COLLECTOR):
            self.log(logging.DEBUG, 'Sending actual message list to log first')
            self.save_messages(data_collector_id, dt=datetime.datetime.now())

        self.log(logging.DEBUG, 'extending actual messages list')
        actual_msgs.extend(messages)
        self.log(logging.DEBUG, f'data collector {data_collector_id} now has {len(actual_msgs)} messages in the buffer')

    def save_messages(self, data_collector_id, dt):
        """
        Send messages for collector to log
        Messages are compacted using gzip and saved with a prefix created from datetime in 'dt' parameter
        :param data_collector_id: id of the data collector to save
        :param dt: packet datetime. It may contain messages from different days
        :return: nothing. Messages for given data collector are cleared from the internal dictionary
        """
        messages = self.messages[data_collector_id]
        if len(messages) == 0:
            self.log(logging.INFO, f'There are no messages for collector {data_collector_id}')
            return

        self.log(logging.DEBUG, f'sending {len(messages)} messages for collector {data_collector_id} to log')

        filename = self.get_filename(data_collector_id, dt)
        # zips the data to a memory file, then copy the file to log bucket
        with gzip.GzipFile(filename=filename, mode='wb') as gz:
            for msg in messages:
                # write one json per line so it can be consumed by spark easily
                json_str = json.dumps(msg)
                gz.write(bytes(json_str + '\n', encoding='utf-8'))
        self.messages[data_collector_id].clear()

    def flush_all(self):
        """
        Flush the whole dictionary to log
        :return: nothing. Data for all collectors is send to log
        """
        self.log(logging.DEBUG, 'Sending all remaining messages to log')
        for id in self.messages:
            self.save_messages(id, dt=datetime.datetime.now())

    def get_filename(self, data_collector_id, dt):
        """
        Constructs the filename to use for a messages packet to send to log
        :param data_collector_id: id of the collector
        :param dt: datetime to use for the packet
        :return: string with the complete name of the file (prefix+filename+ext)
        """
        return f'year={dt.year:04}/month={dt.year:04}{dt.month:02}/day={dt.year:04}{dt.month:02}{dt.day:02}/collector={data_collector_id}/messages_collector_{data_collector_id}_{dt.strftime("%Y-%m-%d %H:%M:%S")}.json.gz'
