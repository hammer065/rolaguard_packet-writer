import datetime
import gzip
import json
import os
import boto3

from S3CollectorMessagesManager import S3CollectorMessagesManager
import unittest


class TestS3CollectorMessagesManager(unittest.TestCase):

    def setUp(self):
        self.TEST_COLLECTOR_ID = 999
        # Complete here with your AWS credentials!
        self.aws_access_key = "XXXXXXXXXXXXXXXXXXX"
        self.aws_secret_key = "XXXXXXXXXXXXXXXXXXX+"
        self.bucket_name = 'collector-messages'
        self.manager = S3CollectorMessagesManager(self.aws_access_key, self.aws_secret_key, self.bucket_name)

    def tearDown(self):
        pass

    def test_get_bucket(self):
        assert self.manager.get_bucket(self.aws_access_key, self.aws_secret_key, self.bucket_name) is not None

    def test_get_messages_for_collector(self):
        assert self.manager.get_messages_for_collector(self.TEST_COLLECTOR_ID) == []
        self.manager.messages_per_collector[self.TEST_COLLECTOR_ID] = [{'one': 1, 'two': 2}, {'three': 3, 'four': 4}]
        assert len(self.manager.get_messages_for_collector(self.TEST_COLLECTOR_ID)) == 2
        self.manager.messages_per_collector[self.TEST_COLLECTOR_ID] = [{'five': 5, 'six': 6}]
        assert len(self.manager.get_messages_for_collector(self.TEST_COLLECTOR_ID)) == 1

    # TODO: test that when messages reach the threshold, they are sent to s3
    def test_save_collector_messages(self):
        assert self.manager.messages_per_collector[self.TEST_COLLECTOR_ID] == []
        self.manager.save_collector_messages(self.TEST_COLLECTOR_ID, [{'one': 1, 'two': 2}, {'three': 3, 'four': 4}])
        assert len(self.manager.messages_per_collector[self.TEST_COLLECTOR_ID]) == 2
        self.manager.save_collector_messages(self.TEST_COLLECTOR_ID, [{'five': 5, 'six': 6}])
        assert len(self.manager.messages_per_collector[self.TEST_COLLECTOR_ID]) == 3

    def test_send_messages_to_s3(self):
        session = boto3.Session(aws_access_key_id=self.aws_access_key,
                                aws_secret_access_key=self.aws_secret_key)
        s3 = session.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        objs = list(bucket.objects.filter(Prefix=f'messages_collector_{self.TEST_COLLECTOR_ID}_'))
        for obj in objs:
            obj.delete()

        self.manager.save_collector_messages(self.TEST_COLLECTOR_ID, [{'one': 1, 'two': 2}, {'three': 3, 'four': 4}])
        dt = datetime.datetime(2020, 2, 1, 10, 15, 0)
        self.manager.send_messages_to_s3(self.TEST_COLLECTOR_ID, dt)
        objs = list(bucket.objects.filter(
            Prefix=f'year=2020/month=202002/day=20200201/collector={self.TEST_COLLECTOR_ID}/messages_collector_{self.TEST_COLLECTOR_ID}_'))
        assert len(objs) == 1
        test_filename = 'temp_obj.json.gz'
        bucket.download_file(objs[0].key, test_filename)
        with gzip.GzipFile(filename=test_filename, mode='r') as gz:
            lines = gz.readlines()
        assert len(lines) == 2
        json1 = json.loads(lines[0])
        assert len(json1) == 2
        assert json1['one'] == 1
        assert json1['two'] == 2
        assert 'three' not in json1
        json2 = json.loads(lines[1])
        assert 'three' in json2
        assert json2['three'] == 3
        objs[0].delete()
        os.remove(test_filename)

    def test_clear_collector_messages(self):
        self.manager.messages_per_collector[self.TEST_COLLECTOR_ID] = [{'one': 1, 'two': 2}, {'three': 3, 'four': 4}]
        assert len(self.manager.messages_per_collector[self.TEST_COLLECTOR_ID]) == 2
        self.manager.clear_collector_messages(self.TEST_COLLECTOR_ID)
        assert len(self.manager.messages_per_collector[self.TEST_COLLECTOR_ID]) == 0


if __name__ == '__main__':
    unittest.main()