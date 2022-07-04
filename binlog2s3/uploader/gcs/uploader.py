import datetime
import io

from google.auth.transport.requests import AuthorizedSession
from google.resumable_media import requests, common
from google.cloud import storage
from google.cloud.exceptions import NotFound

from binlog2s3.uploader.uploader import Uploader


class GCSUploader(Uploader):
    CHUNK_SIZE = 5 * 1024 * 1024 # 5M

    def __init__(self, bucket_name, filename):
        self._bucket_name = bucket_name
        self._url = (f'https://www.googleapis.com/upload/storage/v1/b/{self._bucket_name}/o?uploadType=resumable')

        self._filename = filename
        self._storage_client = storage.Client()
        self._transport = AuthorizedSession(
            credentials=self._storage_client._credentials
        )
        self.test_bucket_access()

        self.part_number = 0
        self.content_type = 'application/octet-stream'
        self.metadata = {'name': self._filename}

        self._bucket = self._storage_client.bucket(self._bucket_name)
        self._request = requests.ResumableUpload(
            upload_url=self._url,
            chunk_size=self.CHUNK_SIZE
        )

        self._buffer = io.BytesIO(b'')
        self._buffer_size = 0

    def test_bucket_access(self):
        try:
            self._storage_client.get_bucket(self._bucket_name)
        except NotFound:
            raise AssertionError("Could not access GCS bucket {name}".format(name=self._bucket_name))

    def create_multipart_upload(self):
        print("{dt} Creating multipart uploader for {filename}".format(
            dt=datetime.datetime.now(), filename=self._filename
        ))

        self._request.initiate(
            transport=self._transport,
            stream=self._buffer,
            stream_final=False,
            metadata=self.metadata,
            content_type=self.content_type
        )

    def upload_part(self, data):
        self.part_number += 1
        print("{dt} Uploading part {number} for {filename} size {size}".format(
            dt=datetime.datetime.now(), number=self.part_number, filename=self._filename, size=len(data)
        ))
        self._buffer.write(data)
        self._buffer.seek((self.part_number - 1) * self.CHUNK_SIZE)
        self._request.transmit_next_chunk(self._transport)


    def close_multipart_upload(self):
        print("{dt} Finishing multipart upload for {filename}".format(
            dt=datetime.datetime.now(), filename=self._filename
        ))
        # self._request.transmit_next_chunk(self._transport)
