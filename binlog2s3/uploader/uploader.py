from abc import ABCMeta, abstractmethod

class Uploader(object, metaclass=ABCMeta):
    @abstractmethod
    def test_bucket_access(self):
        pass

    @abstractmethod
    def create_multipart_upload(self):
        pass

    @abstractmethod
    def upload_part(self, data):
        pass

    @abstractmethod
    def close_multipart_upload(self):
        pass
