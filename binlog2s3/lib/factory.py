def get_binlog_process(mysqlbinlog_bin, hostname, port, username, password, start_file, tempdir):
    from binlog2s3.binlog.process import MySQLBinlogProcess
    return MySQLBinlogProcess(mysqlbinlog_bin, hostname, port, username, password, start_file, tempdir)


def get_binlog_reader(tempdir):
    from binlog2s3.binlog.reader import MySQLBinlogReader
    return MySQLBinlogReader(tempdir)


def get_uploader(provider, bucket_name, filename):
    if provider == "s3":
        from binlog2s3.uploader.s3.uploader import S3Uploader
        return S3Uploader(bucket_name, filename)

    if provider == "gcs":
        from binlog2s3.uploader.gcs.uploader import GCSUploader
        return GCSUploader(bucket_name, filename)

def get_streamer(mysqlbinlog_bin, hostname, port, username, password, start_file, tempdir, provider, bucket_name):
    from binlog2s3.stream.stream import StreamBinlogs

    if provider not in ['s3', 'gcs']:
        raise AssertionError("Unsupported cloud provider: {name}, currently supported are: s3 and gcs".format(name=provider))

    return StreamBinlogs(mysqlbinlog_bin, hostname, port, username, password, start_file, tempdir, provider, bucket_name)
