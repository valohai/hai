from io import BytesIO

import boto3
import pytest
from hai.boto3_multipart_upload import MultipartUploader, S3_MINIMUM_MULTIPART_FILE_SIZE
from moto import mock_s3


@mock_s3
@pytest.mark.parametrize('file_type', ('real', 'imaginary'))
def test_multipart_upload(tmpdir, file_type):
    if file_type == 'real':
        temp_path = tmpdir.join('temp.dat')
        with temp_path.open('wb') as outf:
            for chunk in range(17):
                outf.write(bytes((chunk,)) * 1024 * 1024)
            expected_size = outf.tell()
        file = temp_path.open('rb')
    elif file_type == 'imaginary':
        expected_size = S3_MINIMUM_MULTIPART_FILE_SIZE * 2
        file = BytesIO(b'\xC0' * expected_size)
        file.seek(0)
    else:  # pragma: no cover
        raise NotImplementedError('...')

    s3 = boto3.client('s3', region_name='us-east-1')
    bucket_name = 'mybucket'
    key_name = 'hello/world'
    s3.create_bucket(Bucket=bucket_name)
    mpu = MultipartUploader(s3)
    events = []

    def event_handler(**args):
        events.append(args)

    mpu.on('*', event_handler)

    with file:
        mpu.upload_file(bucket_name, key_name, file)

    obj = s3.get_object(Bucket=bucket_name, Key=key_name)
    assert obj['ContentLength'] == expected_size
    assert any(e['event'] == 'progress' for e in events)


@mock_s3
def test_invalid_chunk_size():
    s3 = boto3.client('s3', region_name='us-east-1')
    mpu = MultipartUploader(s3)
    with pytest.raises(ValueError):
        mpu.upload_file('foo', 'foo', BytesIO(), chunk_size=300)


@mock_s3
def test_invalid_file_size(tmpdir):
    s3 = boto3.client('s3', region_name='us-east-1')
    pth = tmpdir.join('temp.dat')
    pth.write('foofoo')
    mpu = MultipartUploader(s3)
    with pytest.raises(ValueError):
        mpu.upload_file('foo', 'foo', pth.open())


@mock_s3
def test_error_handling():
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='foo')

    def upload_fn(**args):
        raise IOError('the internet is dead')

    s3.upload_part = upload_fn
    mpu = MultipartUploader(s3)
    with pytest.raises(IOError):
        mpu.upload_parts('foo', 'foo', [b'\x00' * S3_MINIMUM_MULTIPART_FILE_SIZE])
