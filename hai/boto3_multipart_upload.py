import logging

import os

from hai.event_emitter import EventEmitter

S3_MINIMUM_MULTIPART_CHUNK_SIZE = 5 * 1024 * 1024
S3_MAXIMUM_MULTIPART_CHUNK_SIZE = 5 * 1024 * 1024 * 1024
S3_MINIMUM_MULTIPART_FILE_SIZE = S3_MINIMUM_MULTIPART_CHUNK_SIZE


class MultipartUploader(EventEmitter):
    event_types = {
        'progress',
        'part-error',
    }
    part_retry_attempts = 10
    minimum_file_size = S3_MINIMUM_MULTIPART_FILE_SIZE
    minimum_chunk_size = S3_MINIMUM_MULTIPART_CHUNK_SIZE
    maximum_chunk_size = 150 * 1024 * 1024
    default_chunk_size = 50 * 1024 * 1024

    def __init__(self, s3, log=None):
        """
        :param s3: The boto3 S3 client to upload with
        :type s3: botocore.client.BaseClient
        :param log: A logger, if desired.
        :type log: logging.Logger
        """
        self.s3 = s3
        self.log = (log or logging.getLogger(self.__class__.__name__))

    def upload_parts(self, bucket, key, parts, create_params=None):
        """
        Upload the given parts of binary data as a multipart upload.

        :param bucket: Bucket to upload to.
        :type bucket: str
        :param key: Key to upload to.
        :type key: str
        :param parts: Iterable (may be a generator) of bytes to upload.
                      It is expected that these chunks all correspond to S3's standards, i.e.
                      are >= S3_MINIMUM_MULTIPART_CHUNK_SIZE (aside from the last part).
                      If they aren't, completing the upload will fail.
        :type parts: Iterable[bytes]
        :param create_params: Any additional parameters to pass to `create_multipart_upload`.
                              These roughly correspond to what one might be able to pass to `put_object`.
        :type create_params: dict[str, object]
        :return: The return value of `complete_multipart_upload`.
        :rtype: dict
        """
        if create_params is None:
            create_params = {}
        mpu = self.s3.create_multipart_upload(Bucket=bucket, Key=key, **create_params)
        part_infos = []
        bytes = 0
        try:
            for part_number, chunk in enumerate(parts, 1):
                for attempt in range(self.part_retry_attempts):
                    try:
                        part = self.s3.upload_part(
                            Bucket=bucket,
                            Key=key,
                            PartNumber=part_number,
                            UploadId=mpu['UploadId'],
                            Body=chunk,
                        )
                    except Exception as exc:
                        self.log.error('Error uploading part {part_number} (attempt {attempt})'.format(
                            part_number=part_number,
                            attempt=attempt,
                        ), exc_info=True)
                        self.emit('part-error', {
                            'chunk': part_number,
                            'attempt': attempt,
                            'attempts_left': self.part_retry_attempts - attempt,
                            'exception': exc,
                        })
                        if attempt == self.part_retry_attempts - 1:
                            raise
                    else:
                        bytes += len(chunk)
                        part_infos.append({'PartNumber': part_number, 'ETag': part['ETag']})
                        self.emit('progress', {
                            'part_number': part_number,
                            'part': part,
                            'bytes_uploaded': bytes,
                        })
                        break
        except:  # noqa
            self.log.debug('Aborting multipart upload')
            self.s3.abort_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=mpu['UploadId'],
            )
            raise

        self.log.info('Completing multipart upload')

        return self.s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=mpu['UploadId'],
            MultipartUpload={'Parts': part_infos},
        )

    def upload_file(self, bucket, key, fp, chunk_size=None, create_params=None):
        """
        Upload the given file in chunks.

        :param bucket: Bucket to upload to.
        :type bucket: str
        :param key: Key to upload to.
        :type key: str
        :param fp: File-like object to upload.
        :type fp: file
        :param chunk_size: Override automagically determined chunk size.
        :type chunk_size: int
        :param create_params: Any additional parameters to pass to `create_multipart_upload`.
                              These roughly correspond to what one might be able to pass to `put_object`.
        :type create_params: dict[str, object]
        :return: The return value of the `complete_multipart_upload` call.
        """
        if not hasattr(fp, 'read'):  # pragma: no cover
            raise TypeError('`fp` must have a `read()` method')

        try:
            size = os.stat(fp.fileno()).st_size
        except (OSError, AttributeError):
            size = None

        if size and size <= self.minimum_file_size:
            raise ValueError(
                'File is too small to upload as multipart {size} bytes '
                '(must be at least {minimum_size} bytes)'.format(
                    size=size,
                    minimum_size=self.minimum_file_size,
                )
            )

        if not chunk_size:
            chunk_size = self.determine_chunk_size_from_file_size(size)
            minimum = max(S3_MINIMUM_MULTIPART_CHUNK_SIZE, self.minimum_chunk_size)
            maximum = min(S3_MAXIMUM_MULTIPART_CHUNK_SIZE, self.maximum_chunk_size)
            chunk_size = int(max(minimum, min(chunk_size, maximum)))

        if not S3_MINIMUM_MULTIPART_CHUNK_SIZE <= chunk_size < S3_MAXIMUM_MULTIPART_CHUNK_SIZE:
            raise ValueError('Chunk size {size} is outside the protocol limits ({min}..{max})'.format(
                size=chunk_size,
                min=S3_MINIMUM_MULTIPART_CHUNK_SIZE,
                max=S3_MAXIMUM_MULTIPART_CHUNK_SIZE,
            ))

        def chunk_generator():
            while True:
                chunk = fp.read(chunk_size)
                if not chunk:
                    break
                yield chunk

        return self.upload_parts(bucket, key, parts=chunk_generator(), create_params=create_params)

    def determine_chunk_size_from_file_size(self, file_size):
        if file_size:
            return file_size / 20
        return self.default_chunk_size
