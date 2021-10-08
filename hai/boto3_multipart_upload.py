import logging
import os
from typing import IO, Any, Dict, Generator, Iterable, Optional

from botocore.client import BaseClient

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

    def __init__(self, s3: BaseClient, log: Optional[logging.Logger] = None) -> None:
        """
        :param s3: The boto3 S3 client to upload with
        :param log: A logger, if desired.
        """
        self.s3 = s3
        self.log = (log or logging.getLogger(self.__class__.__name__))

    def upload_parts(
        self,
        bucket: str,
        key: str,
        parts: Iterable[bytes],
        create_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Upload the given parts of binary data as a multipart upload.

        :param bucket: Bucket to upload to.
        :param key: Key to upload to.
        :param parts: Iterable (may be a generator) of bytes to upload.
                      It is expected that these chunks all correspond to S3's standards, i.e.
                      are >= S3_MINIMUM_MULTIPART_CHUNK_SIZE (aside from the last part).
                      If they aren't, completing the upload will fail.
        :param create_params: Any additional parameters to pass to `create_multipart_upload`.
                              These roughly correspond to what one might be able to pass to `put_object`.
        :return: The return value of `complete_multipart_upload`.
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
                        self.log.error(f'Error uploading part {part_number} (attempt {attempt})', exc_info=True)
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

        return self.s3.complete_multipart_upload(  # type: ignore[no-any-return]
            Bucket=bucket,
            Key=key,
            UploadId=mpu['UploadId'],
            MultipartUpload={'Parts': part_infos},
        )

    def read_chunk(self, fp: IO[bytes], size: int) -> bytes:
        """
        Read a chunk of up to size `size` from the filelike object `fp`.

        Useful for overriding, e.g. computing checksums over the chunks.
        """
        return fp.read(size)

    def upload_file(
        self,
        bucket: str,
        key: str,
        fp: IO[bytes],
        chunk_size: Optional[int] = None,
        create_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Upload the given file in chunks.

        :param bucket: Bucket to upload to.
        :param key: Key to upload to.
        :param fp: File-like object to upload.
        :param chunk_size: Override automagically determined chunk size.
        :param create_params: Any additional parameters to pass to `create_multipart_upload`.
                              These roughly correspond to what one might be able to pass to `put_object`.
        :return: The return value of the `complete_multipart_upload` call.
        """
        if not hasattr(fp, 'read'):  # pragma: no cover
            raise TypeError('`fp` must have a `read()` method')

        try:
            size = os.stat(fp.fileno()).st_size  # type: Optional[int]
        except (OSError, AttributeError):
            size = None

        if size and size <= self.minimum_file_size:
            raise ValueError(
                f'File is too small to upload as multipart {size} bytes '
                f'(must be at least {self.minimum_file_size} bytes)'
            )

        if not chunk_size:
            chunk_size = self.determine_chunk_size_from_file_size(size)
            minimum = max(S3_MINIMUM_MULTIPART_CHUNK_SIZE, self.minimum_chunk_size)
            maximum = min(S3_MAXIMUM_MULTIPART_CHUNK_SIZE, self.maximum_chunk_size)
            chunk_size = int(max(minimum, min(chunk_size, maximum)))

        if not S3_MINIMUM_MULTIPART_CHUNK_SIZE <= chunk_size < S3_MAXIMUM_MULTIPART_CHUNK_SIZE:
            raise ValueError(
                f'Chunk size {chunk_size} is outside the protocol limits '
                f'({S3_MINIMUM_MULTIPART_CHUNK_SIZE}..{S3_MAXIMUM_MULTIPART_CHUNK_SIZE})'
            )

        def chunk_generator() -> Generator[bytes, None, None]:
            while True:
                chunk = self.read_chunk(fp, chunk_size)  # type: ignore[arg-type]
                if not chunk:
                    break
                yield chunk

        return self.upload_parts(bucket, key, parts=chunk_generator(), create_params=create_params)

    def determine_chunk_size_from_file_size(self, file_size: Optional[int]) -> int:
        if file_size:
            return int(file_size / 20)
        return self.default_chunk_size
