"""Upload Sample Data to S3 Bucket

Author: Enrique Olivares <enrique.olivares@wizeline.com>

Description: Uploads data from sample server directly into S3.
"""

import argparse
import tempfile
import urllib.parse
from typing import Optional, Tuple

import boto3
import requests

DATASET_URL = (
    "https://raw.githubusercontent.com/enroliv/adios/main/data/chart-data.csv"
)

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("uri", help="Full URI of destination S3 object.")
parser.add_argument(
    "-r",
    "--replace",
    help="Replace the already existing key",
    action="store_true",
)


class DuplicateError(Exception):
    """Error raised whenever an S3 object key already exists."""


def main(_args: argparse.Namespace) -> None:
    """Main function.

    Args:
        _args (argparse.Namespace): CLI arguments.

    Raises:
        KeyExistsError: Whenever an object already exists and should not be
        replaced.
    """
    uri = _args.uri
    replace = _args.replace

    bucket, key = parse_uri(uri)
    session = boto3.Session()
    if not replace and check_key_exists(bucket, key, session):
        raise DuplicateError("Specified object already exists.")

    client = session.client("s3")
    with tempfile.NamedTemporaryFile("wb+") as tmp:
        download_samples_from_url(tmp.name)
        client.upload_file(tmp.name, bucket, key)


def parse_uri(uri: str) -> Tuple[str, str]:
    """Parse uri string into bucket and key valyes

    Args:
        uri (str): S3 URI string.

    Returns:
        Tuple[str, str]: Bucket name and object key.
    """
    parsed = urllib.parse.urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path[1:]

    return (bucket, key)


def check_key_exists(
    bucket: str, key: str, session: Optional[boto3.Session] = None
) -> bool:
    """Checks if a specified file already exists.

    Args:
        bucket (str): S3 bucket name.
        key (str): S3 object key.
        session (Optional[boto3.Session], optional): Boto3 session object.
        Defaults to None.

    Returns:
        bool: File exists.
    """
    client = session.client("s3") if session else boto3.client("s3")
    response: dict = client.list_objects_v2(Bucket=bucket, Prefix=key)
    for content in response.get("Contents", []):
        if content["Key"] == key:
            return True

    return False


def download_samples_from_url(path: str) -> None:
    """Downloads a set of samples into the specified path.

    Args:
        path (str): Path to output file.
    """
    with open(path, "wb") as out:
        response = requests.get(DATASET_URL)
        out.write(response.content)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
