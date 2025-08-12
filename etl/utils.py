import os, re, sys, yaml
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

def parse_date_from_name(name:str):
    m = re.search(r'(\d{4})[-_]?(\d{2})', name)
    if not m:
        return None, None
    return m.group(1), m.group(2)

def infer_type(name:str): # checks if the files are comments or submissions files
    n = name.lower()
    if "rc" in n:
        return "rc"
    if "rs" in n or "post" in n or "link" in n:
        return "rs"
    return "unknown"

def load_yaml(path: str) -> dict:
    text = open(path, "r", encoding="utf-8").read()
    text = os.path.expandvars(text)   # allows ${BUCKET_BRONZE} etc.
    return yaml.safe_load(text)


def object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        if code in ("404", "NoSuchKey", "NotFound") or status == 404:
            return False
        raise  # real error (credentials, bucket issues etc.)

def validate_s3_env_vars(endpoint: str, key: str, secret: str) -> None:
    """
    Checks that all required S3 environment variables are set.
    Exits the program with an error message if any are missing.
    """
    if not all([endpoint, key, secret]):
        print(
            "ERROR: Missing S3 env vars (S3_ENDPOINT_URL, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY).",
            file=sys.stderr
        )
        sys.exit(2)

def s3_client(layer:str):
    endpoint = os.getenv("S3_ENDPOINT_URL")

    if layer == "bronze":
        key = os.getenv("BRONZE_ACCESS_KEY_ID")
        secret = os.getenv("BRONZE_SECRET_ACCESS_KEY")
    elif layer == "silver":
        key = os.getenv("SILVER_ACCESS_KEY_ID")
        secret = os.getenv("SILVER_SECRET_ACCESS_KEY")
    else:
        sys.exit(f"ERROR: Invalid layer '{layer}'. Must be 'bronze' or 'silver'.")
        
    validate_s3_env_vars(endpoint, key, secret)
    return boto3.session.Session().client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        config=Config(s3={"addressing_style":"path"})
    )

# def s3_client(): # use this instead if all in same bucket
#     endpoint = os.getenv("S3_ENDPOINT_URL")
#     key = os.getenv("S3_ACCESS_KEY_ID")
#     secret = os.getenv("S3_SECRET_ACCESS_KEY")
#     validate_s3_env_vars(endpoint, key, secret)
#     session = boto3.session.Session()
#     return session.client("s3",
#         endpoint_url=endpoint,
#         aws_access_key_id=key,
#         aws_secret_access_key=secret,
#         config=Config(s3={"addressing_style":"path"})
#     )
