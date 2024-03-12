import os
import configparser
def get_aws_credentials():
    # Path to the AWS credentials file
    aws_credentials_path = os.path.expanduser("~/.aws/credentials")

    # Check if the file exists
    if not os.path.exists(aws_credentials_path):
        raise FileNotFoundError("AWS credentials file not found.")

    # Parse the AWS credentials file
    config = configparser.ConfigParser()
    config.read(aws_credentials_path)

    # Retrieve the AWS credentials
    aws_access_key_id = config.get("default", "aws_access_key_id")
    aws_secret_access_key = config.get("default", "aws_secret_access_key")

    return aws_access_key_id, aws_secret_access_key

aws_access_key_id, aws_secret_access_key = get_aws_credentials()
print(aws_access_key_id)
print(aws_secret_access_key)
