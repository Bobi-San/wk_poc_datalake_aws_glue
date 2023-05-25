"""
AWS utilities functions
"""

import os
import time
import boto3


def get_current_region_name():
    """Dynamically determine the AWS Region (env, session))"""
    potential_values = [
        os.environ.get('AWS_REGION'),
        os.environ.get('AWS_DEFAULT_REGION'),
        boto3.DEFAULT_SESSION.region_name if boto3.DEFAULT_SESSION else None,
        boto3.Session().region_name,
    ]
    for region_name in potential_values:
        if region_name:
            return region_name
    return None


def get_current_lambda_function_name():
    """Dynamically determine the AWS Lambda Function Name (env))"""
    return os.environ['AWS_LAMBDA_FUNCTION_NAME']


def exec_func_with_max_retries(func_to_exec, max_tries=3, sleep_sec=5, func_text=None):
    """
    This wrapper allows a function to be executed with retries.
    If the func_to_exec has parameters just use the below call:
        exec_func_with_max_retries(lambda: my_func(p1, p2))
    :param func_to_exec: Name of the function to execute
    :param max_tries: Maximum retries
    :param sleep_sec: Time in seconds to sleep between retries
    :param func_text: Text to display while attempting to execute
    :return: On FAILURE the exception
    """
    for retry_cnt in range(max_tries):
        try:
            if func_text:
                print(func_text)
            func_to_exec()
            break
        except Exception as exception_handler:  # pylint: disable=broad-except
            retry_cnt += 1
            print(exception_handler)
            if retry_cnt < max_tries:
                print(f"WARNING: Retry {retry_cnt} out of {max_tries},"
                      " sleeping {sleep_sec} seconds")
                time.sleep(sleep_sec)
            else:
                print(f"ERROR: Max retries exceeded {retry_cnt} out of {max_tries}")
                raise exception_handler
            continue


def move_s3_key_from_to_location(bucket_name, object_key_from, object_key_to):
    """In AWS S3 there is no file rename nor move nor folders/sub-folders
       Hence AWS does copy+delete of the prefixes keys
    :param bucket_name: Name of S3 bucket
    :param object_key_from: Prefix Key of the source
    :param object_key_to: Prefix Key of the target
    :return: on FAILURE the exception
    """
    s3_client = boto3.client('s3')  # Simple Storage Service
    copy_source = {'Bucket': bucket_name, 'Key': object_key_from}
    s3_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=object_key_to)
    s3_client.delete_object(Bucket=bucket_name, Key=object_key_from)
