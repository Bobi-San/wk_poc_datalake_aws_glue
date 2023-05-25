"""
Select raw data with a LastUpdated date older than n minutes.
The reason is to leave a time gap in case data has been written
in S3 to mitigate the eventual consistency.
"""
import os
from datetime import datetime, timezone, timedelta
import time
import uuid
from dateutil import parser
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


def prefix_object_name(object_name, sep='_', iso_dt=False, iso_tm=False, uu_id=False):
    """
    Prefix the object name upon given parameters in the below order:
        <with_prefix><sep><add_datetime><sep><add_uuid><sep><object_name>
    :param object_name: Name of the object
    :param sep: Separator
    :param iso_dt: Format is YYYYMMDD with current utc date
    :param iso_tm: Format is HH24MISS with current utc time
    :param uu_id: Set by uuid4
    :return: Formatted object_name
    """
    new_prefix = ''
    dt_utc = datetime.now(timezone.utc)
    if iso_dt:
        new_prefix += dt_utc.strftime('%Y%m%d') + sep
    if iso_tm:
        new_prefix += dt_utc.strftime('%H%M%S') + sep
    if uu_id:
        new_prefix += str(uuid.uuid4()) + sep
    return new_prefix + object_name


def exec_func_with_max_retries(func_to_exec, max_tries=3, sleep_sec=5, func_text=None, quiet_mode=True):
    """
    This wrapper allows a function to be executed with retries.
    If the func_to_exec has parameters just use the below call:
        exec_func_with_max_retries(lambda: my_func(p1, p2))
    :param func_to_exec: Name of the function to execute
    :param max_tries: Maximum retries
    :param sleep_sec: Time in seconds to sleep between retries
    :param func_text: Text to display while attempting to execute
    :return: Number of retries. On FAILURE the exception
    """
    retry_cnt = 0
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
                if not quiet_mode:
                    print(f"WARNING: Retry {retry_cnt} out of {max_tries},"
                          f" sleeping {sleep_sec} seconds")
                time.sleep(sleep_sec)
            else:
                if not quiet_mode:
                    print(f"ERROR: Max retries exceeded {retry_cnt} out of {max_tries}")
                raise exception_handler
            continue
    return retry_cnt


def move_s3_key_from_to_location(bucket_name, object_key_from, object_key_to):
    """In AWS S3 there is no file rename nor move nor folders/sub-folders
       Hence AWS does copy+delete of the prefixes keys
    :param bucket_name: Name of S3 bucket
    :param object_key_from: Prefix Key of the source
    :param object_key_to: Prefix Key of the target
    :return: -
    """
    s3_client = boto3.client('s3')  # Simple Storage Service
    copy_source = {'Bucket': bucket_name, 'Key': object_key_from}
    s3_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=object_key_to)
    s3_client.delete_object(Bucket=bucket_name, Key=object_key_from)


def get_s3_key_tag(bucket_name, object_key, tag_key):
    """Get object tag value
    :param bucket_name: Name of S3 bucket
    :param object_key: Prefix key
    :param tag_key: Tag key
    :return: Tag value
    """
    s3_client = boto3.client('s3')  # Simple Storage Service
    tags_response = s3_client.get_object_tagging(Bucket=bucket_name, Key=object_key)
    for tag in tags_response['TagSet']:
        if tag['Key'] == tag_key:
            return tag['Value']
    return None


def put_s3_key_tag(bucket_name, object_key, tag_key, tag_new_value, if_tag_value=None):
    """Get object tag value
    :param bucket_name: Name of S3 bucket
    :param object_key: Prefix key
    :param tag_key: Tag key
    :param tag_new_value: Tag new value
    :param if_tag_value: Change value only if matches current Tag value
    :return: -
    """
    if if_tag_value and if_tag_value != get_s3_key_tag(bucket_name, object_key, tag_key):
        return

    # Create or Replace Tag key:value
    s3_client = boto3.client('s3')  # Simple Storage Service
    tags_response = s3_client.get_object_tagging(Bucket=bucket_name, Key=object_key)
    tag_set = tags_response['TagSet']
    if not any(tag['Key'] == tag_key for tag in tag_set):
        tag_set.append({"Key": tag_key, "Value": tag_new_value})
    else:
        for tag in tag_set:
            if tag['Key'] == tag_key:
                tag['Value'] = tag_new_value
                break
    s3_client.put_object_tagging(Bucket=bucket_name, Key=object_key, Tagging={'TagSet': tag_set})


def list_s3_key_older_than(bucket_name, start_at_prefix, utc_dtm=None, minutes_ago=None):
    """
    List all the keys in S3 bucket starting from a prefix down.
    Keys with LastModified datetime older than given minutes ago will be returned as a list
    :param bucket_name: Name of S3 bucket
    :param start_at_prefix: Starting from this prefix down
    :param utc_dtm: UTC datetime
    :param minutes_ago: older than minutes ago
    :return: list of dict: Key, LastModified(ISO 8601), Size, StorageClass
            Note: an empty list will be returned when input parameters are invalid
    """
    if not utc_dtm:
        utc_dtm = datetime.now(timezone.utc)
    if not minutes_ago or not isinstance(minutes_ago, int):
        minutes_ago = 5  # TODO: Maybe get it from SSM # pylint: disable=W0511

    # Validate
    try:
        utc_dtm_iso8601 = utc_dtm.strftime('%Y-%m-%dT%H:%M:%S')
        parser.parse(utc_dtm_iso8601)
        utc_dtm -= timedelta(minutes=minutes_ago)
    except ValueError:
        print(f'ERROR: Invalid utc_dtm|minutes_ago: {utc_dtm},{minutes_ago}')
        return []

    # Get the details
    s3_keys_list = []
    try:
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=start_at_prefix)
        for page in page_iterator:
            for obj in page['Contents']:
                # Must have Key in dictionary, filter out "Folders", older "Files"
                if 'Key' in obj \
                        and not str(obj['Key']).endswith('/') \
                        and obj['LastModified'] <= utc_dtm:
                    # Convert LastModified to string ISO 8601
                    last_modified1_dt = obj['LastModified'].strftime('%Y-%m-%dT%H:%M:%S')
                    last_modified2_dt = obj['LastModified'].strftime('%f')
                    s3_keys_list.append({
                        'Key': obj['Key'],
                        'LastModified': last_modified1_dt
                                        + '.' + last_modified2_dt[:3] + 'Z',
                        'Size': obj['Size'],
                        'StorageClass': obj['StorageClass']
                    })
    except Exception as exception_handler:  # pylint: disable=W0703
        print(exception_handler)
        print(f'ERROR: Cannot list_s3_key_older_than '
              f'{bucket_name}, {start_at_prefix}, {utc_dtm}, {minutes_ago}')
        return []
    return s3_keys_list


print('========== Get list of allowed Source Id ============')
# Init AWS common objects
region_name = get_current_region_name()

# Init AWS clients
ssm_client = boto3.client('ssm', region_name=region_name)  # Simple System Manager

# Get valid list of incoming sources id from Simple System Manager
valid_source_id_list = ssm_client.get_parameter(  # TODO: Wrap into common function # pylint: disable=W0511
    Name='/datalake/bronze/source_id-list')['Parameter']['Value']


print('========== Select candidate prefixes ============')
bucket_name = 'rgi-sandbox-repo-dev'  # TODO: Get it from SSM # pylint: disable=W0511
start_at_prefix = 'DataLakeV1/ArrivalHub/PendingSelection/' # TODO: Get it from SSM # pylint: disable=W0511
list_s3_key = list_s3_key_older_than(bucket_name, start_at_prefix, minutes_ago=2)


print('========== Scan over prefixes ============')
move_to_prefix = 'DataLakeV1/ArrivalHub/PendingValidations/'  # TODO: Get it from SSM # pylint: disable=W0511
new_object_tag_value = 'ProcessStatus'  # TODO: Maybe get it from SSM # pylint: disable=W0511
object_tag_value = 'PendingValidations'  # TODO: Get it from SSM # pylint: disable=W0511
# or dynamically:
#if move_to_prefix.endswith('/'):
#    object_tag_value = move_to_prefix.split('/')[-2]
#else:
#    object_tag_value = move_to_prefix.split('/')[-1]

for s3_key in list_s3_key:
    print('=====================================================================================')
    print(s3_key.get('Size'), s3_key.get('LastModified'), s3_key.get('Key'))
    object_key_from = s3_key.get('Key')

    # =====================================================================================
    # TODO: to set into common function # pylint: disable=W0511
    # Inspect s3_key prefix
    object_prefixes_list = object_key_from.split('/')  # Split in prefixes path
    object_prefixes_count = len(object_prefixes_list)
    # Check prefixes parts, expected at least Medallion/SubFolder/SourceId/FimeName.json
    # TODO: The below may need to create a metric to set off an alarm # pylint: disable=W0511
    if object_prefixes_count < 4:
        raise Exception(f"ERROR: Invalid object prefixes path,"
                        f" expected 4 actual {object_prefixes_count}")
    # Extract Source Id and "filename"
    source_id = object_prefixes_list[-2]  # Skip "filename" and get last sub-folder prefix
    object_name = object_prefixes_list[-1]  # Get "filename"
    root_prefix = '/'.join(object_prefixes_list[:object_prefixes_count-3])
    # =====================================================================================

    print('========== Calidate candidate prefixes ============')
    if len(object_key_from) > 4 and object_key_from[-5:] == '.json' \
            and source_id in valid_source_id_list:
        # Update key tag
        put_s3_key_tag(bucket_name, object_key_from, new_object_tag_value, object_tag_value)
        # Set the new destination
        object_key_to = move_to_prefix + source_id + '/' + object_key_from.rsplit('/', 1)[-1]
        # Move key to new destination
        if 'PlaceHolder' not in object_key_from:
            print('move_s3_key_from_to_location from:', object_key_from)
            print('move_s3_key_from_to_location to:  ', object_key_to)
            move_s3_key_from_to_location(bucket_name, object_key_from, object_key_to)
    else:
        print(f"Ignored: {object_key_from}")
