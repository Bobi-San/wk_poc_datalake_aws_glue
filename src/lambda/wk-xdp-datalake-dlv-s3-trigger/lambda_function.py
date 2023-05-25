"""
AWS S3 Lambda Trigger with high level validations
"""

import urllib.parse
import uuid
import boto3
from scripts import aws_utils


# Init AWS common objects
region_name = aws_utils.get_current_region_name()
lambda_func_name = aws_utils.get_current_lambda_function_name()

# Init AWS clients
s3_client = boto3.client('s3')  # Simple Storage Service
ssm_client = boto3.client('ssm', region_name=region_name)  # Simple System Manager


def lambda_handler(event, context):
    """Function called by S3 as Trigger"""

    # Get the object from the trigger event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)

    print("CONTEXT:", context)
    print("LAMBDA FUNC:", lambda_func_name)
    print("REGION NAME:", region_name)
    print("BUCKET NAME:", bucket_name)
    print("OBJECT KEY:", object_key)
    print("CONTENT TYPE:", response['ContentType'])

    # This trigger will only handle the incoming raw data for all Sources
    if '/Bronze/Delivered/' in object_key:
        try:
            valid_source_id_list = ssm_client.get_parameter(
                Name='/datalake/bronze/source_id-list')['Parameter']['Value']
            object_prefixes_list = object_key.split('/')  # Split in prefixes path
            object_prefixes_count = len(object_prefixes_list)

            # Check prefixes parts, expected at least Medallion/SubFolder/SourceId/FimeName.json
            # TODO: The below may need to create a metric to set off an alarm # pylint: disable=W0511
            if object_prefixes_count < 4:
                raise Exception(f"ERROR: Invalid object prefixes path,"
                                f" expected 4 actual {object_prefixes_count}")

            source_id = object_prefixes_list[-2]  # Skip filename and get last sub-folder prefix
            file_name = object_prefixes_list[-1]  # Get filename
            root_prefix = '/'.join(object_prefixes_list[:object_prefixes_count-3])
            print("OBJECT JSON:", file_name, " SOURCE ID:", source_id)

            # Derive the next location where to move it
            if len(object_key) > 4 and object_key[-5:] == '.json' \
                    and source_id in valid_source_id_list:
                raise_exception_flg = False
                object_ext = object_key[-4:]
                s3_move_to_location = root_prefix + '/PendingSelection/' + source_id + '/' + file_name
                print("OBJECT EXT:", object_ext)
            else:
                raise_exception_flg = True
                if source_id not in valid_source_id_list:
                    s3_move_to_location = root_prefix + '/Rejected/Unknown' + '/' \
                                        + str(uuid.uuid4()) + '_' + file_name
                else:
                    s3_move_to_location = root_prefix + '/Rejected/' + source_id + '/' \
                                        + str(uuid.uuid4()) + '_' + file_name
            # Move file to next location
            print("OBJECT MOVETO:", s3_move_to_location)
            aws_utils.exec_func_with_max_retries(
                lambda: aws_utils.move_s3_key_from_to_location(
                    bucket_name, object_key, s3_move_to_location),
                func_text=f"Moving from bucket {bucket_name} key {object_key}"
                          f"to {s3_move_to_location}"
            )

            # TODO: The below may need to create a metric to set off an alarm # pylint: disable=W0511
            # TODO: Information from the Metadata may be collected for statistics # pylint: disable=W0511
            if raise_exception_flg:
                raise Exception(f"ERROR: Invalid object SourceId or Extension."
                                f" Moved to {s3_move_to_location}")

        except Exception as exception_handler:
            print(exception_handler)
            print(f'ERROR: AWS S3 Trigger failed for Key {object_key} in Bucket {bucket_name}')
            raise exception_handler
    else:
        raise Exception('ERROR: Invalid object key is not in /Bronze/Delivered/')
    return response['ContentType']
