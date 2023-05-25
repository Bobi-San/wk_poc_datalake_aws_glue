"""
AWS S3 Lambda Trigger with high level validations
"""

import urllib.parse
from scripts import aws_utils
from scripts.aws_s3_triggers import validation_incoming_source_delivery


def lambda_handler(event, context):
    """Function called by S3 as Trigger"""

    # Init AWS common objects
    lambda_func_name = aws_utils.get_current_lambda_function_name()
    region_name = aws_utils.get_current_region_name()

    # Get the object from the trigger event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    print("CONTEXT:", context)
    print("LAMBDA FUNC:", lambda_func_name)
    print("REGION NAME:", region_name)
    print("BUCKET NAME:", bucket_name)
    print("OBJECT KEY:", object_key)

    # Limit the trigger to only few S3 prefixes
    if '/ArrivalHub/Delivered/' in object_key:
        try:
            validation_incoming_source_delivery(bucket_name, object_key)

        except Exception as exception_handler:
            print(exception_handler)
            print(f'ERROR: S3 trigger failed for {object_key} in {bucket_name}')
            raise exception_handler
    else:
        print(f'WARNING: Per design ignoring {object_key} in {bucket_name} in {lambda_func_name}')

    return 's3://' + bucket_name + '/' + object_key
