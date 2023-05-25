"""
AWS S3 Triggers
"""

import boto3
from scripts import aws_utils


def validation_incoming_source_delivery(bucket_name, object_key):
    """
    Simple validations for the incoming source (raw data) deliveries
    :param bucket_name: Bucket name
    :param object_key: Prefix key
    :return: -
    """

    # Init AWS common objects
    region_name = aws_utils.get_current_region_name()

    # Init AWS clients
    ssm_client = boto3.client('ssm', region_name=region_name)  # Simple System Manager

    # Get valid list of incoming sources id from Simple System Manager
    valid_source_id_list = ssm_client.get_parameter(
        Name='/datalake/bronze/source_id-list')['Parameter']['Value']
    object_prefixes_list = object_key.split('/')  # Split in prefixes path
    object_prefixes_count = len(object_prefixes_list)

    # Check prefixes parts, expected at least Medallion/SubFolder/SourceId/FimeName.json
    # TODO: The below may need to create a metric to set off an alarm # pylint: disable=W0511
    if object_prefixes_count < 4:
        raise Exception(f"ERROR: Invalid object prefixes path,"
                        f" expected 4 actual {object_prefixes_count}")

    source_id = object_prefixes_list[-2]  # Skip "filename" and get last sub-folder prefix
    object_name = object_prefixes_list[-1]  # Get "filename"
    root_prefix = '/'.join(object_prefixes_list[:object_prefixes_count-3])
    print("OBJECT JSON:", object_name, " SOURCE ID:", source_id)

    # Derive the next location where to move it
    if len(object_key) > 4 and object_key[-5:] == '.json' \
            and source_id in valid_source_id_list:
        raise_exception_flg = False
        object_ext = object_key[-4:]
        object_tag_value = 'PendingSelection'  # TODO: Maybe get it from SSM # pylint: disable=W0511
        s3_move_to_location = root_prefix + '/' + object_tag_value + '/' + source_id + '/' \
                            + aws_utils.prefix_object_name(object_name, iso_dt=True, iso_tm=True)
        print("OBJECT EXT:", object_ext)
    else:
        raise_exception_flg = True
        object_tag_value = 'Rejected'  # TODO: Maybe get it from SSM # pylint: disable=W0511
        if source_id not in valid_source_id_list:
            s3_move_to_location = root_prefix + '/' + object_tag_value + '/' + '/' \
                                + aws_utils.prefix_object_name(
                                    object_name, iso_dt=True, iso_tm=True, uu_id=True)
        else:
            s3_move_to_location = root_prefix + '/' + object_tag_value + '/' + source_id + '/' \
                                + aws_utils.prefix_object_name(
                                    object_name, iso_dt=True, iso_tm=True)

    # Move file to next location
    print("OBJECT MOVETO:", s3_move_to_location)
    new_object_tag_value = 'ProcessStatus'  # TODO: Maybe get it from SSM # pylint: disable=W0511
    aws_utils.put_s3_key_tag(bucket_name, object_key, new_object_tag_value, object_tag_value)
    if 'PlaceHolder' not in object_key:  # TODO: To remove later # pylint: disable=W0511
        aws_utils.exec_func_with_max_retries(
            lambda: aws_utils.move_s3_key_from_to_location(
                bucket_name, object_key, s3_move_to_location),
            func_text=f"Moving from {bucket_name} {object_key} to {s3_move_to_location}"
        )
    else:
        print(f'WARNING: TEST not moved from {bucket_name} {object_key} to {s3_move_to_location}')  # TODO: To remove later # pylint: disable=W0511

    # TODO: The below may need to create a metric to set off an alarm # pylint: disable=W0511
    # TODO: Information from the Metadata may be collected for statistics # pylint: disable=W0511
    if raise_exception_flg:
        raise Exception(f"ERROR: Invalid SourceId or Extension, moved to {s3_move_to_location}")
