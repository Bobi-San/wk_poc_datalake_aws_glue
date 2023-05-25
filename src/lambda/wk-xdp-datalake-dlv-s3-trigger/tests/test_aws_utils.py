import os
import datetime
import boto3
from scripts import aws_utils
import unittest
from unittest.mock import MagicMock, patch
from freezegun import freeze_time


# -----------------------------------------------------------------------------
class TestAWSUtils(unittest.TestCase):

    @patch.dict(os.environ, {'AWS_REGION': 'ThisValue', 'AWS_DEFAULT_REGION': ''})
    def test_get_current_region_name_os_env1_ok(self):
        expected = 'ThisValue'
        with patch('boto3.DEFAULT_SESSION') as boto3_def_ses, \
                patch('boto3.Session') as boto3_session:
            mock_bt3_def_ses = MagicMock()
            mock_bt3_session = MagicMock()
            bt3_def_ses_mock = boto3.DEFAULT_SESSION
            bt3_session_mock = boto3.Session()
            bt3_def_ses_mock.return_value = mock_bt3_def_ses
            bt3_session_mock.return_value = mock_bt3_session
            bt3_def_ses_mock.region_name = None
            bt3_session_mock.region_name = None
            self.assertEqual(expected, aws_utils.get_current_region_name())

    @patch.dict(os.environ, {'AWS_REGION': '', 'AWS_DEFAULT_REGION': 'ThisValue'})
    def test_get_current_region_name_os_env2_ok(self):
        expected = 'ThisValue'
        with patch('boto3.DEFAULT_SESSION') as boto3_def_ses, \
                patch('boto3.Session') as boto3_session:
            mock_bt3_def_ses = MagicMock()
            mock_bt3_session = MagicMock()
            bt3_def_ses_mock = boto3.DEFAULT_SESSION
            bt3_session_mock = boto3.Session()
            bt3_def_ses_mock.return_value = mock_bt3_def_ses
            bt3_session_mock.return_value = mock_bt3_session
            bt3_def_ses_mock.region_name = None
            bt3_session_mock.region_name = None
            self.assertEqual(expected, aws_utils.get_current_region_name())

    @patch.dict(os.environ, {'AWS_REGION': '', 'AWS_DEFAULT_REGION': ''})
    def test_get_current_region_name_bt3_def_ses(self):
        expected = 'ThisValue'
        with patch('boto3.DEFAULT_SESSION') as boto3_def_ses, \
                patch('boto3.Session') as boto3_session:
            mock_bt3_def_ses = MagicMock()
            mock_bt3_session = MagicMock()
            bt3_def_ses_mock = boto3.DEFAULT_SESSION
            bt3_session_mock = boto3.Session()
            bt3_def_ses_mock.return_value = mock_bt3_def_ses
            bt3_session_mock.return_value = mock_bt3_session
            bt3_def_ses_mock.region_name = 'ThisValue'
            bt3_session_mock.region_name = None
            self.assertEqual(expected, aws_utils.get_current_region_name())

    @patch.dict(os.environ, {'AWS_REGION': '', 'AWS_DEFAULT_REGION': ''})
    def test_get_current_region_name_bt3_session(self):
        expected = 'ThisValue'
        with patch('boto3.DEFAULT_SESSION') as boto3_def_ses, \
                patch('boto3.Session') as boto3_session:
            mock_bt3_def_ses = MagicMock()
            mock_bt3_session = MagicMock()
            bt3_def_ses_mock = boto3.DEFAULT_SESSION
            bt3_session_mock = boto3.Session()
            bt3_def_ses_mock.return_value = mock_bt3_def_ses
            bt3_session_mock.return_value = mock_bt3_session
            bt3_def_ses_mock.region_name = None
            bt3_session_mock.region_name = 'ThisValue'
            self.assertEqual(expected, aws_utils.get_current_region_name())

    @patch.dict(os.environ, {'AWS_REGION': '', 'AWS_DEFAULT_REGION': ''})
    def test_get_current_region_name_none(self):
        expected = None
        with patch('boto3.DEFAULT_SESSION') as boto3_def_ses, \
                patch('boto3.Session') as boto3_session:
            mock_bt3_def_ses = MagicMock()
            mock_bt3_session = MagicMock()
            bt3_def_ses_mock = boto3.DEFAULT_SESSION
            bt3_session_mock = boto3.Session()
            bt3_def_ses_mock.return_value = mock_bt3_def_ses
            bt3_session_mock.return_value = mock_bt3_session
            bt3_def_ses_mock.region_name = None
            bt3_session_mock.region_name = None
            self.assertEqual(expected, aws_utils.get_current_region_name())

    @patch.dict(os.environ, {'AWS_LAMBDA_FUNCTION_NAME': 'ThisValue'})
    def test_get_current_lambda_function_name_os_env_ok(self):
        expected = 'ThisValue'
        self.assertEqual(expected, aws_utils.get_current_lambda_function_name())

    @patch.dict(os.environ, {'AWS_LAMBDA_FUNCTION_NAME': 'NotThisValue'})
    def test_get_current_lambda_function_name_os_env_nok(self):
        expected = 'ThisValue'
        self.assertNotEqual(expected, aws_utils.get_current_lambda_function_name())

    def test_prefix_object_name_default(self):
        expected = 'ThisValue.txt'
        object_name = 'ThisValue.txt'
        self.assertEqual(expected, aws_utils.prefix_object_name(object_name))

    @freeze_time("2023-05-03 08:54:17")
    def test_freeze_time_ok(self):
        # Double check freeze_time works ok, see also https://pypi.org/project/freezegun/
        assert aws_utils.datetime.now() == datetime.datetime(2023, 5, 3, 8, 54, 17)

    @freeze_time("2023-05-03 08:54:17")
    def test_prefix_object_name_date_ok(self):
        expected = '20230503_name.txt'
        object_name= 'name.txt'
        self.assertEqual(expected, aws_utils.prefix_object_name(object_name, iso_dt=True))

    @freeze_time("2023-05-03 08:54:17")
    def test_prefix_object_name_time_ok(self):
        expected = '085417_name.txt'
        object_name= 'name.txt'
        self.assertEqual(expected, aws_utils.prefix_object_name(object_name, iso_tm=True))

    @patch('scripts.aws_utils.uuid.uuid4', return_value="abcd-1234")
    def test_prefix_object_name_uuid_ok(self, mock_uuid4):
        expected = 'abcd-1234_name.txt'
        object_name= 'name.txt'
        self.assertEqual(expected, aws_utils.prefix_object_name(object_name, uu_id=True))

    @freeze_time("2023-05-03 08:54:17")
    @patch('scripts.aws_utils.uuid.uuid4', return_value="abcd-1234")
    def test_prefix_object_name_uuid_ok(self, mock_uuid4):
        expected = '20230503#085417#abcd-1234#name.txt'
        object_name = 'name.txt'
        self.assertEqual(expected, aws_utils.prefix_object_name(object_name, sep='#', iso_dt=True, iso_tm=True, uu_id=True))

    def test_exec_func_with_max_retries_one_ok(self):
        expected = 0
        result = aws_utils.exec_func_with_max_retries(lambda: print('Forza Lazio'))
        self.assertEqual(expected, result)

    def test_exec_func_with_max_retries_max_ok(self):
        def no_working_code():
            raise ValueError
        self.assertRaises(ValueError,
                          aws_utils.exec_func_with_max_retries,
                          lambda: no_working_code(),
                          sleep_sec=1)

    def test_exec_func_with_max_retries_two_ok(self):
        expected = 2
        global_nr = 0
        def no_working_code():
            nonlocal global_nr
            global_nr += 1
            if global_nr < 3:
                raise ValueError
        retries = aws_utils.exec_func_with_max_retries(
            lambda: no_working_code(),
            max_tries=4,
            sleep_sec=1)
        self.assertEqual(expected, retries)

    def test_move_s3_key_from_to_location(self):
        mock_s3_client = MagicMock()
        mock_s3_object = MagicMock()
        mock_s3_client.Object.return_value = mock_s3_object

        with patch('boto3.client') as boto3_client:
            boto3_client.return_value = mock_s3_client
            aws_utils.move_s3_key_from_to_location('bucket_name', 'object_key_from', 'object_key_to')
            boto3_client.assert_called_once_with('s3')
            mock_s3_client.copy_object.assert_called_once()
            mock_s3_client.delete_object.assert_called_once()

    @patch('boto3.client')
    def test_get_s3_key_tag(self, mock_boto):
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        # Mock the get_object_tagging method
        mock_get_object_tagging = MagicMock(return_value={
            'TagSet': [ {'Key': 'TagKey1', 'Value': 'TagVal1'},
                        {'Key': 'TagKey2', 'Value': 'TagVal2'},
                        {'Key': 'TagKey3', 'Value': 'TagVal3'},
                        ]})
        mock_s3.get_object_tagging = mock_get_object_tagging

        # Call the function to be tested
        result = aws_utils.get_s3_key_tag('bucket_name', 'object_key', 'TagKey2')

        # Assertions
        expected = 'TagVal2'
        mock_boto.assert_called_once_with('s3')
        mock_get_object_tagging.assert_called_once_with(Bucket='bucket_name', Key='object_key')
        self.assertEqual(expected, result)

    @patch('boto3.client')
    def test_put_s3_key_tag_no_if_tag_value(self, mock_boto):
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        # Mock the get_object_tagging method
        mock_get_object_tagging = MagicMock(return_value={
            'TagSet': [
                {'Key': 'TagKey1', 'Value': 'TagVal1'},
                {'Key': 'TagKey2', 'Value': 'TagVal2'},
                {'Key': 'TagKey3', 'Value': 'TagVal3'},
            ]})
        mock_s3.get_object_tagging = mock_get_object_tagging

        # Mock the put_object_tagging method
        mock_put_object_tagging = MagicMock()
        mock_s3.put_object_tagging = mock_put_object_tagging

        # Call the function to be tested
        aws_utils.put_s3_key_tag('bucket_name', 'object_key', 'TagKey2', 'NewValue', if_tag_value=None)

        # Assertions
        expected = {
            'TagSet': [
                {'Key': 'TagKey1', 'Value': 'TagVal1'},
                {'Key': 'TagKey2', 'Value': 'NewValue'},
                {'Key': 'TagKey3', 'Value': 'TagVal3'},
            ]}
        mock_boto.assert_called_once_with('s3')
        mock_put_object_tagging.assert_called_once_with(
            Bucket='bucket_name',
            Key='object_key',
            Tagging=expected
        )

    @patch('boto3.client')
    def test_put_s3_key_tag_with_if_tag_value_found(self, mock_boto):
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        # Mock the get_object_tagging method
        mock_get_object_tagging = MagicMock(return_value={
            'TagSet': [
                {'Key': 'TagKey1', 'Value': 'TagVal1'},
                {'Key': 'TagKey2', 'Value': 'TagVal2'},
                {'Key': 'TagKey3', 'Value': 'TagVal3'},
            ]})
        mock_s3.get_object_tagging = mock_get_object_tagging

        # Mock the put_object_tagging method
        mock_put_object_tagging = MagicMock()
        mock_s3.put_object_tagging = mock_put_object_tagging

        # Call the function to be tested
        aws_utils.put_s3_key_tag('bucket_name', 'object_key', 'TagKey2', 'NewValue', if_tag_value='TagVal2')

        # Assertions
        expected = {
            'TagSet': [
                {'Key': 'TagKey1', 'Value': 'TagVal1'},
                {'Key': 'TagKey2', 'Value': 'NewValue'},
                {'Key': 'TagKey3', 'Value': 'TagVal3'},
            ]}
        mock_boto.assert_called_with('s3')
        mock_put_object_tagging.assert_called_once_with(
            Bucket='bucket_name',
            Key='object_key',
            Tagging=expected
        )

    @patch('boto3.client')
    def test_put_s3_key_tag_with_if_tag_value_not_found(self, mock_boto):
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        # Mock the get_object_tagging method
        mock_get_object_tagging = MagicMock(return_value={
            'TagSet': [
                {'Key': 'TagKey1', 'Value': 'TagVal1'},
                {'Key': 'TagKey2', 'Value': 'TagVal2'},
                {'Key': 'TagKey3', 'Value': 'TagVal3'},
            ]})
        mock_s3.get_object_tagging = mock_get_object_tagging

        # Mock the put_object_tagging method
        mock_put_object_tagging = MagicMock()
        mock_s3.put_object_tagging = mock_put_object_tagging

        # Call the function to be tested
        aws_utils.put_s3_key_tag('bucket_name', 'object_key', 'TagKey2', 'NewValue', if_tag_value='NotFound')

        # Assertions
        mock_boto.assert_called_with('s3')
        mock_put_object_tagging.assert_not_called()
