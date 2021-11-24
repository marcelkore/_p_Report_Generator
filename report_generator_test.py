"""
This script is used to test the report generator script.

"""
import json
import os

from report_generator import (
    clean_str,
    connect_mongo,
    connect_sf,
    get_dict_values,
    upload_file_to_s3_secure,
)

# mongo creds

mng_username = os.environ["mongo_username"]
mng_password = os.environ["mongo_password"]

# Access credentials stored in github secrets
bucket_name = os.environ["B"]
FILE_NAME = "TEST_FILE.json"
access_key_id = os.environ["access_key_id"]
aws_secret_key = os.environ["access_secret_key"]

# snowflake credentials
sf_username = os.environ["sf_username"]
sf_password = os.environ["sf_password"]
sf_url = os.environ["sf_url"]


def test_clean_str() -> None:
    """
    Test the clean_str function.
    """
    assert clean_str("single)") == "single"
    assert clean_str("single;") == "single"
    assert clean_str("single'") == "single"
    assert clean_str("a") == "a"


def test_get_dict_values() -> None:
    """
    Test the str_to_dict function.

    """
    array_val = {"0": "Jessa", "1": "Tate"}

    assert get_dict_values(array_val) == ["Jessa", "Tate"]


def test_connect_mongodb() -> None:
    """
    Checks that we can connect to mongodb.
    """
    # call mongo connect function
    data_frame = connect_mongo(mng_username, mng_password)

    # check that  a dataframe is returned
    assert data_frame.empty is False


def test_connect_sf() -> None:
    """

    Testing to see if we can successfully connect to snowflake. We do
    this by checking that the current role is the same as the one we provide.

    """

    query = "select current_user()"

    data_frame = connect_sf(
        username=sf_username,
        password=sf_password,
        url=sf_url,
        query=query,
    )

    assert data_frame["CURRENT_USER()"][0] == sf_username.upper()


def test_upload_file_to_s3_secure() -> None:

    """
    Checks to see if we can upload a file to s3. i.e., connection
    to s3 is successful.
    """

    # create a test file
    json_file = {"name": "Jessa"}
    json_file_object = json.dumps(json_file)

    # upload the test file to s3
    upload_file_to_s3_secure(
        json_file_object,
        bucket_name,
        "json_file.json",
        access_key_id,
        aws_secret_key,
    )
