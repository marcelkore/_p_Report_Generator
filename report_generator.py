"""
The script processes data in two separate data stores stores
- PostgreSQL (student data)- This data is hosted in snowflake
- MongoDB (teacher data)- This data is hosted in a mongo db instance
  contained in a docker container.
The application will output a report in json listing each student, the
teacher the student has and the class ID the student is scheduled for.
The script will take as input:
    1. Credentials for connecting to snowflake
    2. Credentials for connecting to mongo
    3. Credentials for the final report location (hosted in AWS S3)
"""
import ast

# import getpass
import json
import sys

import boto3
import botocore
import pandas as pd
import pymongo
import snowflake.connector

data: dict = {}
data["student_info"] = []


def connect_mongo(
    username: str,
    password: str,
) -> pd.DataFrame:
    """This function connects to mongo db, retrieves the collection name
    provided specified in the input parameters and returns the dataframe
    of the collection.
    The collection details are stored in the mongo_creds.csv file for this
    application.
    These would be passed as input paramters to this function (if required)
    :param mong_uri: The mongo uri
    :param username: The username
    :param password: The password
    :param database: The database
    :param collection: The collection
    :return: The dataframe of the results
    """

    database: str = "anubis"
    collection: str = "teachers"

    try:
        # connect to mongodb
        # client = MongoClient(mongo_uri, username=username, password=password)
        client = pymongo.MongoClient(
            f"mongodb+srv://{username}:{password}\
            @anubis-cluster.u6kla.mongodb.net/anubis?retryWrites=true&w=majority"
        )
    # catch pymongo errors
    except pymongo.errors.ServerSelectionTimeoutError as error:
        print("ERROR - CANNOT CONNECT TO MONGO")
        print(error)
        sys.exit()

    # get database and collection
    mydb = client[database]
    mycol = mydb[collection]

    # get data
    mongo_df = pd.DataFrame(list(mycol.find()))

    client.close()
    return mongo_df


def connect_sf(
    username: str,
    password: str,
    url: str,
    query: str,
) -> pd.DataFrame:
    """
    This function takes in as input the credentials for connecting to snowflake
    and a query.  It then connects to snowflake and executes the query.
    This function assumes that the dataset returned is small enough to fit
    in memory.
    :param username: Snowflake username
    :param password: Snowflake password
    :param role: Snowflake role
    :param sf_url: Snowflake URL
    :param query: Snowflake query
    :return: pandas dataframe of results
    """

    database: str = "anubis"
    schema: str = "anubis"
    warehouse: str = "anubis"
    role: str = "anubis"

    # create snowflake connection object
    ctx = snowflake.connector.connect(
        user=username,
        password=password,
        account=url,
        role=role,
        database=database,
        schema=schema,
        warehouse=warehouse,
    )

    # create a cursor object
    cursor = ctx.cursor()

    try:
        # execute sql query
        cursor.execute(query)

        # fetch data from connection in batches
        # we opt to load all the data here as I know the size of the data.
        # If the data is too large, we can use the fetchmany()
        # method to load batches of data
        snowflake_dataframe = cursor.fetch_pandas_all()

        # return dataframe
        return snowflake_dataframe
    except snowflake.connector.errors.ProgrammingError as error:
        # show default error message
        print("ERROR: CANNOT CONNECT TO SNOWFLAKE ACCOUNT")
        print(error)
    finally:
        cursor.close()
        sys.exit()

    ctx.close()


# a function that uploads a file to a secure s3 with username and password
def upload_file_to_s3_secure(
    file_to_upload,
    bucket_name: str,
    object_name: str = None,
    username: str = None,
    password: str = None,
) -> None:
    """
    Upload a file to an S3 bucket
    :param file_to_upload: File to upload
    :param bucket_name: Bucket to upload to
    :param username: username for s3
    :param password: password for s3
    :return: True if file was uploaded, else False
    """

    # Upload the file
    s3_client = boto3.client(
        "s3", aws_access_key_id=username, aws_secret_access_key=password
    )
    try:
        s3_client.put_object(
            Body=json.dumps(file_to_upload),
            Bucket=bucket_name,
            Key=object_name,
        )
    except botocore.exceptions.ClientError as error:
        print("ERROR - CANNOT UPLOAD FILE TO S3")
        print(error)
        sys.exit()


# A function to clean the string
def clean_str(str1: str) -> str:
    """clean the string from ),;,\t and ' as not a wanted data
    :param str1: the string to clean
    :return:The new string with wanted data
    """
    str1 = str1.replace(")", "")
    str1 = str1.replace(";", "")
    str1 = str1.replace("\t", "")
    str1 = str1.strip("'")
    return str1


# A function to convert str into dictionary
def str_to_dict(str1: str) -> dict:
    """
    converting string into dictionary
    :param str1: The string which will be converted
    :return: The dictionary will sent
    """

    return ast.literal_eval(str1)


# a function to get dictionary values
def get_dict_values(dic: dict) -> list:
    """
    geting the dictionary values into the list
    :param dic: The desired dictionary to be converted
    :return :list will be returned with the dictionary values
    """
    return list(dic.values())


# appending student data for json
def append_student_data(
    studentname: str, teachername: str, class_id: str
) -> None:
    """
    Appending student name, teacher name and class id into the json object
    :param studentname:This will be string containing student name
    :param teachername:This will be string containing teacher name
    :param class_id:The class id matching with student and teacher
    :return: None
    """
    data["student_info"].append(
        {"student": studentname, "teacher": teachername, "id": class_id}
    )


# process teacher data to get student class id and assign to teacher
def process_teachers(
    studentname: str,
    clid: dict,
    duplicatecheck: list,
    class_id: str,
    mongo_username: str,
    mongo_password: str,
) -> None:
    """
    Handling teachers data to get students enrolled to which teacher
    :param studentname:This will be string containing student name
    :param clid:This dictionary will be empty and will be used in the if
     statement
    :param duplicatecheck:This dictionary will be empty but will be
    doing appending entries to find the duplicates in clid
    :param class_id:This will be the string and will contain the class id
                                 of the student in which he is enrolled
    :return: It will return None but further call a function to append
                                         the data into the json object
    """

    # get dataframe with teachers data using snowflake function
    teachers_df = connect_mongo(mongo_username, mongo_password)
    teachers_df.drop(columns=["_id"], inplace=True)

    print(teachers_df.head(1))

    # remove duplicate entries
    teachers_df = teachers_df.loc[
        teachers_df.astype(str).drop_duplicates().index
    ]

    # convert dataframe to dictionary
    teacher_dict = teachers_df.to_dict("records")

    for value in teacher_dict:
        clid = value["cid"]
        fname: dict = value["fname"]
        lname: dict = value["lname"]
        duplicatecheck.append(clid)
        val_list = get_dict_values(clid)
        val_list_f = get_dict_values(fname)
        val_list_l = get_dict_values(lname)
        position = val_list.index(class_id)
        teachername = val_list_f[position] + " " + val_list_l[position]
        append_student_data(studentname, teachername, class_id)


# A function to get student data to further identify teachers role
def process_students(
    sf_username: str,
    sf_password: str,
    sf_url: str,
    mongo_username: str,
    mongo_password: str,
) -> None:
    """
    This function will read both the teacher and student data
    using the respective functions
    student data = "connect_sf" function
    teacher data = "connect_mongo" function
    It utilizes  a list (data structure) that is initialized at
    the beginning of this script, and populated with the student +
    teacher data to form dictionary of objects.
    :return: It will return None but will further call the
    function to get the teachers output
    """

    # query to retrieve teachers data from snowflake
    query = " select * from anubis.students;"

    # get dataframe with student data using snowflake function
    students_df = connect_sf(
        username=sf_username,
        password=sf_password,
        query=query,
        url=sf_url,
    )

    print(students_df.head(1))

    student_dict = students_df.to_dict(orient="records")

    for value in student_dict:
        class_id = clean_str(value["CID"])
        fname = clean_str(value["FNAME"])
        lname = clean_str(value["LNAME"])
        studentname = fname + " " + lname
        clid: dict = {}
        duplicatecheck: list = []
        process_teachers(
            studentname,
            clid,
            duplicatecheck,
            class_id,
            mongo_username,
            mongo_password,
        )


# A function to output a json file
def dump_json(
    bucket_name: str,
    json_file_output_name: str,
    access_key_id: str,
    aws_secret_access_key: str,
) -> None:
    """
    This function will output the json file with all the data
                        that has been appended in the json object
    :param: None Not required
    :Return: Not required
    """

    upload_file_to_s3_secure(
        data,
        bucket_name,
        json_file_output_name,
        username=access_key_id,
        password=aws_secret_access_key,
    )


# A Function to run the script
def main() -> None:
    """
    Main Function to take input from the user
    :then calling the Handle_Students Function and calling
    dump_json function to output our desired result
    """

    # print("The credentials below are for the final report location: ")
    # s3_bucket = getpass.getpass("Please enter the s3 bucket name:")
    # s3_access_key_id = getpass.getpass("Please enter the s3 access key id:")
    # s3_secret_key = getpass.getpass("Please enter the s3 secret key:")
    # json_file_output_name = input("Please enter the json file name: ")
    # sf_username = getpass.getpass("Please enter the snowflake username:")
    # sf_password = getpass.getpass("Please enter the snowflake password:")
    # sf_url = getpass.getpass("Please enter the snowflake account URL:")

    print("The credentials below are for the final report location: ")
    s3_bucket = input("Please enter the s3 bucket name:")
    s3_access_key_id = input("Please enter the s3 access key id:")
    s3_secret_key = input("Please enter the s3 secret key:")
    json_file_output_name = input("Please enter the json file name: ")
    sf_username = input("Please enter the snowflake username:")
    sf_password = input("Please enter the snowflake password:")
    sf_url = input("Please enter the snowflake account URL:")
    mongo_username = input("Please enter the mongo username:")
    mongo_password = input("Please enter the mongo password:")

    process_students(
        sf_username, sf_password, sf_url, mongo_username, mongo_password
    )

    dump_json(
        bucket_name=s3_bucket,
        json_file_output_name=json_file_output_name,
        access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_key,
    )


if __name__ == "__main__":
    main()
