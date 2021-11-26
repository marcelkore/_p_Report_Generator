# Report Generator

This python app connects to two data sources and generates a simple report storing
it in an s3 bucket defined by the user.

## Input Parameters

- Credentials for:
  - AWS S3 Bucket Name
  - AWS Access Key Id
  - AWS Secret Key
  - Mongo Cloud Username and Password
  - Snowflake DW Username and Password
  - Name of the JSON file that contains the report

## Data Sources:

- mongo db hosted on a mongodb.cloud container containing student data
- snowflake data warehouse containing teacher information.

## About Report Generator:

The application will use both data stores to generate a report in json listing each student,
the teacher the student has and the class ID the student is scheduled for.

The application will provide the user the ability to pick an AWS S3 bucket to store the final
report which will be in json format.

- The code shows the use of type hinting.
- The code has 50% or more of code coverage with unit tests. Pytest is used.
- The code has integration tests with the
- CI with Github action workflow is created that
  - runs pylint
  - runs pytest
- Shows the use of github secrets to store credentials
