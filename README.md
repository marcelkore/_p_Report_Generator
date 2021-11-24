# Report Generator

This python app connects to two data sources and generates a simple report storing
it in an s3 bucket defined by the user.

## Input Parameters

- S3 bucket
- S3 access key id
- S3 secret key
- Name of the JSON file that contains the report

## Data Sources:

- mongo db hosted on a docker container containing student data
- snowflake data warehouse containing teacher information.

The application will use both data stores to output a report in json listing each student,
the teacher the student has and the class ID the student is scheduled for.

The application will provide the user the ability to pick an AWS S3 bucket to store the final
report which will be in json format.

- The code shows the use of type hinting.
- ** UPDATE WHICH LINTING LIBRARY TO USE **
- The code has 50% or more of code coverage with unit tests. Pytest is used for unit tests.
- The code has integration tests.
- CI/CD with Github action workflow is created that
  - runs pylint - this is achieved by running "run_pylint.py" which runs the required tests.
    A threshold value of 9 is set as the default.
  - runs pytest
- Shows the use of github secrets to store credentials
