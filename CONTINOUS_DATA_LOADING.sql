-- =============================================================================
-- CONTINUOUS DATA LOADING PIPELINE: AWS S3 -> Snowflake via Snowpipe
-- =============================================================================
-- S3 Source  : s3://ajay-awss3-snowflake-sns-bucket1/sourcefolder/CUSTOMERDATA.csv
-- External ID: 1234567890
-- IAM Role   : arn:aws:iam::654654234202:role/DATA_INGESTION_USING_SNOWPIPE
-- =============================================================================


-- =============================================================================
-- STEP 1: Set up the environment (role, warehouse, database, schema)
-- =============================================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE DATABASE AWSS3_SNOWFLAKE_DATALOADING;
CREATE OR REPLACE SCHEMA AWSS3_SNOWFLAKE_DATALOADING.RAW;
DROP SCHEMA IF EXISTS AWSS3_SNOWFLAKE_DATALOADING.PUBLIC;


-- =============================================================================
-- STEP 2: Create the target table for customer data
-- =============================================================================
CREATE OR REPLACE TABLE AWSS3_SNOWFLAKE_DATALOADING.RAW.CUSTOMER_LOAD (
    ID           NUMBER(10,0),
    NAME         VARCHAR(100),
    AGE          NUMBER(3,0),
    ADDRESS      VARCHAR(255),
    PHONE_NUMBER VARCHAR(20),
    SALARY       NUMBER(12,2),
    RUNBY        VARCHAR(50)
);


-- =============================================================================
-- STEP 3: Create a storage integration to securely connect to the S3 bucket
-- =============================================================================
CREATE OR REPLACE STORAGE INTEGRATION AJAY_S3_SNOWFLAKE_SNOWPIPE_STORAGE_INTEGRATION
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = S3
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::654654234202:role/DATA_INGESTION_USING_SNOWPIPE'
    STORAGE_ALLOWED_LOCATIONS = ('s3://ajay-awss3-snowflake-sns-bucket1/sourcefolder/');

-- Verify the integration and retrieve STORAGE_AWS_IAM_USER_ARN & STORAGE_AWS_EXTERNAL_ID
-- (needed to configure the AWS IAM trust policy)
DESC STORAGE INTEGRATION AJAY_S3_SNOWFLAKE_SNOWPIPE_STORAGE_INTEGRATION;


-- =============================================================================
-- STEP 4: Create a CSV file format
-- =============================================================================
CREATE OR REPLACE FILE FORMAT AWSS3_SNOWFLAKE_DATALOADING.RAW.FF_CSV
    TYPE                      = CSV
    SKIP_HEADER               = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';


-- =============================================================================
-- STEP 5: Create an external stage pointing to the S3 bucket
-- =============================================================================
CREATE OR REPLACE STAGE AWSS3_SNOWFLAKE_DATALOADING.RAW.AWS_S3_SNOWFLAKE_STAGE
    URL                = 's3://ajay-awss3-snowflake-sns-bucket1/sourcefolder/'
    FILE_FORMAT        = AWSS3_SNOWFLAKE_DATALOADING.RAW.FF_CSV
    STORAGE_INTEGRATION = AJAY_S3_SNOWFLAKE_SNOWPIPE_STORAGE_INTEGRATION;


-- =============================================================================
-- STEP 6: Verify the stage can list files from S3
-- =============================================================================
LIST @AWSS3_SNOWFLAKE_DATALOADING.RAW.AWS_S3_SNOWFLAKE_STAGE;


-- =============================================================================
-- STEP 7: One-time bulk load (use before enabling Snowpipe for historical data)
-- =============================================================================
COPY INTO AWSS3_SNOWFLAKE_DATALOADING.RAW.CUSTOMER_LOAD
FROM @AWSS3_SNOWFLAKE_DATALOADING.RAW.AWS_S3_SNOWFLAKE_STAGE;


-- Verify loaded data
SELECT * FROM AWSS3_SNOWFLAKE_DATALOADING.RAW.CUSTOMER_LOAD



-- =============================================================================
-- STEP 8: Create Snowpipe for continuous auto-ingest from S3
-- (Requires an S3 event notification -> SQS queue configured in AWS)
-- =============================================================================
CREATE OR REPLACE PIPE AWSS3_SNOWFLAKE_DATALOADING.RAW.CUSTOMER_LOAD_PIPE
    AUTO_INGEST = TRUE
AS
    COPY INTO AWSS3_SNOWFLAKE_DATALOADING.RAW.CUSTOMER_LOAD
    FROM @AWSS3_SNOWFLAKE_DATALOADING.RAW.AWS_S3_SNOWFLAKE_STAGE;

-- List all pipes to confirm creation and get the SQS notification channel ARN
SHOW PIPES;


-- =============================================================================
-- STEP 9: Monitor and validate the Snowpipe
-- =============================================================================

-- Check row count
SELECT COUNT(*) FROM AWSS3_SNOWFLAKE_DATALOADING.RAW.CUSTOMER_LOAD;

-- Check current pipe status (pending files, last ingested timestamp, etc.)
SELECT SYSTEM$PIPE_STATUS('AWSS3_SNOWFLAKE_DATALOADING.RAW.CUSTOMER_LOAD_PIPE');

-- Review copy history for this pipe (file-level load details)
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE PIPE_NAME = 'CUSTOMER_LOAD_PIPE';

-- Check credits consumed by Snowpipe (last 30 days)
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE PIPE_NAME = 'CUSTOMER_LOAD_PIPE'
ORDER BY START_TIME DESC;

  