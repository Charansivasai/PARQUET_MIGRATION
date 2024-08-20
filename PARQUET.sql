
--Create File Format
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = 'PARQUET';
    COMPRESSION = 'AUTO'
    COLUMN_TRANSFORMATION = 'MATCH_BY_COLUMN_NAME';
	

--Create storage intergration (Amazon S3)
create or replace storage integration HISTORY_DATA_MIGRATION
    type = external_stage
    storage_provider = s3
    enabled = TRUE
    storage_aws_role_arn = '"Amazon Resource Names"'
    storage_allowed_locations = ('"S3 url link"')
    COMMENT ='DONE';
	

--Create a Stage
CREATE OR REPLACE STAGE Charan
STORAGE_INTEGRATION = HISTORY_DATA_MIGRATION
URL ='"S3 url link"';
FILE_FORMAT =PARQUET_FORMAT;

LIST @Charan;



SELECT * FROM @charan/HISTORICAL_DATA/FILE1.parquet
(FILE_FORMAT => PARQUET_FORMAT );

SELECT
$1:"Column_1",
$1:"Column_2",
$1:"Column_3",
$1:"Column_4",
$1:"Column_5",
$1:"Column_6",
$1:"Column_7",
$1:"Column_8",
$1:"Column_9",
$1:"Column_10",
$1:"Column_11",
$1:"Column_12",
$1:"Column_13"
FROM @charan/HISTORICAL_DATA/FILE1.parquet
(FILE_FORMAT => PARQUET_FORMAT );

--Converting necessary column data into the appropriate data type.  
SELECT
$1:"Column_1"::NUMBER(38,0) AS Column_1,
$1:"Column_2"::NUMBER(6,0) AS Column_2,
try_to_number($1:"Column_3"::VARCHAR(100)) AS Column_3,
try_to_number($1:"Column_4"::VARCHAR(100)) AS Column_4,
$1:"Column_5"::VARCHAR(2) AS Column_5,
$1:"Column_6"::VARCHAR(5) AS Column_6,
$1:"Column_7"::VARCHAR(4) AS Column_7,
$1:"Column_8"::TIMESTAMP_NTZ(9) AS Column_8,
$1:"Column_9"::NUMBER(38,0) AS Column_9,
$1:"Column_10"::NUMBER(38,0) AS Column_10,
try_to_number($1:"Column_11"::VARCHAR(100)) AS Column_11,
try_to_number($1:"Column_12"::VARCHAR(100)) AS Column_12,
$1:"Column_13"::VARCHAR(100) AS Column_13
FROM @charan/HISTORICAL_DATA/FILE1.parquet
(FILE_FORMAT => PARQUET_FORMAT );


--Copy Into
COPY INTO DATABASE_NAME.SCHEMA_NAME.TABLE_NAME
FROM (
SELECT
$1:"Column_1"::NUMBER(38,0) AS Column_1,
$1:"Column_2"::NUMBER(6,0) AS Column_2,
try_to_number($1:"Column_3"::VARCHAR(100)) AS Column_3,
try_to_number($1:"Column_4"::VARCHAR(100)) AS Column_4,
$1:"Column_5"::VARCHAR(2) AS Column_5,
$1:"Column_6"::VARCHAR(5) AS Column_6,
$1:"Column_7"::VARCHAR(4) AS Column_7,
$1:"Column_8"::TIMESTAMP_NTZ(9) AS Column_8,
$1:"Column_9"::NUMBER(38,0) AS Column_9,
$1:"Column_10"::NUMBER(38,0) AS Column_10,
try_to_number($1:"Column_11"::VARCHAR(100)) AS Column_11,
try_to_number($1:"Column_12"::VARCHAR(100)) AS Column_12,
try_to_number($1:"Column_13"::VARCHAR(100)) AS Column_13,
FROM @charan/HISTORICAL_DATA/FILE1.parquet
(FILE_FORMAT => PARQUET_FORMAT )
)
FORCE = TRUE;
