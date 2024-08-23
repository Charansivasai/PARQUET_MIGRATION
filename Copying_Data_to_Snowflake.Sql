
	


	




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