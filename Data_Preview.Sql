
--Data Preview--
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