CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`machine_learning_curated` (
  `timestamp` bigint,
  `x` float,
  `y` float,
  `z` float,
  `sensorreadingtime` bigint,
  `serialnumber` string,
  `distancefromobject` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'FALSE',
    'dots.in.keys' = 'FALSE',
    'case.insensitive' = 'TRUE',
    'mapping' = 'TRUE'
)
         STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://human-analytics/machine_learning_curated/'
TBLPROPERTIES ('classification' = 'json');