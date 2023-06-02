CREATE OR REPLACE TABLE
  `db-accidents-group-7.accidents_dataset.D_severity` AS
SELECT
  Severity,
  Id
FROM
  `db-accidents-group-7.staging_dataset.Severity`; 