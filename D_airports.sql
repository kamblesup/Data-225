CREATE OR REPLACE TABLE
  `db-accidents-group-7.accidents_dataset.D_airports` AS
SELECT
  airport_code,
  airport_id
FROM
  `db-accidents-group-7.staging_dataset.Airports`;