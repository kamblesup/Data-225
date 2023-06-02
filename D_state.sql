CREATE OR REPLACE TABLE
  `db-accidents-group-7.accidents_dataset.D_state` AS
SELECT
  state_name,
  state_id
FROM
  `db-accidents-group-7.staging_dataset.State`;