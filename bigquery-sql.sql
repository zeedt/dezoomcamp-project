-- Create or replace a partitioned and clustered table
CREATE OR REPLACE TABLE dezoomcamp.incidents_partitioned_clustered
PARTITION BY DATE_TRUNC(date_SIF, YEAR)
CLUSTER BY drug_involvement_int, state 
AS
SELECT *, CAST(drug_involvement as INT) as drug_involvement_int
 FROM `dtc-de.dezoomcamp.incidents`;