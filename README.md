### dezoomcamp Project

This project was developed using public dataset provided by DASIL (Data Analysis & Social Enquiry Lab). The dataset used is incidents record from 2000 to 2015. However, 2010 and 2013 data were excluded for this project because of inconsistency compared to other year datasets.

The data can be downloaded [here](https://dasil.grinnell.edu/DataRepository/NIBRS/IncidentLevelSTATA.zip). The Zip file contains dta files for each year.


-----

##### Tools used

1. Terraform
2. Airflow
3. Dbt
4. Gcp
5. Docker
6. BigQuery data warehouse
7. Data Studio


-----

##### Steps

1. Terraform was used to set up VM instance needed for this project. The terraform configuration files can be found in the `terraform` folder of this repository.

2. Airflow was set up on GCP virtual machine. Airflow set up is documented on Airflow website.

3. Airflow dag was created to download the data, unzip, convert each dta file to parquet (using pandas), published parquet files to GCS bucket and also create an external table on Bigquery data warehouse with the parquet files. The dag file is in the airflow folder of this repository.

4. Table was partitioned (using incident time (year specifically)) and clustered (drug_involvement_int, state) on Bigquery. The query for this partitioning and clustering can be found in `big-query-sql.sql` file.

5. Data was transformed with Dbt and two seperate tables were created. One of the tables created incidents summary per year while the other created incidents summary per state. The dbt project for this can be found in `dezoomcamp-project` folder in this repository

6. Data was visualized using Data Studio.


Below is the Dbt lineage graph

![Dbt Lineage Graph](./images/lineage_graph.png)


-----

This is the [link](https://datastudio.google.com/reporting/d40abf3d-bd3a-4320-915f-da7024c2afca) to the report dashboard.

