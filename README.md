## Problem Statement

Earthquakes are an everlasting occurrence that have the capacity for colossal damage in societal infrastructure which directly results in both lives and economical loss. Seismic events with a magnitude of 5.5-6.0 are considered to be moderate, but still have the capabilities of wrecking havoc in heavily populated areas. According to NOAA, there were 17 earthquakes of the aforementioned magnitude that resulted in an average economic damage of 2.1 billion USD and a median damage of 529 million USD. Preventative measures must to taken to combat these occurrences, but the scientific equipment currently available is not precise enough to measure the necessary factors for earthquake predictions. That being said, earthquake early warnings, forecasting, and probability calculations are still possible and can be relatively accurate. The goal of this project is to build an end to end pipeline for extracting and cleaning data gathered from USGS for the analyzation and assessment of possible patterns in earthquake events.

## Table of Contents

- [Workflow Overview](<Workflow Overview>)
- [Dataset](#Dataset)
- [Project Re-Creation](<Project Re-Creation>)
- [Dashboard](#Dashboard)
- [Next Steps & Final Thoughts](<Next Steps & Final Thoughts>)
- [References](#References)
## Workflow Overview

The diagram below illustrates the complete workflow, beginning with the ingestion of raw data and culminating in the derivation of insights displayed on dashboards, aimed at discovering earthquake patterns.
##### Technologies illustrated in Diagram:

- **Cloud Platform:** Google Cloud Platform (GCP)
- **Infrastructure as Code (IaC):** Terraform
- **Workflow Orchestration:**** Mage
- **Data Warehouse:** BigQuery
- **Batch Processing:** Dataproc (Spark)
- **Metadata Storage:** Cloud Filestore
- **Compute services:** Cloud Run 

[Link to Diagram](https://googlecloudcheatsheet.withgoogle.com/architecture?link=ujDpHYjxeUBrVfdwUzWHRihFDPRHBMuEWmaNvhHLITWJcJKzJsRPeJbpTqWPUlWA)

![](images/20240424142712.png?raw=true)
## Dataset

The dataset was taken from ANSS ComCat server using functions taken from USGS libcomcat. 
[Link to Dataset](https://code.usgs.gov/ghsc/esi/libcomcat-python)
## Project Re-Creation

1. Clone the Github repository a directory for replicating this project.
2. Install terraform from the official HashiCorp website
3. This project requires a GCP account - Create a GCP project as well as 2 different service accounts for Mage and Cloud Functions
	1. The first service account should include these Permissions:
		1. Artifact Registry Read
		2. Artifact Registry Writer
		3. Cloud Run Developer
		4. Cloud SQL
		5. Service Account Token Creator 
	2. The second service account should include read and write permissions for buckets.
5. Download the service account credential key for the first service account. 
	1. Either create an env variable or copy & paste the contents of the json file in the *io_config.yaml* file under: 
	  ```GOOGLE_SERVICE_ACC_KEY: #include json credential key here```
6. Configure the variables.tf file by adding values to *app_name* and *project_id* (GCP)
	1. ```default     = #add app_name```
	2. ```default     = #add project id```
7. Configure the *metadata.yaml* file in both pipelines within the pipeline folder.
	1. *gcs_to_bigquery_spark*
		```
	      variables:
			  cluster_name: #name of cluster
			  project_id: #gcp project id
			  region: #region
		```
1. Initialize Google Cloud using the command.
	1. Download and install the gcloud CLI
	2. Activate the command ```gcloud init```
2. Change directory to the gcp folder and activate terraform.
	1. ```terraform init```
	2. ```terraform plan```
	3. ```terraform apply```
3. Go to Cloud Run in GCP and open up the Mage Instance
	1. Ensure that everyone can access the instance in the Network tab.
4. Git clone the repo into the working directory
5. Triggers & Data Loader parameters in *usgs_raw_data_to_gcs* pipeline if frequency and time period need to be changed.
	1. Default runs are set to a monthly batch load
6. Running the *usgs_raw_data_to_gcs* pipeline will automatically trigger the entire workflow.
7. Clean and Transformed Data will be directly ingested into BigQuery in the form of:
	1. *eq_events.eq_dataset*
	2. *eq_events.eq_daily*
	3. *eq_events.eq_weekly*
	4. *eq_events.eq_monthly*
8. Data can be sourced directly from BigQuery into Looker Studio for dashboard creation and data analysis. 
9. Look studio will automatically refresh when new data comes in monthly. 
10. When everything is finished the project can be torn down with:
	1.  ```terraform destroy```
## Dashboard

This dashboard presents statistics pertaining to the magnitude, depth, significance, and frequency of earthquakes, focusing primarily on terrestrial occurrences. The dataset spans from January 1989 to March 2024, with the flexibility to adjust the time period within this range. The second page explores the interrelationships among these features and includes a heatmap for visualizing earthquake occurrences.

![](images/20240424021600.png?raw=true)
![](images/20240424021629.png?raw=true)

To interact with the dashboard, click on the link:
[Link to Dashboard](https://lookerstudio.google.com/s/hCM172ps3Mw)
## Next Steps & Final Thoughts

- CI/CD will be implemented using Cloud Build to ensure that changes to the pipeline will be systematically implemented when pushed to github.
- USGS also provides data regarding different types of magnitude as well as additional characteristics of earthquakes which may add to patterns worth exploring.
- Adding a streaming data source and ingestion method to this pipeline to ensure maximum data freshness. 

The Data Engineering Zoomcamp course is an extremely well thought-out program that teaches all that are willing to learn a fairly robust stack of technologies as well as important concepts related to data engineering. Both the contributors and community at Data Talks Club have been extremely generous with their time, knowledge, and resources and I just wanted to thank them for all that they have done. Onwards!

## References

1. [Earthquake Stats](https://www.kansascityfed.org/oklahomacity/oklahoma-economist/2016q1-economic-damage-large-earthquakes/#:~:text=The%20average%20economic%20damage%20was,fell%20into%20three%20general%20groupings)
2. [Data Engineering Zoomcamp Github](https://github.com/DataTalksClub/data-engineering-zoomcamp)
3. [DataTalks.Club](https://datatalks.club/)
4. [Link to Dataset](https://code.usgs.gov/ghsc/esi/libcomcat-python)
5. [Link to Project Repo](https://github.com/hguber/de-eq-asmnt-2024)
