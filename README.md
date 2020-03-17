# Sparkify Data Pipeline with Airflow

## Contents

1. [Introduction](#Introduction)
2. [Project Description](#motivation)
3. [Source Data](#Datasets)

## Introduction<a name="installation"></a>

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

## Project Description<a name="motivation"></a>

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Source Data <a name="Datasets"></a>

There are two datasets that reside in S3. Here are the S3 links for each:

Song data: `s3://udacity-dend/song_data`</br>
Log data: `s3://udacity-dend/log_data`</br>
Log data json path: `s3://udacity-dend/log_json_path.json`