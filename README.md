Data Engineer Technical Test

Task Overview
This task will consist of two parts.
In the first task you will be required to, using Python or any other programming language of your choice, to write a small
application which will extract data from the ‘Spotify Dataset 2023’ dataset in the Kaggle API, clean and transform it, then
load the data into a database, and finally perform some basic data visualisations.
In the second, you will integrate your solution from the first task into a data pipeline diagram using GCP cloud architecture,
explaining your choices for each step of the pipeline.

Task Details

Task 1
1. Extract the ‘spotify-albums_data_2023.csv’ and ‘spotify_tracks_data_2023.csv’ files from the Kaggle API.
2. Clean the first of the 2 data files. Remove any unnecessary columns, null values, anything you deem unnecessary.
3. Transform the data to a final state which should adhere to the following rules:
a. The data has a new column called ‘radio_mix’ where any song, equal to or shorter than 3 minutes, falls
true in this category and all others are false.
b. The data contains all ‘non-explicit’ songs which have a ‘track_popularity’ value greater than 50%.
4. Load the data into a database. Use MySQL/SQLite or any other database tool for this.
5. With the stored transformed data, display the following:
a. The top 20 ‘labels’ against their total number of ‘tracks’.
b. The top 25 popular ‘tracks’ released between 2020 and 2023.

Task 2
1. Design a cloud architecture flowchart for your ETL solution from task 1. Use GCP tools for your solution,
explaining its purpose and the rationale behind your choices. Ensure your pipeline covers all aspect of the data
journey (Extraction, Cleansing/Validation, Transformation, Loading, Visualisation, and Monitoring/Management).
Note, you will not need to use GCP for this task, only design an architecture utilising GCP tools.
Requirements
1. Use the Kaggle API for data extraction. The dataset to be used is called ‘Spotify Dataset 2023’.
2. Ensure your coded solution adheres to software engineering best practices. Structure your solution inside a
repository (not in a notebook) and save it in GitHub.
3. Write a few functioning unit tests for your code (You don’t have to write tests for all parts of your code, unless
you want to).
4. Ensure your solutions are scalable and efficient.
5. Use GCP tools for the 2 nd task (You may use some 3 rd party tools. If you do, explain the rationale behind your
choice).


During the interview, you will be expected to present and discuss your work. Please be prepared to talk through your
processes, address any questions or concerns, and demonstrate your knowledge of the API and GCP.
To help you prepare for the task, we recommend familiarizing yourself with the following GCP components:
1. BigQuery: Data warehouse for large-scale data analytics.
2. Dataflow: A fully managed service for real-time and batch data processing.
3. Cloud Storage: Object storage for storing and serving user-generated content.
4. Pub/Sub: A messaging service for sending and receiving messages between independent applications.
5. Looker Studio: A dashboard and reporting tool for creating visualizations and reports.
We look forward to seeing your solution and discussing it with you during the interview.
