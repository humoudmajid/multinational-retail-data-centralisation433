# Multinational Retail Data Centralisation
This project focuses on increasing proficiency in data cleaning and extraction through the application of the Pandas module in Python. Subsequently, the cleaned data is then transferred to a PostgreSQL database. The schema is then developed into a star based schema and it is then queried using PostreSQL to fit different scenarios.
<br/>
## Project Overview
Here you'll find a detailed breakdown of the tasks involved in creating a local PostgreSQL database, uploading data from various sources, processing it, creating a database schema, and running SQL queries.
<br/>
## Main Technologies
Postgres: The primary database management system. <br/>
AWS (s3): For storing and accessing data from the cloud. <br/>
boto3: A Python library for interacting with AWS services. <br/>
rest-API: Utilized for accessing data via RESTful APIs. <br/>
csv: For handling CSV file formats. <br/>
Python (Pandas): For data manipulation and analysis. <br/>
## Project Utils
Data Extraction: Methods for uploading data into pandas data frames from different sources. <br/>
Data Cleaning: Class for cleaning different tables uploaded from various sources. <br/>
Database Connector: Class for initiating the database engine and uploading data into the database. <br/>
Main: Methods for uploading data directly into the local database.
## Step by Step Data Processing 
We have six sources of data, each requiring specific handling: <br/>
1. Remote Postgres database in AWS Cloud: Extracting sales data and user data, cleaning and formatting the data for database insertion. <br/>
2. Public link in AWS cloud: Accessing card details stored as PDFs, extracting relevant information using the tabula package. <br/>
3. AWS S3 Bucket: Downloading product data, converting data types, and cleaning. <br/>
4. Restful-API: Retrieving store details and date-time data, converting the JSON responses into pandas dataframes. <br/>
5. General Data Cleaning Notes: Ensuring data cleanliness and consistency for successful database insertion. <br/>
6. Typical Workflow: Converting data fields, adding foreign and primary keys, and creating additional columns based on conditional data segmentation.
## SQL Queries
Once the data is cleaned and loaded into the database, we can run SQL queries to extract valuable insights: <br/>

Number of stores and their distribution across countries. <br/>
Locations with the most stores. <br/>
Months with the highest sales revenue. <br/>
Sales distribution between online and offline channels. <br/>
Percentage of sales from different store types. <br/>
Monthly sales revenue trends. <br/>
Staff count across different countries. <br/>
Top-selling stores in Germany. <br/>
Time taken for making sales.
