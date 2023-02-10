# UFC Data Project

## Problem:
The UFC community lacks a resource that lets people query specific athlete data.

## Goal:
Create an front-end that lets users query specific data from all relevant athletes or compare athletes for a more visual experience. 

## Overview:
![ufc-diagram](https://user-images.githubusercontent.com/23240195/218166935-f794ad91-e1d7-49ee-af03-cea53d4f88a9.png)

### Web Scraping
Web scrape fighter data off the official UFC website to get accurate data about all current and former fighters.
The web scraping will be done using Scrapy. Scrapy is used because it is asyncronous which has been shown to be faster when web scraping compared to syncronous execution and multi threading. After the data is scraped and stored in python, it will be stored into a csv file. Then the csv is sent to Google Drive (to automatically update the data since Tableau free does not offer Redshift connection) as well as S3. 

### AWS
AWS is used to store our raw data in an initial S3 Bucket. From there, AWS Glue is used to crawl the data and create tables to later import into redshift. PySpark is used to separate/filter the data into a fact table of measurable data and the bio dimension, which contains information about the fighter. 

After the new CSV files are stored again in S3, AWS Glue crawlers are used to crawl the new files and the redshift cluster tables. To get the crawled tables into redshift, AWS Glue Jobs were used to map the data to the specific AWS Redshift cluster and tables.

### Tableau
Tableau was used to create dynamic dashboards that can be used to query the data for fighters that have specific stats and to visually compare fighters.

## Website
View the visualizations at: analysismma.com

## Other Links
Tableau public link dashboard 1: https://public.tableau.com/views/ufc-compare/Dashboard2?:language=en-US&:display_count=n&:origin=viz_share_link

Tableau public link dashboard 2: https://public.tableau.com/views/ufc-data/Dashboard1?:language=en-US&:display_count=n&:origin=viz_share_link

Note: There may be issues viewing the dashboard on the website depending on screen size and browser. The visualizations should work for laptops and desktops on chrome, edge, and safari. Firefox has issues showing the dashboard correctly and there is no current view for mobile.
