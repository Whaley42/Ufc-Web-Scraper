# UFC Data Project

## Goal of this Project:
    Scrape UFC fighter data that can be used to create an end-user dashboard to query and compare fighters.

## The Process:
### Web Scraping
Web scrape fighter data off the official UFC website to get accurate data about all current and former fighters.
The web scraping will be done using Scrapy. I decided to be use Scrapy because it is asyncronous which has been shown to be faster when web scraping compared to syncronous execution and multi threading. After the data is scraped and stored in python, it will be stored into a csv file and sent to an S3 Bucket on AWS.

Data Folder: Contains the raw csv to be sent to AWS

Postscrape Folder: Contains scrapy configurations.

Spiders Folder: Contains main.py that is used to start the program.

Classes Folder: Contains the classes used to scrape the data.

### AWS
AWS is used to store our raw data in an initial S3 Bucket. From there, AWS Glue is used to crawl the data and create tables to later import into redshift. PySpark is used to separate the data into a fact table of measurable data and the bio dimension, which contains information about the fighter. 

    raw_df = glueContext.create_dynamic_frame.from_catalog(
    database = "local-ufc", 
    table_name = "raw_data"
    ).toDF()
    filtered_df = raw_df.filter(raw_df["fights"] > 0)
    filtered_df = filtered_df.filter(filtered_df["sig_strikes_attempted"] > 0)
    renamed_df = filtered_df.withColumnRenamed("place_of_birth", "hometown") \
            .withColumnRenamed("octagon_debut", "debut") \
            .withColumnRenamed("standing", "strikes_standing") \
            .withColumnRenamed("clinch", "strikes_clinch") \
            .withColumnRenamed("ground", "strikes_ground") \
            .withColumnRenamed("sig_str_head", "sig_strikes_head") \
            .withColumnRenamed("sig_str_body", "sig_strikes_body") \
            .withColumnRenamed("sig_str_leg", "sig_strikes_leg") \
            .withColumnRenamed("sig_str_landed", "sig_strikes_land_per15") \
            .withColumnRenamed("sig_str_absorbed", "sig_strikes_absorb_per15") \
            .withColumnRenamed("sig_str_defense", "sig_strike_defense")
    fact_df = renamed_df.select(
            "id",
            "sig_strikes_landed",
            "sig_strikes_attempted",
            "sig_strikes_land_per15",
            "sig_strikes_absorb_per15",
            "strikes_standing",
            "strikes_clinch",
            "strikes_ground",
            "sig_strikes_head",
            "sig_strikes_body",
            "sig_strikes_leg",
            "sig_strike_defense",
            "knockdown_avg",
            "takedowns_landed",
            "takedowns_attempted",
            "takedown_avg",
            "takedown_defense",
            "submission_avg",
            "ko_tko",
            "dec",
            "sub",
            "height",
            "reach",
            "leg_reach",
            "age",
            "average_fight_time",
            "fights",
            "wins")
        bio_df = renamed_df.select(
            "id",
            "first_name",
            "last_name",
            "division",
            "status",
            "fighting_style",
            "trains_at",
            "hometown",
            "debut")
    
    from awsglue.dynamicframe import DynamicFrame

    fact_convert = DynamicFrame.fromDF(fact_df, glueContext, "convert")
    bio_convert = DynamicFrame.fromDF(bio_df, glueContext, "convert")

After the new CSV files are stored again in S3, AWS Glue crawlers are used to crawl the new files as well as the redshift cluster tables that were created to store the data. To get the crawled tables into redshift, AWS Glue Jobs was used to map the data to the specific AWS Redshift cluster and tables.

### Tableau
Tableau was used to create dynamic dashboards that can be used to not only query the data for fighters that have specific stats but also to visually compare fighters.

View the visualizations at: analysismma.com

Tableau public link dashboard 1: https://public.tableau.com/views/ufc-compare/Dashboard2?:language=en-US&:display_count=n&:origin=viz_share_link

Tableau public link dashboard 2: https://public.tableau.com/views/ufc-data/Dashboard1?:language=en-US&:display_count=n&:origin=viz_share_link

Note: There may be issues viewing the dashboard on the website depending on screen size and browser. The visualizations should work for laptops and desktops on chrome, edge, and safari. Firefox has issues showing the dashboard correctly and there is no current view for mobile.