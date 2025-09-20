from os import getenv

from snowflake.snowpark import Session
from dotenv import load_dotenv
import os


def create_snowflake_session():
    # Load environment variables from .env file
    load_dotenv()

    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DB"),
        "schema": getenv("SNOWFLAKE_SCHEMA"),
    }

    # create the user session so I can run queries
    session = Session.builder.configs(connection_parameters).create()
    return session


def loading2snowflake(transformed_file_path):

    session = create_snowflake_session()

    try:
        #create the resources I will use to load my data
        session.sql("CREATE WAREHOUSE IF NOT EXISTS BOOKING_WH").collect()
        session.sql("USE WAREHOUSE BOOKING_WH").collect()
        session.sql("CREATE DATABASE IF NOT EXISTS BOOKING_DB").collect()
        session.sql("USE DATABASE BOOKING_DB").collect()
        session.sql("CREATE SCHEMA IF NOT EXISTS STAGING").collect()
        session.sql("USE SCHEMA STAGING").collect()
        session.sql("CREATE STAGE IF NOT EXISTS booking_clean_data").collect()
        print("created all the necessary ressources : VWH, DB, SCHEMA, STAGING VARIABLE")

        # clearing the internal stage variable to not append the new added data into the previously added data
        session.sql("REMOVE @booking_clean_data").collect()
        print("CLEARING THE INTERNAL STAGE @booking_clean_data")
        # storing the parquet file into an internal stage
        session.file.put(transformed_file_path,"@booking_clean_data")

        # loading my parquet file into a df, so I can write it into a snowflake table
        booking_property_df = session.read.parquet("@booking_clean_data")
        print("READ THE PARQUET FILE CONTENT AND STORE IT INTO A DATAFRAME")

        booking_property_df.write.save_as_table("BOOKING_PROPERTIES", mode="overwrite") # you can use mode="append"
        print("WROTE THE DATA INTO BOOKING_PROPERTIES SNOWFLAKE TABLE")

        session.sql("""
                    CREATE OR REPLACE TABLE BOOKING_DB.STAGING.BOOKING_PROPERTIES AS
                    SELECT *,TO_TIMESTAMP("scrape_timestamp" / 1000) AS normalized_timestamp
                    FROM BOOKING_DB.STAGING.BOOKING_PROPERTIES;
                    """).collect()

    except Exception as e:
      print(f"error: {e}")

    finally:
      session.close()

def olap_modeling():

    session = create_snowflake_session()

    session.sql("CREATE SCHEMA IF NOT EXISTS ANALYTICS").collect()
    session.sql("USE SCHEMA ANALYTICS").collect()
    session.sql("CREATE SCHEMA IF NOT EXISTS ANALYTICS").collect()
    session.sql("USE SCHEMA ANALYTICS").collect()
    print("CREATED ANALYTICS SCHEMA")

    # creating DIM_PROPERTY(property_id, property_url, address, wifi_speed, latittude, longitude)
    session.sql("""
                    CREATE TABLE IF NOT EXISTS DIM_PROPERTY (
                     property_id INT IDENTITY(1,1),
                     property_url TEXT,
                     address TEXT,
                     wifi_speed TEXT,
                     latitude FLOAT,
                     longitude FLOAT
                    )
                    """).collect()
    print("CREATED DIM_PROPERTY SUCCESSFULLY ")

    # loading data using merge so the task will be idempotent like no matter how much u run it, it results the same result
    session.sql("""
                MERGE INTO DIM_PROPERTY AS TARGET
                USING (
                    SELECT 
                        DISTINCT "property_url", 
                        "address", 
                        "wifi_speed", 
                        "latitude", 
                        "longitude" 
                    FROM BOOKING_DB.STAGING.BOOKING_PROPERTIES
                
                ) AS source
                ON source."property_url" = target.property_url
                WHEN NOT MATCHED THEN 
                    INSERT (
                        property_url, 
                        address, 
                        wifi_speed, 
                        latitude, 
                        longitude)
                VALUES(
                    source."property_url", 
                    source."address", 
                    source."wifi_speed", 
                    source."latitude", 
                    source."longitude" )
        """).collect()
    print("CREATED DIM_PROPERTY SUCCESSFULLY ")

    # creating DIM_CATEGORY(category_id,category)
    session.sql("""
                    CREATE TABLE IF NOT EXISTS DIM_CATEGORY (
                     category_id INT IDENTITY(1,1),
                     category TEXT
                    )
                    """).collect()
    print("CREATED DIM_CATEGORY SUCCESSFULLY ")


    session.sql("""
                MERGE INTO DIM_CATEGORY AS target
                USING (
                    SELECT DISTINCT "category"
                    FROM BOOKING_DB.STAGING.BOOKING_PROPERTIES
                ) AS source
                ON source."category" = target.category
                WHEN NOT MATCHED THEN INSERT(category)VALUES(source."category")
                """).collect()
    print("LOADED DATA INTO DIM_CATEGORY SUCCESSFULLY ")


    # creating DIM_DATE(date_id,scrape_timestamp,day,month,year,weekday)
    session.sql("""
                    CREATE TABLE IF NOT EXISTS DIM_DATE (
                     date_id INT IDENTITY(1,1),
                     scrape_timestamp TEXT,
                     day INT,
                     month INT,
                     year INT,
                     weekday INT
                    )
                    """).collect()
    print("CREATED DIM_DATE SUCCESSFULLY ")


    session.sql("""
                MERGE INTO DIM_DATE AS target
                USING (
                    SELECT 
                        "NORMALIZED_TIMESTAMP" AS scrape_timestamp,
                        DAY("NORMALIZED_TIMESTAMP") AS day,
                        MONTH("NORMALIZED_TIMESTAMP") AS month,
                        YEAR("NORMALIZED_TIMESTAMP") AS year,
                        DAYOFWEEK("NORMALIZED_TIMESTAMP") AS weekday
                    FROM BOOKING_DB.STAGING.BOOKING_PROPERTIES 
                ) AS source
                ON source.scrape_timestamp = target.scrape_timestamp
                WHEN NOT MATCHED THEN 
                INSERT(scrape_timestamp,day,month,year,weekday)
                VALUES(
                    source.scrape_timestamp,
                    source.day,
                    source.month,
                    source.year,
                    source.weekday
                    )
                """).collect()
    print("LOADED DATA INTO DIM_DATE SUCCESSFULLY ")


    # creating DIM_LOCATION(location_id,city,zone)
    session.sql("""
                    CREATE TABLE IF NOT EXISTS DIM_LOCATION (
                     location_id INT IDENTITY(1,1),
                     city TEXT,
                     zone TEXT
                    )
                    """).collect()
    print("CREATED DIM_LOCATION SUCCESSFULLY ")

    session.sql("""
                MERGE INTO DIM_LOCATION as target
                USING (
                        SELECT 
                            DISTINCT "city",
                            "zone",
                        FROM BOOKING_DB.STAGING.BOOKING_PROPERTIES 
                ) AS source
                ON target.city = source."city" AND target.zone = source."zone"
                WHEN NOT MATCHED THEN
                    INSERT(
                        city,
                        zone
                    )VALUES(
                        source."city",
                        source."zone"
                    )
                """).collect()
    print("LOADED DATA INTO DIM_LOCATION SUCCESSFULLY ")


    # creating FACT_PROPERTY_REVIEWSFACT_PROPERTY_REVIEWS(scrape_timestamp, category_id, reviews..., min_price, max_price)
    session.sql("""
                    CREATE TABLE IF NOT EXISTS FACT_PROPERTY_REVIEWS (
                     property_id INT IDENTITY(1,1),
                     scrape_timestamp TIMESTAMP_NTZ,
                     category_id INT,
                     general_review_count FLOAT,
                     weighted_avg FLOAT,
                     comfort_score FLOAT,
                     value_score FLOAT,
                     location_score FLOAT,
                     wifi_score FLOAT,
                     avg_review_score_all FLOAT,
                     avg_review_score_all_count FLOAT,
                     avg_review_score_families FLOAT,
                     avg_review_score_families_count FLOAT,
                     avg_review_score_couples FLOAT,
                     avg_review_score_couples_count FLOAT,
                     avg_review_score_solo_travelers FLOAT,
                     avg_review_score_solo_travelers_count FLOAT,
                     avg_review_score_business_travellers FLOAT,
                     avg_review_score_business_travellers_count FLOAT,
                     avg_review_score_groups_friends FLOAT,
                     avg_review_score_groups_friends_count FLOAT,
                     min_price FLOAT,
                     max_price FLOAT
                    )
                    """).collect()
    print("CREATED FACT_PROPERTY_REVIEWS SUCCESSFULLY ")


    session.sql("""
               MERGE INTO FACT_PROPERTY_REVIEWS AS target
                USING (
                    SELECT 
                     bp."NORMALIZED_TIMESTAMP" AS NORMALIZED_TIMESTAMP,
                     dc.category_id,
                     bp."general_review_count",
                     bp."weighted_avg",
                     bp."comfort_score",
                     bp."value_score",
                     bp."location_score",
                     bp."wifi_score",
                     bp."avg_review_score_all",
                     bp."avg_review_score_all_count",
                     bp."avg_review_score_families",
                     bp."avg_review_score_families_count",
                     bp."avg_review_score_couples",
                     bp."avg_review_score_couples_count",
                     bp."avg_review_score_solo_travelers",
                     bp."avg_review_score_solo_travelers_count",
                     bp."avg_review_score_business_travellers",
                     bp."avg_review_score_business_travellers_count",
                     bp."avg_review_score_groups_friends",
                     bp."avg_review_score_groups_friends_count",
                     bp."min_price",
                     bp."max_price"
                    FROM BOOKING_DB.STAGING.BOOKING_PROPERTIES bp
                    JOIN BOOKING_DB.ANALYTICS.DIM_CATEGORY dc
                    ON bp."category" = dc.category
                ) as source
                ON target.SCRAPE_TIMESTAMP = source."NORMALIZED_TIMESTAMP"
                WHEN NOT MATCHED THEN INSERT (
                  "SCRAPE_TIMESTAMP",
                  category_id,
                  general_review_count,
                  weighted_avg,
                  comfort_score,
                  value_score,
                  location_score,
                  wifi_score,
                  avg_review_score_all,
                  avg_review_score_all_count,
                  avg_review_score_families,
                  avg_review_score_families_count,
                  avg_review_score_couples,
                  avg_review_score_couples_count,
                  avg_review_score_solo_travelers,
                  avg_review_score_solo_travelers_count,
                  avg_review_score_business_travellers,
                  avg_review_score_business_travellers_count,
                  avg_review_score_groups_friends,
                  avg_review_score_groups_friends_count,
                  min_price,
                  max_price
                )VALUES(
                    source."NORMALIZED_TIMESTAMP",
                    source.category_id,
                    source."general_review_count",
                    source."weighted_avg",
                    source."comfort_score",
                    source."value_score",
                    source."location_score",
                    source."wifi_score",
                    source."avg_review_score_all",
                    source."avg_review_score_all_count",
                    source."avg_review_score_families",
                    source."avg_review_score_families_count",
                    source."avg_review_score_couples",
                    source."avg_review_score_couples_count",
                    source."avg_review_score_solo_travelers",
                    source."avg_review_score_solo_travelers_count",
                    source."avg_review_score_business_travellers",
                    source."avg_review_score_business_travellers_count",
                    source."avg_review_score_groups_friends",
                    source."avg_review_score_groups_friends_count",
                    source."min_price",
                    source."max_price"
                )
                """).collect()
    print("LOADED DATA INTO  FACT_PROPERTY_REVIEWS SUCCESSFULLY ")



