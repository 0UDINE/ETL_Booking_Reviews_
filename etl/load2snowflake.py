import snowflake.connector


def load2snowflake(transformed_file):
    conn = snowflake.connector.connect(
        user='******',
        password='*****',
        account='*****',
        warehouse='BOOKING_WH',
        database='BOOKING_DB',
        schema='STAGING'
    )
    cursor = conn.cursor()
    cursor.execute("CREATE WAREHOUSE IF NOT EXISTS BOOKING_WH") # default size x-small
    cursor.execute("USE WAREHOUSE BOOKING_WH")
    cursor.execute("CREATE DATABASE IF NOT EXISTS BOOKING_DB")
    cursor.execute("USE DATABASE BOOKING_DB")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS STAGING")
    cursor.execute("USE SCHEMA STAGING")

    cursor.execute(
        "CREATE TABLE IF NOT EXISTS "
        "BOOKING_PROPERTIES ("
            "property_id TEXT,"
            "scrape_timestamp TIMESTAMP_NTZ,"
            "property_url TEXT,"
            "category TEXT,"
            "general_review REAL,"
            "general_review_count REAL,"
            "weighted_avg REAL,"
            "comfort_score REAL,"
            "value_score REAL,"
            "location_score REAL,"
            "wifi_score REAL,"
            "avg_review_score_all REAL,"
            "avg_review_score_all_count REAL," 
            "avg_review_score_families REAL,"
            "avg_review_score_families_count REAL,"
            "avg_review_score_couples REAL,"
            "avg_review_score_couples_count REAL,"
            "avg_review_score_solo_travelers REAL,"
            "avg_review_score_solo_travelers_count REAL,"
            "avg_review_score_business_travellers REAL,"
            "avg_review_score_business_travellers_count REAL,"
            "avg_review_score_groups_friends REAL,"
            "avg_review_score_groups_friends_count REAL,"
            "min_price REAL,"
            "max_price REAL,"
            "latitude REAL,"
            "longitude REAL,"
            "address TEXT,"
            "zone TEXT,"
            "city TEXT,"
            "wifi_speed TEXT"
        ")")

    cursor.execute("REMOVE @%BOOKING_PROPERTIES;")# REMOVE so the BOOKING_PROP variable does not keep the previous added data

    cursor.execute(f"PUT file://{transformed_file} @%BOOKING_PROPERTIES")

    cursor.execute(
        "COPY INTO BOOKING_PROPERTIES "
        "FROM @%BOOKING_PROPERTIES "
        "FILE_FORMAT = (TYPE = 'PARQUET') "
        "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
        "ON_ERROR = 'CONTINUE'"
    )

    print("✅ Data successfully loaded into Snowflake.")

