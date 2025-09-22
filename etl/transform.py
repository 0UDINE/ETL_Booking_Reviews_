import os
import pandas as pd
from datetime import datetime, UTC, timedelta
import pyarrow.csv as pv
import pyarrow.parquet as pq



def transform():
    data_src_dir = "./data/raw"
    dfs = []

    for file in os.listdir(data_src_dir):
        if file.endswith('.csv'):
            file_path = os.path.join(data_src_dir, file)
            df = pd.read_csv(file_path)
            dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df.to_csv(r'./data/raw/raw_booking.csv', index=False)

    all_duplicates = combined_df[combined_df.duplicated(keep=False)]
    if not all_duplicates.empty:
        combined_df = combined_df.drop_duplicates()


    # Standardization of data types
    # Standardize scrape_timestamp to datetime format
    # converting scrape timestamp from object to datetime64
    combined_df.scrape_timestamp = pd.to_datetime(combined_df.scrape_timestamp)

    # Timestamp Normalization
    # Converts Unix timestamps (in milliseconds) to ISO 8601 strings.
    # Leaves existing datetime strings untouched.
    # Ensures consistent formatting across mixed sources (CSV, Parquet, API).
    def normalize_timestamp(ts):
        if isinstance(ts, int) or isinstance(ts, float):
            return datetime.fromtimestamp(ts / 1000, UTC).isoformat()
        return ts  # assume it's already a string

    combined_df["scrape_timestamp"] = combined_df["scrape_timestamp"].map(normalize_timestamp)


    # Data Inconsistency in OSM Locations
    # OpenStreetMap (OSM) API data occasionally assigns the name of a nearby village
    # instead of the corresponding city name.
    #To address this data inconsistency, we will programmatically overwrite the village
    # name with the name of the city for those specific locations
    combined_df.loc[combined_df.address.str.contains('Marrakech',na=False), 'city'] = 'Marrakech'
    combined_df.loc[combined_df.address.str.contains('Tangier',na=False), 'city'] = 'Tangier'

    # Create derived metric
    # Calculating weighted_avg
    weighted_avg = (
                           combined_df.avg_review_score_families * combined_df.avg_review_score_families_count +
                           combined_df.avg_review_score_couples * combined_df.avg_review_score_couples_count +
                           combined_df.avg_review_score_solo_travelers * combined_df.avg_review_score_solo_travelers_count +
                           combined_df.avg_review_score_business_travellers * combined_df.avg_review_score_business_travellers_count +
                           combined_df.avg_review_score_groups_friends * combined_df.avg_review_score_groups_friends_count
                   ) / (
                           combined_df.avg_review_score_families_count +
                           combined_df.avg_review_score_couples_count +
                           combined_df.avg_review_score_solo_travelers_count +
                           combined_df.avg_review_score_business_travellers_count +
                           combined_df.avg_review_score_groups_friends_count
                   )

    # Find the index of the 'max_price' column
    general_review_count_index = combined_df.columns.get_loc('general_review_count')

    # Insert the new column right after 'max_price'
    combined_df.insert(loc=general_review_count_index + 1, column='weighted_avg', value=weighted_avg)

    # Check consistency between review counts and totals
    combined_df['count_sum'] = (
            combined_df.avg_review_score_families_count +
            combined_df.avg_review_score_couples_count +
            combined_df.avg_review_score_solo_travelers_count +
            combined_df.avg_review_score_business_travellers_count +
            combined_df.avg_review_score_groups_friends_count
    )
    combined_df.query('count_sum == avg_review_score_all_count')
    combined_df.drop(columns=['count_sum'], inplace=True)

    # Data Quality Checks
    # Review Score Range Check
    combined_df.query(
        'avg_review_score_all > 0 and avg_review_score_all < 10 and '
        'avg_review_score_families > 0 and avg_review_score_families < 10 and '
        'avg_review_score_couples > 0 and avg_review_score_couples < 10 and '
        'avg_review_score_solo_travelers > 0 and avg_review_score_solo_travelers < 10 and '
        'avg_review_score_groups_friends > 0 and avg_review_score_groups_friends < 10 and '
        'avg_review_score_business_travellers > 0 and avg_review_score_business_travellers < 10',
        inplace=True
    )

    # Review Count Check
    combined_df = combined_df.query(
        "avg_review_score_all_count > 0 and \
         avg_review_score_families_count > 0 and \
         avg_review_score_couples_count > 0 and \
         avg_review_score_solo_travelers_count > 0 and \
         avg_review_score_groups_friends_count > 0 and \
         avg_review_score_business_travellers_count > 0"
    )

    # Latitude & Longitude Validity
    combined_df.query('latitude > -90 and latitude < 90 and longitude > -180 and longitude < 180', inplace=True)

    # Price Validity
    combined_df.query('min_price >= 0 and max_price >= min_price', inplace=True)

    # Handling missing values
    # handling missing zone values
    def get_zone_from_address(address):
        if pd.isna(address):
            return pd.NA

        parts = [part.strip() for part in address.split('  ') if part.strip()]

        # Expanded list of zone identifiers with different administrative levels
        zone_identifiers = [
            'Cercle de', 'Cercle d',
            'Prefecture de', 'Prefecture d',
            'Province de', 'Province d',
            'Arrondissement de', 'Arrondissement d',
            'cadat de', 'cadat d',
            'Pachalik de', 'Pachalik d',
            'Commune de', 'Commune d'
        ]

        for part in parts:
            for identifier in zone_identifiers:
                if part.startswith(identifier):
                    return part

        # As a last resort, return the administrative part before the region
        # This is a fallback strategy
        region_indicators = ['Marrakesh', 'Province', 'Prefecture']
        for i, part in enumerate(parts):
            if any(indicator in part for indicator in region_indicators) and i > 0:
                return parts[i - 1]

        return pd.NA

    # Row Selection (mask): .loc[] uses the boolean mask to choose rows.
    # It will only return the rows where the mask is True.
    # In our case, only rows with a missing zone.
    mask = combined_df['zone'].isna()
    combined_df.loc[mask, 'zone'] = combined_df.loc[mask, 'address'].apply(get_zone_from_address)

    timestamp = (datetime.now() - timedelta(hours=1)).strftime('%Y%m%d_%H%M%S') # right now minus one hour. so we can be like the image time GMT not GMT+1
    combined_df.to_csv(f'./data/staging/staged_booking{timestamp}.csv', index=False)
    # Read CSV into Arrow Table
    table = pv.read_csv(f'./data/staging/staged_booking{timestamp}.csv')
    # Write to Parquet
    pq.write_table(table, f'./data/staging/staged_booking{timestamp}.parquet')
    print(f"✅ Transformation complete. Final row count: {len(combined_df)}")
    print(f"✅ Transformation complete. Final columns count: {len(combined_df.columns)}")
    parquet_file = pq.ParquetFile(f"./data/staging/staged_booking{timestamp}.parquet")
    num_columns = len(parquet_file.schema.names)
    print(f"🧮 Parquet column count: {num_columns}")
    return f"/opt/prefect/data/staging/staged_booking{timestamp}.parquet"

