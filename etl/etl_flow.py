from prefect import flow, task
import sys
import os
# Add the parent directory (project root) to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scraper import scrape_single_threaded
from etl import transform
from etl import load2snowflake


@task
def extract_data():
    print("Starting extraction...")
    cities = ["Marrakech", "Tangier"]
    # scrape_single_threaded(cities, batch_size=5)
    print("Extraction completed!")


@task
def transform_data():
    print(f"Transforming data")
    transformed_file = transform()
    print("Transformation completed!")
    return transformed_file

@task
def load_data(transformed_file):
    print(f"Loading data into snowflake :")
    load2snowflake(transformed_file)
    print("Loading completed!")
    return "success"

@flow
def booking_etl_flow():
    print("Flow started!")
    extract_data()
    transformed_file = transform_data()
    load_data(transformed_file)


if __name__ == "__main__":
    print("Running booking ETL flow...")
    booking_etl_flow()
    print("Done!")