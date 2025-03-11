import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import logging
import urllib.parse

# Configure Logging
logging.basicConfig(filename="etl_log.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# MySQL Connection Details
DB_USER = "root"
DB_PASSWORD = "Charran@2801"
DB_HOST = "localhost"
DB_NAME = "retail_optimization"

# Encode password (for special characters)
password = urllib.parse.quote_plus(DB_PASSWORD)
engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{password}@{DB_HOST}/{DB_NAME}")

# Function to Load Data from CSV
def load_data(file_path):
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Loaded {file_path} successfully.")
        return df
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")
        return None

# Function to Clean and Transform Data
def transform_data(stores_df, features_df, sales_df):
    try:
        # ✅ Standardize Column Names
        stores_df.columns = ["store_id", "store_type", "store_size"]
        features_df.columns = ["store_id", "date", "temperature", "fuel_price", "markdown1", "markdown2",
                               "markdown3", "markdown4", "markdown5", "cpi", "unemployment", "is_holiday"]
        sales_df.columns = ["store_id", "dept_id", "date", "weekly_sales", "is_holiday"]

        # ✅ Convert Date Format
        features_df["date"] = pd.to_datetime(features_df["date"], format="%d/%m/%Y").dt.date
        sales_df["date"] = pd.to_datetime(sales_df["date"], format="%d/%m/%Y").dt.date

        # ✅ Convert Data Types
        stores_df["store_id"] = stores_df["store_id"].astype(int)
        features_df["store_id"] = features_df["store_id"].astype(int)
        sales_df["store_id"] = sales_df["store_id"].astype(int)
        sales_df["dept_id"] = sales_df["dept_id"].astype(int)

        # ✅ Convert Boolean Columns
        features_df["is_holiday"] = features_df["is_holiday"].astype(int)
        sales_df["is_holiday"] = sales_df["is_holiday"].astype(int)

        # ✅ Ensure No Negative Sales Data
        sales_df = sales_df[sales_df["weekly_sales"] >= 0]

        logging.info("Data transformation successful.")
        return stores_df, features_df, sales_df
    except Exception as e:
        logging.error(f"Error in data transformation: {e}")
        return None, None, None

# Function to Load Data into MySQL
def load_to_mysql(df, table_name, batch_size=5000):
    try:
        df.to_sql(table_name, engine, if_exists="append", index=False, chunksize=batch_size)
        logging.info(f"Loaded {table_name} into MySQL successfully.")
    except Exception as e:
        logging.error(f"Error loading {table_name} into MySQL: {e}")

# Main ETL Process
def run_etl():
    logging.info("ETL Process Started")

    # Extract Phase
    stores_df = load_data(r"D:\projects\retail_optimization\retail_dataset\Stores.csv")
    features_df = load_data(r"D:\projects\retail_optimization\retail_dataset\Features.csv")
    sales_df = load_data(r"D:\projects\retail_optimization\retail_dataset\Sales.csv")

    if stores_df is None or features_df is None or sales_df is None:
        logging.error("ETL Process Terminated: Data Load Failure")
        return

    # Transform Phase
    stores_df, features_df, sales_df = transform_data(stores_df, features_df, sales_df)
    if stores_df is None or features_df is None or sales_df is None:
        logging.error("ETL Process Terminated: Data Transformation Failure")
        return

    # Load Phase
    load_to_mysql(stores_df, "stores")
    load_to_mysql(features_df, "features")
    load_to_mysql(sales_df, "sales")

    logging.info("ETL Process Completed Successfully ✅")

# Run the ETL Process
if __name__ == "__main__":
    run_etl()
