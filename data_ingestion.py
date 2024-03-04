"""
Module: Data Ingestion

This module provides utility functions for data ingestion, including querying databases and reading CSV files from the web.

Import Statements: The code imports necessary modules such as create_engine and text from SQLAlchemy, logging, and pandas to manipulate data.

Logger Configuration: Establishes a logger named 'data_ingestion' to manage log messages, including timestamps, logger names, and levels.
"""
from sqlalchemy import create_engine, text
import logging
import pandas as pd

# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

### START FUNCTION

def create_db_engine(db_path):
    """
    Create a SQLAlchemy database engine for connecting to the database using the specified database path.
    
    Args:
        db_path (str): Path or connection string to the database.

    Returns:
        engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine object.
        
    Raises:
        ImportError: If SQLAlchemy is not installed.
        Exception: If there's an error during engine creation.
    """
    try:

        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine # Return the engine object if it all works well
    except ImportError: #If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:# If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e
    
def query_data(engine, sql_query):
    """
    Executes an SQL query on the provided database engine.

    Args:
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy database engine.
        sql_query (str): SQL query to execute.

    Returns:
        pandas.DataFrame: Result of the query as a DataFrame.

    Raises:
        ValueError: If the query result is empty.
        Exception: If any other error occurs during execution.
    """
    try:

        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e
    
def read_from_web_CSV(URL):
    """
    Reads CSV data from the provided web URL.

    Args:
        URL (str): Web URL pointing to a CSV file.

    Returns:
        pandas.DataFrame: DataFrame containing the CSV data.

    Raises:
        pd.errors.EmptyDataError: If the CSV file is empty.
        Exception: If any other error occurs during reading.
    """
    try:

        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e
    
### END FUNCTION