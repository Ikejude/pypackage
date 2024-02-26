### START FUNCTION

def create_db_engine(db_path):
    """
    Create a database engine using SQLAlchemy.

    This function creates a database engine using the provided database path and tests the connection.

    Parameters:
    db_path (str): The path to the database file.

    Returns:
    sqlalchemy.engine.base.Engine: An Engine object representing the connection to the database.

    Example:
    >>> engine = create_db_engine('sqlite:///example.db')
    >>> engine
    Engine(sqlite:///example.db)
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
    Execute a SQL query on the provided database engine.

    This function executes the provided SQL query on the specified database engine and returns the result as a DataFrame.

    Parameters:
    engine (sqlalchemy.engine.base.Engine): The database engine to execute the query on.
    sql_query (str): The SQL query to execute.

    Returns:
    pandas.DataFrame: A DataFrame containing the results of the SQL query.

    Example:
    >>> df = query_data(engine, 'SELECT * FROM table_name')
    >>> df.head()
       column1  column2
    0        1        2
    1        3        4
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
    Read a CSV file from the web.

    This function reads a CSV file from the specified URL and returns it as a DataFrame.

    Parameters:
    URL (str): The URL of the CSV file.

    Returns:
    pandas.DataFrame: A DataFrame containing the data from the CSV file.

    Example:
    >>> df = read_from_web_CSV('https://example.com/data.csv')
    >>> df.head()
       column1  column2
    0        1        2
    1        3        4
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