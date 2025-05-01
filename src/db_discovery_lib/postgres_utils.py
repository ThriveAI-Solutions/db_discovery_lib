import psycopg
import pandas as pd

def get_connection_credentials(DB_NAME="postgres", USER="postgres", PASSWORD="admin", HOST="localhost", PORT="5432"):
    """Returns credentials for database connection."""
    return DB_NAME, USER, PASSWORD, HOST, PORT

def connect_to_postgresql(DB_NAME, USER, PASSWORD, HOST, PORT):
    """
    Establishes a connection to a PostgreSQL database using psycopg.
    Returns the connection object if successful, otherwise None.
    """
    try:
        conn = psycopg.connect(
            dbname=DB_NAME,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        print("✅ Connection to PostgreSQL successful!")
        return conn
    except psycopg.OperationalError as e:
        print(f"❌ Error connecting to PostgreSQL: {e}")
        return None

def check_connection(conn):
    """
    Checks if the database connection is alive.
    Returns True if the connection is valid, otherwise False.
    """
    try:
        if conn is not None:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
            print("✅ Connection is active.")
            return True
        else:
            print("❌ No connection established.")
            return False
    except psycopg.OperationalError as e:
        print(f"❌ Connection lost: {e}")
        return False

def select_query(conn, sql_query = 'SELECT version();'):
    """
    Runs a simple SELECT statement to verify the database connection.
    Returns True if the query executes successfully.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql_query)  # Fetches PostgreSQL version
            result = cur.fetchone()
        print(f"✅ Select result: {result[0]}")
        return True
    except Exception as e:
        print(f"❌ Query execution failed: {e}")
        return False

def verify_query(conn, sql_query = 'SELECT version();'):
    """
    Runs a simple SELECT statement to verify the database connection.
    Returns True if the query executes successfully.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql_query)  # Fetches PostgreSQL version
            result = cur.fetchone()
        print(f"✅ PostgreSQL version: {result[0]}")
        return True
    except Exception as e:
        print(f"❌ Query execution failed: {e}")
        return False


def create_pretend_employee_df():
    # Sample DataFrame with different data types
    data = {
        "id": [1, 2, 3],  # INTEGER
        "name": ["Joe", "Kyle", "Frank"],  # TEXT
        "age": [25, 30, 35],  # INTEGER
        "salary": [55000.50, 62000.75, 72000.00],  # FLOAT
        "is_active": [True, False, True],  # BOOLEAN
        "created_at": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"])  # TIMESTAMP
    }
    df = pd.DataFrame(data)
    return df 

def infer_sql_dtype(pd_dtype):
    """
    Maps pandas dtypes to PostgreSQL data types.
    
    Parameters:
        pd_dtype (dtype): A pandas data type.
    
    Returns:
        str: Corresponding PostgreSQL data type.
    """
    if pd_dtype == "int64":
        return "INTEGER"
    elif pd_dtype == "float64":
        return "FLOAT"
    elif pd_dtype == "bool":
        return "BOOLEAN"
    elif "datetime" in str(pd_dtype):
        return "TIMESTAMP"
    elif pd_dtype == "object":
        return "TEXT"
    else:
        return "TEXT"  # Default fallback type

def test_potgresql_workflow(): 
    DB_NAME, USER, PASSWORD, HOST, PORT = get_connection_credentials
    connection = connect_to_postgresql(DB_NAME, USER, PASSWORD, HOST, PORT)

    if connection and check_connection(connection):
        verify_query(connection)

    if connection and check_connection(connection):
        Select_query(connection)

    if connection:
        connection.close()
        print("🔌 Connection closed.")
    return "workflow test end"     

def create_table_from_dataframe(df, table_name, dbname, user, password, host, port):
    """
    Creates a PostgreSQL table based on the DataFrame structure and inserts all rows.

    Parameters:
        df (pd.DataFrame): The DataFrame whose structure and data will be used.
        table_name (str): The name of the PostgreSQL table to create.
        dbname (str): Database name.
        user (str): Database username.
        password (str): Database password.
        host (str, optional): Database host. Defaults to "localhost".
        port (str, optional): Database port. Defaults to "5432".

    Returns:
        bool: True if successful, False otherwise.
    """
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    try:
        # Establish connection
        with psycopg.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        ) as conn:
            with conn.cursor() as cur:
                # Generate SQL column definitions dynamically
                
                columns = ", ".join([f'{col} {infer_sql_dtype(str(dtype))}' for col, dtype in df.dtypes.items()])
                
                # Create table if not exists
                create_table_query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns});'
                cur.execute(create_table_query)
                print(f"✅ Table '{table_name}' created or already exists.")

                # Insert DataFrame data into the table
                for _, row in df.iterrows():
                    values = tuple(row.replace({pd.NA: None}))  # Replace NaNs with NULL values
                    placeholders = ", ".join(["%s"] * len(values))  # Creates (%s, %s, ...)
                    insert_query = f'INSERT INTO "{table_name}" VALUES ({placeholders});'
                    cur.execute(insert_query, values)

                conn.commit()
                print(f"✅ Data inserted into '{table_name}' successfully.")
                return True

    except Exception as e:
        print(f"❌ Error creating table or inserting data: {e}")
        return False

def sql_to_dataframe(query, dbname, user, password, host, port):
    """
    Executes a given SQL SELECT query and retrieves the results into a pandas DataFrame.
    
    Parameters:
        query (str): The SQL SELECT statement to execute.
        dbname (str): Database name.
        user (str): Database username.
        password (str): Database password.
        host (str, optional): Database host.  
        port (str, optional): Database port. 
    
    Returns:
        pd.DataFrame: A DataFrame containing the query results.
    """
    try:
        with psycopg.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        ) as conn:
            df = pd.read_sql(query, conn)
            print("✅ Query executed successfully. Data retrieved.")
            return df

    except Exception as e:
        print(f"❌ Error executing query: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

def drop_table(conn, table_name):
    """
    Drops a table from the connected PostgreSQL database.

    Parameters:
        conn: The active database connection.
        table_name (str): The name of the table to drop.

    Returns:
        bool: True if the table was dropped successfully, False otherwise.
    """
    try:
        with conn.cursor() as cur:
            # Always sanitize table names to avoid injection
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
            conn.commit()
            print(f"✅ Table '{table_name}' dropped successfully.")
            return True

    except Exception as e:
        print(f"❌ Error dropping table '{table_name}': {e}")
        return False




