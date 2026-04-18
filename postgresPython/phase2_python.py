# CS 3431
# For additional examples:
# https://www.psycopg.org/docs/usage.html#query-parameters

#  If psycopg2 is not installed, install it using pip installer :  
#     pip install psycopg2 (Windows)  
#  or pip3 install psycopg2 (Mac)
# 
#  If the installation of psycopg2 fails, try installing the binary. The binary is  a stand-alone package, not requiring a compiler or external libraries:
#     $ pip install psycopg2-binary  (Windows)
# or  $ pip3 install psycopg2-binary  (Mac)
#    
import json
import psycopg2
from contextlib import contextmanager
from dotenv import load_dotenv
import os

# Load environment variables from .env file (if used)
# load_dotenv() is a function from the python-dotenv package that loads environment variables from a .env file into the Python environment. 
load_dotenv()

# Database connection parameters
#TODO: update  username and password
psql_params = {
    "DB_NAME" : "tempyelp",    # Make sure that "tempdb" database is already created in your PostgreSQL server.
    "DB_USER" : 'postgres',  # get from environment variables
    "DB_PASSWORD" : 'wmtll..3', # get from environment variables
    "DB_HOST" : "localhost",  # or the IP address of your database server
    "DB_PORT" : "5432"  # Default PostgreSQL port
}
@contextmanager
def connect_psql(db_params):
    conn = None
    #connect to tempdb database on postgres server using psycopg2
    try:
        conn = psycopg2.connect(dbname=db_params["DB_NAME"],
                                user=db_params["DB_USER"],
                                password=db_params["DB_PASSWORD"],
                                host=db_params["DB_HOST"],
                                port=db_params["DB_PORT"])
        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()
        print("Connected to PostgreSQL")

        # Yield cursor (execution pauses here, allowing the caller to use it)
        yield cursor

        # Commit transactions (if needed)
        conn.commit()

    except Exception as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()  # Rollback in case of an error
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Connection closed")

"""cleanStr4SQL function removes the "single quote" or "back quote" characters from strings. """
def cleanStr4SQL(s):
    return s.replace("'","`").replace("\n"," ")

# Insert business data
def insert_business():
    with connect_psql(psql_params) as cursor:
        #reading the JSON file
        with open('.//yelp_business.JSON','r') as f:    #TODO: update path for the input file
            line = f.readline()
            count_line = 0
            while line:
                data = json.loads(line)
                # Generate the INSERT statement for the current business
                # TODO: The below INSERT statement is based on a simple (and incomplete) businesstable schema. Update the statement based on your own table schema and include values for all businessTable attributes

                is_open: bool = bool(data["is_open"])
                try:
                    cursor.execute(
                        """INSERT INTO Business (b_id, rating, name, city, address, latitude, longitude, state, is_open,
                                                 zip, tip_count)
                                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); """,
                                   (data['business_id'], data["stars"], cleanStr4SQL(data["name"]), data["city"], cleanStr4SQL(data["address"]), data['latitude'], data['longitude'], data["state"], is_open, data["postal_code"], 0))
                except Exception as e:
                    print("Insert to business table failed for row " + str(count_line) + "!",e)
                    return

                line = f.readline()
                count_line +=1
            print(count_line)
            f.close()

# insert_business()


# The above code assumes the following BusinessTable schema
"""
CREATE TABLE Business (
   b_id CHAR (22),
   'name' VARCHAR,
   rating REAL,
   address VARCHAR,
   city NAME,
    zip VARCHAR,
   'state' VARCHAR,
   tip_count INTEGER,
    is_open BOOLEAN,
    latitude FLOAT,
    longitude FLOAT,
   PRIMARY KEY (b_id)
)
"""

def insert_checkin():
    with connect_psql(psql_params) as cursor:
        # reading the JSON file
        with open('C:/Users/victo/YelpDataset/yelpdata_CS3431/yelp_checkin.JSON', 'r') as f:  # TODO: update path for the input file
            line = f.readline()
            count_line = 0
            while line:
                data = json.loads(line)
                clean_date = data['date'].split(',')[0]
                try:
                    cursor.execute(
                        """INSERT INTO CheckIn (b_id, time_stamp)
                           VALUES (%s, %s); """,
                        (data['business_id'], clean_date))
                except Exception as e:
                    print("Insert to business table failed for row " + str(count_line) + "!", e)
                    return

                line = f.readline()
                count_line += 1
            print(count_line)
            f.close()

# insert_checkin()

"""
CREATE TABLE CheckIn (
    time_stamp TIMESTAMP,
    b_id CHAR(22),
    PRIMARY KEY (time_stamp, b_id),
    FOREIGN KEY (b_id) REFERENCES Business(b_id)
);
"""