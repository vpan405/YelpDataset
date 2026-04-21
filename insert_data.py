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

# edit paths here
json_folder: str = "./yelpdata_CS3431/"

business_file: str = "yelp_business.json"
checkin_file: str = "yelp_checkin.json"
user_file: str = "yelp_user.json"
tip_file: str = "yelp_tip.json"

# Database connection parameters
psql_params = {
    "DB_NAME" : "tempyelp",
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
        # reading the JSON file
        with open(json_folder + business_file,'r') as f:
            line = f.readline()
            count_line = 0
            unique_categories: list = [] # used for BusinessCategory Table
            while line:
                data: dict = json.loads(line)
                b_id = data["business_id"]

                # Generate the INSERT statement for the current business

                ## BUSINESS INSERT
                is_open: bool = bool(data["is_open"])
                try:
                    cursor.execute("""INSERT INTO Business (b_id, rating, name, city, address, latitude, longitude, state, is_open, zip, tip_count)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); """,
                                    (b_id, data["stars"], cleanStr4SQL(data["name"]), data["city"], cleanStr4SQL(data["address"]), data['latitude'], data['longitude'], data["state"], is_open, data["postal_code"], 0))
                except Exception as e:
                    print("Insert to business table failed for row " + str(count_line) + "!",e)
                    return

                ## HOURS INSERT
                for day, time_range in data["hours"].items():
                    times = time_range.split("-")
                    try:
                        cursor.execute("""INSERT INTO Hours (weekday, b_id, open_time, close_time)
                                        VALUES (%s, %s, %s, %s); """,
                                        (day, b_id, times[0], times[1]))
                    except Exception as e:
                        print("Insert to hours table failed for row " + str(count_line) + "!",e)
                        return

                ## BUSINESSCATEGORY INSERT
                ## BELONGSTO INSERT
                categories = data["categories"].split(", ")
                for category in categories:
                    if category not in unique_categories:
                        unique_categories.append(category)
                        try:
                            cursor.execute("""INSERT INTO BusinessCategory (cat_name)
                                            VALUES (%s); """,
                                            (category,))
                        except Exception as e:
                            print("Insert to businesscategory table failed for category " + category + "!",e)
                            return
                    try:
                        cursor.execute("""INSERT INTO BelongsTo (cat_name, b_id)
                                        VALUES (%s, %s); """,
                                        (category, b_id))
                    except Exception as e:
                        print("Insert to belongsto table failed for row " + str(count_line) + "!",e)
                        return

                ## BUSINESSATTRIBUTE INSERT
                for attribute, value in data["attributes"].items():
                    if type(value) == dict:
                        for subattribute, subvalue in value.items():
                            try:
                                cursor.execute("""INSERT INTO BusinessAttribute (att_name, att_value, b_id)
                                                VALUES (%s, %s, %s); """,
                                                (subattribute, subvalue, b_id))
                            except Exception as e:
                                print("Insert to businessattribute table failed for row " + str(count_line) + "!",e)
                                return
                    else:
                        try:
                            cursor.execute("""INSERT INTO BusinessAttribute (att_name, att_value, b_id)
                                            VALUES (%s, %s, %s); """,
                                            (attribute, value, b_id))
                        except Exception as e:
                            print("Insert to businessattribute table failed for row " + str(count_line) + "!",e)
                            return

                line = f.readline()
                count_line +=1
        print(count_line)
        f.close()

def insert_checkin():
    with connect_psql(psql_params) as cursor:
        # reading the JSON file
        with open(json_folder + checkin_file, 'r') as f:
            line = f.readline()
            count_line = 0
            while line:
                data = json.loads(line)
                clean_date = data['date'].split(',')[0]
                ## CHECKIN INSERT
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

def insert_users():
    with connect_psql(psql_params) as cursor:
        # reading the JSON file
        with open(json_folder + user_file,'r') as f:
            line = f.readline()
            count_line = 0
            while line:
                data = json.loads(line)
                # Generate the INSERT statement for the current business

                ## USERS INSERT
                try:
                    cursor.execute(
                        """INSERT INTO Users (user_id, name, avg_stars, create_date, funny_score, useful_score,
                                              cool_score, num_fans, tips)
                                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s); """,
                                   (data['user_id'], data['name'], data['average_stars'], data["yelping_since"], data["funny"], data["useful"], data["cool"], data["fans"], 0))

                except Exception as e:
                    print("Insert to users table failed for row " + str(count_line) + "!",e)
                    return

                line = f.readline()
                count_line +=1
        with open(json_folder + user_file,'r') as f:
            line = f.readline()
            count_line = 0
            while line:
                data = json.loads(line)

                ## FRIENDOF INSERT
                for friend_id in data["friends"]:
                    try:
                        cursor.execute("""INSERT INTO FriendOf (user_following, user_followed)
                                VALUES (%s, %s); """,
                                ([data['user_id'], friend_id]))
                    except Exception as e:
                        print("Insert to users table failed for row " + str(count_line) + "!",e)
                        return

                line = f.readline()
                count_line +=1
        print(count_line)
        f.close()

def insert_tip():
    with connect_psql(psql_params) as cursor:
        # reading the JSON file
        with open(json_folder + tip_file, 'r') as f:
            line = f.readline()
            count_line = 0
            while line:
                data = json.loads(line)

                ## TIP INSERT
                try:
                    cursor.execute(
                        """INSERT INTO Tip (time_stamp, num_likes, tip_text, b_id, user_ID)
                           VALUES (%s, %s, %s, %s, %s); """,
                        (data['date'], data['likes'], data['text'], data['business_id'], data['user_id']))
                except Exception as e:
                    print("Insert to tip table failed for row " + str(count_line) + "!", e)
                    return

                line = f.readline()
                count_line += 1
            print(count_line)
            f.close()

insert_business()
insert_checkin()
insert_users()
insert_tip()
