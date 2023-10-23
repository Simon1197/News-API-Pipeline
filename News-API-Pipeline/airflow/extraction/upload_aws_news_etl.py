import configparser
import pathlib
import psycopg2
import sys
from validation import validate_input
from psycopg2 import sql

"""
Part of DAG. Upload S3 CSV data to Redshift. Takes one argument of format YYYYMMDD. This is the name of 
the file to copy from S3. Script will load data into temporary table in Redshift, delete 
records with the same post ID from main table, then insert these from temp table (along with new data) 
to main table. This means that if we somehow pick up duplicate records in a new DAG run,
the record in Redshift will be updated to reflect any changes in that record, if any (e.g. higher score or more comments).
"""

# Parse our configuration file
script_path = pathlib.Path(__file__).parent.resolve()
parser = configparser.ConfigParser()
parser.read(f"{script_path}/configuration.conf")

# Store our configuration variables
USERNAME = parser.get("aws_config", "redshift_username")
PASSWORD = parser.get("aws_config", "redshift_password")
HOST = parser.get("aws_config", "redshift_hostname")
PORT = parser.get("aws_config", "redshift_port")
REDSHIFT_ROLE = parser.get("aws_config", "redshift_role")
DATABASE = parser.get("aws_config", "redshift_database")
BUCKET_NAME = parser.get("aws_config", "bucket_name")
ACCOUNT_ID = parser.get("aws_config", "account_id")
TABLE_NAME = "news1"

# Check command line argument passed
try:
    output_name = sys.argv[1]
except Exception as e:
    print(f"Command line argument not passed. Error {e}")
    sys.exit(1)

# Our S3 file & role_string
file_path = f"s3://{BUCKET_NAME}/{output_name}.csv"
role_string = f"arn:aws:iam::{ACCOUNT_ID}:role/{REDSHIFT_ROLE}"

# Create Redshift table if it doesn't exist
# sql_create_table = sql.SQL(
#     """CREATE TABLE IF NOT EXISTS {table} (
#                             id varchar PRIMARY KEY,
#                             title varchar(max),
#                             num_comments int,
#                             score int,
#                             author varchar(max),
#                             created_utc timestamp,
#                             url varchar(max),
#                             upvote_ratio float,
#                             over_18 bool,
#                             edited bool,
#                             spoiler bool,
#                             stickied bool
#                         );"""
# ).format(table=sql.Identifier(TABLE_NAME))

sql_create_table = sql.SQL(
    """CREATE TABLE IF NOT EXISTS {table} (
                        id varchar NOT NULL,
                        author varchar NOT NULL,
                        title varchar(max) NOT NULL,
                        published_at timestamp NOT NULL,
                        description varchar(max) NOT NULL,
                        compound_sentiment float,
                        positive_sentiment float,
                        negative_sentiment float,
                        neutral_sentiment float,
                        sentiment_category varchar NOT NULL,
                        url varchar(max) NOT NULL,
                        source_id varchar NOT NULL,
                        source_name varchar PRIMARY KEY,
                        urlToImage varchar(max) NOT NULL,
                        content text NOT NULL,
                        language varchar NOT NULL,
                        category varchar NOT NULL,
                        country varchar NOT NULL,
                        Keywords varchar NOT NULL,
                        NumKeywordsInTitle INT NOT NULL,
                        NumKeywordsInContent INT NOT NULL
    );"""
).format(table=sql.Identifier(TABLE_NAME))

# If ID already exists in table, we remove it and add new ID record during load.
create_temp_table = sql.SQL(
    "CREATE TEMP TABLE our_staging_table (LIKE {table});"
).format(table=sql.Identifier(TABLE_NAME))

sql_copy_to_temp = f"""
    COPY our_staging_table (id, author, title, published_at, description, 
                            compound_sentiment, positive_sentiment, negative_sentiment, neutral_sentiment, 
                            sentiment_category, url, source_id, source_name, urlToImage, content, language, 
                            category, country, Keywords, NumKeywordsInTitle, NumKeywordsInContent)
    FROM '{file_path}' iam_role '{role_string}'
    IGNOREHEADER 1 DELIMITER ',' CSV;
"""

print(f"Copying data from {file_path} to our_staging_table")
print(f"SQL statement: {sql_copy_to_temp}")




delete_from_table = sql.SQL(
    "DELETE FROM {table} USING our_staging_table WHERE {table}.id = our_staging_table.id;"
).format(table=sql.Identifier(TABLE_NAME))
insert_into_table = sql.SQL(
    "INSERT INTO {table} SELECT * FROM our_staging_table;"
).format(table=sql.Identifier(TABLE_NAME))
drop_temp_table = "DROP TABLE our_staging_table;"


def main():
    """Upload file form S3 to Redshift Table"""
    validate_input(output_name)
    rs_conn = connect_to_redshift()
    print('it works1')
    load_data_into_redshift(rs_conn)
    print('it works2')

def connect_to_redshift():
    """Connect to Redshift instance"""
    try:
        rs_conn = psycopg2.connect(
            dbname=DATABASE, user=USERNAME, password=PASSWORD, host=HOST, port=PORT
        )
        return rs_conn 
    except Exception as e:
        print(f"Unable to connect to Redshift. Error {e}")
        sys.exit(1)


def load_data_into_redshift(rs_conn):
    """Load data from S3 into Redshift"""
    with rs_conn:

        cur = rs_conn.cursor()
        print('it works4')
        cur.execute(sql_create_table)
        print('it works5')
        cur.execute(create_temp_table)
        print('it works6')
        cur.execute(sql_copy_to_temp)
        print('it works7')
        cur.execute(delete_from_table)
        print('it works8')
        cur.execute(insert_into_table)
        print('it works9')
        cur.execute(drop_temp_table)
        print('it works10')

        # Commit only at the end, so we won't end up
        # with a temp table and deleted main table if something fails
        rs_conn.commit()


if __name__ == "__main__":
    main()