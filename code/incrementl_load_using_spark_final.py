from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()
data = [(1, "Alice"), (2, "Bob")]
df = spark.createDataFrame(data, ["id", "name"])
df.show()


%%configure -f
{
    "conf": {
        "spark.jars": "s3://techno-data-bucket/lib/postgresql-42.6.0.jar"
    }
}


# config.py
POSTGRES_CONFIG = {
    "host": "database-1.czo6aq40aeuq.ap-south-1.rds.amazonaws.com",
    "port": "5432",
    "database": "postgres",
    "user": "postgres",
    "password": "Password"
}

REDSHIFT_CONFIG = {
    "host": "redshift-cluster-1.cjc47fqwyen7.ap-south-1.redshift.amazonaws.com",
    "port": "5439",
    "database": "dev",
    "user": "awsuser",
    "password": "Password1"
}



	



from pyspark.sql import SparkSession
def create_spark_session(app_name="DailyLoad"):
    """Create and return a Spark session."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    
    return spark

def get_total_record_count(spark, source_table, start_date=None, end_date=None):
    """Get the total number of records to be loaded from PostgreSQL."""
    jdbc_url = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
    properties = {
        "user": POSTGRES_CONFIG["user"],
        "password": POSTGRES_CONFIG["password"],
        "driver": "org.postgresql.Driver"
    }

    query = f"(SELECT COUNT(*) FROM {source_table}"
    if start_date and end_date:
        query += f" WHERE updated_at >= '{start_date}' AND updated_at < '{end_date}'"
    query += ") AS tmp"

    df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
    total_count = df.collect()[0][0]
    print(f"Total records to load: {total_count}")
    return total_count
	



spark=create_spark_session()
source_table='bike_stores.staffs'

get_total_record_count(spark, source_table, start_date=None, end_date=None)


import traceback
from pyspark.sql.functions import max as spark_max

def get_max_update_date(spark, target_table):
    """
    Fetch the maximum update date from Redshift to identify new records.
    """
    try:
        redshift_jdbc_url = f"jdbc:redshift://{REDSHIFT_CONFIG['host']}:{REDSHIFT_CONFIG['port']}/{REDSHIFT_CONFIG['database']}"
        properties = {
            "user": REDSHIFT_CONFIG["user"],
            "password": REDSHIFT_CONFIG["password"]
        }
        
        df = spark.read.jdbc(redshift_jdbc_url, target_table, properties=properties)
        
        max_date = df.select(spark_max("last_update")).collect()[0][0]
        
        return max_date
    
    except Exception as e:
        error_msg = str(e)
        
        if "relation" in error_msg and "does not exist" in error_msg:
            print(f"Table '{target_table}' does not exist in Redshift.")
        else:
            print(f"Error fetching max update date from Redshift: {e}")
        
        #traceback.print_exc()

        return None

spark=create_spark_session()
target_table = 'dwh.dim_staff'
#dwh.dim_staff 
#dwh.staffs_stagging
get_max_update_date(spark, target_table)

import traceback
def fetch_data_from_postgres(spark, source_table, max_date):
    """
    Fetch incremental data from PostgreSQL based on the max update date.
    """
    try:
        postgres_jdbc_url = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
        properties = {
            "user": POSTGRES_CONFIG["user"],
            "password": POSTGRES_CONFIG["password"],
            "driver": "org.postgresql.Driver"
        }
        
        if max_date:
            query = f"SELECT * FROM {source_table} WHERE last_update > '{max_date}'"
        else:
            query = f"SELECT * FROM {source_table}"
        
        df = spark.read.jdbc(postgres_jdbc_url, f"({query}) as src", properties=properties)
        
        return df
    
    except Exception as e:
        if "column" in str(e) or "syntax error" in str(e):
            print("SQL error — possible issue with column names or query syntax.")
        else:
            print("Unknown error while fetching data.")
            print(f"Exception details: {e}")
        
        return None
#     except Exception as e:
#         print(f"Error fetching data from PostgreSQL: {e}")
        
        
#         return None

spark=create_spark_session()
source_table='bike_stores.staffs'
max_date=''
# 2025-03-19 20:46:16.028
# 2025-03-19 20:46:16.028
fetch_data_from_postgres(spark, source_table, max_date)
#df.show()


def write_to_redshift(df, table_name, mode):
    """
    Write DataFrame to Redshift with error handling.
    """
    try:
        redshift_jdbc_url = f"jdbc:redshift://{REDSHIFT_CONFIG['host']}:{REDSHIFT_CONFIG['port']}/{REDSHIFT_CONFIG['database']}"
        properties = {
            "user": REDSHIFT_CONFIG["user"],
            "password": REDSHIFT_CONFIG["password"],
            "tempdir": "s3://redshift-data-temp-1/temp/"
        }

        df.write \
            .format("io.github.spark_redshift_community.spark.redshift") \
            .option("url", redshift_jdbc_url) \
            .option("dbtable", table_name) \
            .option("tempdir", properties["tempdir"]) \
            .option("forward_spark_s3_credentials", "true") \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .mode(mode) \
            .save()
        
        return True  # Success
    except Exception as e:
        print(f"Error writing to Redshift: {e}")
        return False

def incremental_load(spark, source_table, target_table, mode):
    """
    Perform incremental load from PostgreSQL to Redshift.
    """
    try:
        print('Loading started')
        max_date = get_max_update_date(spark, target_table)
        print(f"Max update date in {target_table}: {max_date}")

        incremental_df = fetch_data_from_postgres(spark, source_table, max_date)
        
        if incremental_df is None or incremental_df.count() == 0:
            print("No new data found. Skipping load.")
            return  
        
        print(f"Records to be loaded: {incremental_df.count()}")
        success = write_to_redshift(incremental_df, target_table, mode)

        if not success:
            print("Load failed. Logging failed batch for retry.")
            incremental_df.write.mode("append").parquet("s3://redshift-data-temp-1/failed-batch/")
        else:
            print("Incremental load completed successfully!")
    except Exception as e:
        print(f"Error in incremental_load function: {e}")


spark=create_spark_session()
source_table='bike_stores.staffs'
target_table = 'dwh.staffs_stagging'
mode = 'overwrite'


incremental_load(spark, source_table, target_table, mode)

# Define table names and primary key column
source_table = "dwh.staffs_stagging"
target_table = "dwh.staffs_main"
primary_key = "staff_id"  # Replace with your actual primary key column name



# Call the stored procedure with dynamic table names and primary key
call_sql = f"CALL merge_staging_to_main('{target_table}', '{source_table}', '{primary_key}')"
try:
    #redshift_jdbc_url = f"jdbc:redshift://{REDSHIFT_CONFIG['host']}:{REDSHIFT_CONFIG['port']}/{REDSHIFT_CONFIG['database']}"
    redshift_jdbc_url = (
        f"jdbc:redshift://{REDSHIFT_CONFIG['host']}:{REDSHIFT_CONFIG['port']}/{REDSHIFT_CONFIG['database']}"
        f"?user={REDSHIFT_CONFIG['user']}&password={REDSHIFT_CONFIG['password']}"
    )
    connection = spark.sparkContext._jvm.java.sql.DriverManager.getConnection(redshift_jdbc_url)
    statement = connection.createStatement()
    statement.execute(call_sql)
    print(f"Successfully merged {source_table} into {target_table} using primary key {primary_key}")
except Exception as e:
    print(f"Error executing procedure: {str(e)}")
finally:
    if 'statement' in locals():
        statement.close()
    if 'connection' in locals():
        connection.close()




