#%%
print("PYHTON LOG: Importing Libraries")

import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
import pyspark.pandas as ps
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings("ignore")
from dotenv import load_dotenv
import os
load_dotenv()
print("PYTHON LOG: Imported libraries successfully")

HOST=os.getenv('HOST')
PORT=os.getenv('PORT')
DATABASE=os.getenv('DATABASE')
USER=os.getenv('USER')
PASSWORD=os.getenv('PASSWORD')

jdbc_url="jdbc:postgresql://{}/{}".format(HOST,DATABASE)
jdbc_driver_path = "C:/spark/jars/postgresql-42.7.3.jar"

print('PYTHON LOG: Creating Spark Session')
# Initialize Spark session
spark = SparkSession \
    .builder \
    .config("sparkk.jars", jdbc_driver_path) \
    .config('spark.driver.extraClassPath', jdbc_driver_path) \
    .appName("Data ETL") \
    .getOrCreate()
print('PYTHON LOG: Created Spark Session Successfully')
    
str_engine_wh = "postgresql://" + USER + ":" + PASSWORD + "@" + HOST + ":" + PORT + "/" + DATABASE
print('PYHTON LOG: attempting database connection')
db_engine = create_engine(str_engine_wh)
print('PYHTON LOG: Database connection successful')

print("PYTHON LOG: Loading data")
# Load data using pyspark.pandas
path = "csv_files/german_credit_data.csv"
df = ps.read_csv(path)

# Convert to PySpark DataFrame using to_spark()
sdf = df.to_spark()

print("PYTHON LOG: Saving to database")
sdf.write.format('jdbc') \
    .mode('overwrite') \
    .option("url", jdbc_url) \
    .option("user", USER) \
    .option("password", PASSWORD) \
    .option('dbtable', 'stage_loan') \
    .option("driver", "org.postgresql.Driver") \
    .save()
    
print('PYTHON LOG: Saved to database successfully')
#%%
print("PYTHON LOG: Stopping Spark Session")
spark.stop()
print("PYTHON LOG: Spark Session stopped successfully")
#%%