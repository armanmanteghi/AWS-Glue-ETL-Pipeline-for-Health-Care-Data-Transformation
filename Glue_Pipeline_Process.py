import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lower, trim, to_date
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from S3
input_path = 's3://health-care-dataset-project/health-care-data/'
df = spark.read.format("csv").option("header", "true").load(input_path)

# Standardize column names
df = df.toDF(*[c.lower().replace(" ", "_") for c in df.columns])

# Normalize text data
df = df.withColumn("name", lower(trim(col("name"))))
df = df.withColumn("medical_condition", lower(trim(col("medical_condition"))))
df = df.withColumn("doctor", lower(trim(col("doctor"))))
df = df.withColumn("hospital", lower(trim(col("hospital"))))
df = df.withColumn("insurance_provider", lower(trim(col("insurance_provider"))))
df = df.withColumn("medication", lower(trim(col("medication"))))
df = df.withColumn("test_results", lower(trim(col("test_results"))))

# Handle missing values
df = df.na.fill({
    "age": 0, 
    "gender": "unknown", 
    "blood_type": "unknown", 
    "medical_condition": "unknown", 
    "date_of_admission": "1900-01-01", 
    "doctor": "unknown", 
    "hospital": "unknown", 
    "insurance_provider": "unknown", 
    "billing_amount": 0.0, 
    "room_number": 0, 
    "admission_type": "unknown", 
    "discharge_date": "1900-01-01", 
    "medication": "unknown", 
    "test_results": "unknown"
})

# Convert date columns
df = df.withColumn("date_of_admission", to_date(col("date_of_admission"), "yyyy-MM-dd"))
df = df.withColumn("discharge_date", to_date(col("discharge_date"), "yyyy-MM-dd"))

# Ensure numerical columns are in the correct format
df = df.withColumn("age", col("age").cast("int"))
df = df.withColumn("billing_amount", col("billing_amount").cast("double"))
df = df.withColumn("room_number", col("room_number").cast("int"))

# Correct inconsistent data
df = df.withColumn("gender", lower(trim(col("gender"))))
df = df.withColumn("admission_type", lower(trim(col("admission_type"))))

# Coalesce to a single partition to write to a single CSV file
df = df.coalesce(1)

# Write the transformed data back to S3 in CSV format
output_path = 's3://health-care-dataset-project/transformed-data/'
df.write.mode('overwrite').option("header", "true").csv(output_path)

job.commit()