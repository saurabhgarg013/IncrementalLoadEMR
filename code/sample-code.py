from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()
data = [(1, "Alice"), (2, "Bob")]
df = spark.createDataFrame(data, ["id", "name"])
df.show()





Install the Jupyter extension

Install the Python extension

Ensure you have PySpark installed locally if needed