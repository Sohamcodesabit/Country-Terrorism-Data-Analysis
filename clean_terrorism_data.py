import os
os.environ["HADOOP_HOME"] = "C:/Spark/hadoop"
os.environ["PYSPARK_PYTHON"] = "C:/Users/Soham/AppData/Local/Programs/Python/Python312/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Users/Soham/AppData/Local/Programs/Python/Python312/python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, trim, expr

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, trim, expr

# ✅ Initialize Spark
spark = SparkSession.builder \
    .appName("Clean Global Terrorism Data") \
    .getOrCreate()

# ✅ Load dataset
input_path = "C:/Users/Soham/OneDrive/Desktop/IR/Country-Terrorism-Data-Analysis/datasets/globalterrorismdb.csv"   # Change if your file is elsewhere
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

print("Initial rows:", df.count())
print("Initial columns:", len(df.columns))

# ✅ Step 1: Trim all string columns (remove extra spaces)
for c in df.columns:
    df = df.withColumn(c, trim(col(c)))

# ✅ Step 2: Clean numeric columns (many contain text or empty strings)
numeric_cols = [
    "nkill", "nwound", "nperps", "nperpcap", "property", "weaptype1", 
    "weapsubtype1", "attacktype1", "targtype1", "nreleased"
]

for col_name in numeric_cols:
    df = df.withColumn(
        col_name,
        when(col(col_name).rlike("^[0-9]+$"), col(col_name).cast("int"))
        .otherwise(expr("try_cast(" + col_name + " as int)"))
    )

# ✅ Step 3: Handle missing values
df = df.fillna({
    "nkill": 0,
    "nwound": 0,
    "nperps": 0,
    "property": 0
})

# ✅ Step 4: Remove duplicates
df = df.dropDuplicates()

# ✅ Step 5: Drop useless or completely null columns
non_null_cols = [c for c in df.columns if df.select(c).na.drop().count() > 0]
df = df.select(non_null_cols)

# ✅ Step 6: Optional — remove irrelevant text-heavy columns
drop_cols = [
    "summary", "addnotes", "scite1", "scite2", "scite3",
    "motive", "propcomment", "weapdetail"
]
df = df.drop(*[c for c in drop_cols if c in df.columns])

# ✅ Step 7: Save cleaned dataset
output_path = "cleaned_terrorism_data.csv"
df.write.mode("overwrite").option("header", True).csv(output_path)

print("✅ Cleaning complete.")
print("Rows after cleaning:", df.count())
print("Saved to:", output_path)

spark.stop()
