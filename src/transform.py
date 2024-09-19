from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, count, when, isnan, mean, round, trim, ltrim, rtrim, regexp_replace


spark = SparkSession.builder.getOrCreate()

df = spark.read.csv('C:/Users/Ivan/Downloads/Titanic-Dataset.csv', header=True, inferSchema=True)


#print(df.show())

na_counts = df.select([count(when(isnull(c) | isnan(c),c)).alias(c) for c in df.columns])
na_counts.show()

average_age = df.select(mean(col("Age"))).collect()[0][0]
print(average_age)

df_filled = df.fillna(value={'Age': average_age})
#df_filled.show()

df_age_formatted = df_filled.withColumn('Age', col('Age').cast('integer'))
#df_age_formatted.show()

df_fare_formatted = df_age_formatted.withColumn('Fare', round(col('Fare'),2))
#df_fare_formatted.show(truncate=False)

df_fare_formatted = df_fare_formatted.withColumn("Survived", when(df["Survived"] == 1, "Yes").otherwise("No"))
df_fare_formatted.show(truncate=False)

#df_name_formatted = df_age_formatted.withColumn('Name', trim(col('Name')))
#df_name_formatted.show(truncate=False)

#df_name_formatted = df_fare_formatted.withColumn("Name", regexp_replace(col("Name"), r"[\u200B-\u200D\uFEFF]", ""))
#df_name_formatted.show()

#df_cleaned = df_name_formatted.withColumn("Name", regexp_replace(col("Name"), u"\u00A0", ""))
#df_cleaned.show()

#df_cleaned = df_cleaned.withColumn("Name", regexp_replace(col("Name"), u"\u200B", ""))
#df_cleaned.show()

#df_cleaned = df_cleaned.withColumn("Name", regexp_replace(col("Name"), r'[^\x20-\x7E]', ''))
#df_cleaned.show(truncate=False)
