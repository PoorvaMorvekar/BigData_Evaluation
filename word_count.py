from pyspark.sql import SparkSession
s = SparkSession.builder.appName('WordCountApp').getOrCreate()

bigdatafile = s.read.text('s3://finalpracticalfeb/shakespeare_data.txt').coalesce(10)

bigdatafile.show()

bigdatafile.withColumnRenamed('value', 'word').createTempView("data")


# Count the number of words
print(bigdatafile.count())

# First three values in text file
print(bigdatafile.take(3))

# 10 longest words and their length
length = s.sql('SELECT word, length(word) AS len FROM data ORDER BY len DESC LIMIT 10')
length.show()

# 10 highest occurred words and count of occurrence
count = s.sql("SELECT word, count(word) AS word_count FROM data GROUP BY word ORDER BY word_count DESC LIMIT 10")
count.show()

