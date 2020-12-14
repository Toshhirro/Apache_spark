import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd

# Column names
logColumns = ["logdate","logtime","size","r_version","r_arch","r_os","package","version","country","ip_id"]

# Input Data as a list of values
listInput = [
 ("2012-10-01","02:13:48",1061394,"2.15.1","i686","linux-gnu","Amelia","1.6.3","AU",1),
 ("2012-10-01","02:37:34",868687,"2.15.0","x86_64","linux-gnu","RCurl","1.95-0","US",3),
 ("2012-10-01","04:06:10",1023,"NA","NA","NA","NA","NA","US",4),
 ("2012-10-01","08:17:26",2094435,"2.15.1","x86_64","linux-gnu","mosaic","0.6-2","US",2),
 ("2012-10-01","08:29:01",868687,"2.15.1","x86_64","linux-gnu","RCurl","1.95-0","US",2),
 ("2012-10-01","08:28:54",2094449,"2.15.1","x86_64","linux-gnu","mosaic","0.6-2","US",2)
]

# Finding Downloads by country, package using RDD API

def getDownloadsByCountry(inputRDD):
    # <enter your code here> #
    outputRDD = inputRDD.map(lambda x: (x[8],x[6])) \
      .map(lambda x: ((x), 1)) \
      .reduceByKey(lambda a,b: a+b) \
      .sortByKey(ascending=False)
    return outputRDD

# Increment Accumulator variable using RDD API
# Below function will increment the accumulator variable for every record having size > 1000000

def getDownloadCount(inputRDD,accDownloadCount):
    # <enter your code here> #
    accDownloadCount+=inputRDD.map(lambda x: x[2]).filter(lambda x: x>1000000).count()
    return accDownloadCount.value

# Filter records
# Below function will read the input Dataframe having records in listInput and filter out records
# where version = "NA" and package = "NA" 

def filterRecords(inputDF):
   # <enter your code here> #
    outputDF = df.filter((col('package') != "NA") & (col('version')!='NA')).sort("logdate","logtime")
    return outputDF

# Add download_type column
# Below function will add a new column called download_type to the input dataframe

def addDownloadType(inputDF):
    # <enter your code here> #
    outputDF = inputDF.withColumn("download_type",when((col('size')<= 1000000),'small').otherwise('large'))
    return outputDF

# Total number of downloads for each package

def getPackageCount(inputDF):
    # <enter your code here> #
    outputDF = inputDF.groupBy("package").agg(count("package").alias('package_count'))
    return outputDF

# Aggregate input dataframe based on logdate, download_type and find total_size,average_size

def aggDownloadType(inputDF):
    # <enter your code here> #
    outputDF = inputDF.groupBy("download_type","logdate").agg(sum("size").alias('total_size'), round(avg("size"),0).alias('average_size'))
    return outputDF

# Do not modify the code after this line
#-----------------------------------------------------------------#

if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("PySpark samples").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Initialize Accumulator Variable
    accDownloadCount = spark.sparkContext.accumulator(0)

    inputRDD = spark.sparkContext.parallelize(listInput)
    downloadsByCountryRDD = getDownloadsByCountry(inputRDD)

    print("Output : Downloads By Country")
    print("###############################################")
    print(downloadsByCountryRDD.collect())

    print("Output : Download Count")
    print("###############################################")
    downloadCount = getDownloadCount(inputRDD,accDownloadCount)
    print("Total Downloads of size > 1000000 :" + str(downloadCount))

    df_pandas = pd.DataFrame.from_dict(listInput)
    df = spark.createDataFrame(df_pandas, logColumns)
    
    print(" Output : Filter records")
    print("###############################################")
    df_filter = filterRecords(df)
    df_filter.show()

    print("Output : Add DownloadType column")
    print("###############################################")
    df_downloadType = addDownloadType(df_filter)
    df_downloadType.show()

    df_package_count = getPackageCount(df_downloadType)

    print("Output : Count by Package")
    print("###############################################")
    df_package_count.show()

    df_agg_download_type = aggDownloadType(df_downloadType)

    print("Output : Total and Average Size")
    print("###############################################")
    df_agg_download_type.show()
 