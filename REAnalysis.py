import sys
from pyspark import SparkContext

# Display Property ID, Size, Price for given location
# Sample Output:
# (u'1500741', 1800, 324900), 
# (u'1633709', 750, 164900), 

def getProperiesForLocation(inputRDD, location):
    # <enter your code here> #
    outputRDD = inputRDD.filter(lambda x: x[1]==location)
    return outputRDD

# Display Unique Locations

def getUniqueLocations(inputRDD):
    # <enter your code here> #
    outputRDD = inputRDD.map(lambda x: x[1]).distinct()
    return outputRDD

# Compute Price of Property
# Sample Output Record: (u'1629848', u'Thomas County', 29000, 563565.0)

def getPropertyWithPrice(inputRDD):
    # <enter your code here> #
    outputRDD = inputRDD.map(lambda x: (x[0],x[1],x[2],round(float(x[5])*float(x[6]),0)))
    return outputRDD

# Get Property ID, count, sorted in descending order of count

def getPropertyCountByLocation(inputRDD):
    # <enter your code here> #
    outputRDD = outputRDD = inputRDD.map(lambda x: ((x[1]), 1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x:x[1],ascending=False)
    return outputRDD

# Set Operations on RDD
# Get (Property ID, Location) for properties having 3 bedrooms and more than 2 bathrooms and costing less than USD 150000

def getRequiredProperties(inputRDD):
    # <enter your code here> #
    temp1 = sc.parallelize(inputRDD.filter(lambda x: (x[3]=='3') & (x[2]<'150000')).map(lambda x:(x[0],x[1])).collect())
    temp2 = sc.parallelize(inputRDD.filter(lambda x: (x[4]=='2') & (x[2]<'150000')).map(lambda x:(x[0],x[1])).collect())
    outputRDD = temp1.intersection(temp2)
    return outputRDD

if __name__ == "__main__":
    
    # create Spark Context
    sc = SparkContext("local", "Real Estate Analysis")

    # Create Initial RDD from File
    inputFile = "RealEstate.txt"
    inputRDD = sc.textFile(inputFile)
    inputRDD=sc.parallelize(inputRDD.map(lambda x: x.split('|')).filter(lambda x:x[1]!='Location').collect())
    # Properties for given location
    location = "La Oceana"
    propertiesByLocRDD = getProperiesForLocation(inputRDD, location)

    print("Properties for Location - %s" %location)
    print("-----------------------------------------------------------------------------------")
    print(propertiesByLocRDD.collect())

    # Display Unique Locations
    uniqueLocationsRDD = getUniqueLocations(inputRDD)
    print("Unique Locations")
    print("-----------------------------------------------------------------------------------")
    print(uniqueLocationsRDD.collect())

    #  3: Compute Price of Property
    propertyWithPriceRDD = getPropertyWithPrice(inputRDD)
    print("Properties with Price")
    print("-----------------------------------------------------------------------------------")
    print(propertyWithPriceRDD.collect())
    
    #  4: Get Property ID, count, sorted in descending order of count
    propertyCountRDD = getPropertyCountByLocation(inputRDD)
    print("Properties count by Location (sorted by count)")
    print("-----------------------------------------------------------------------------------")
    print(propertyCountRDD.collect())


    #  5: Properties with 3 bedrooms and alleast 2 bathrooms
    reqdPropertyListRDD = getRequiredProperties(inputRDD)
    print("Properties with 3 bedrooms and alleast 2 bathrooms")
    print("-----------------------------------------------------------------------------------")
    print(reqdPropertyListRDD.collect())
