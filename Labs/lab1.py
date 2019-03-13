# Import statements

from pyspark import SparkContext

if __name__ == "__main__":
    # Create the SparkContext
    sc = SparkContext(appName="Lab 1")

    trips = sc.textFile("../data/trips/*")
    print("Total trips rows : " + str(trips.map(lambda each: each.split(",")).count()))

    sc.stop()