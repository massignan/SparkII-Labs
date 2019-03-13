# Import statements
from pyspark import SparkContext

if __name__ == "__main__":
    # Create the SparkContext
    sc = SparkContext(appName="Lab 2")

    # RDD for the TRIPS
    input1 = sc.textFile("../data/trips/*")
    header1 = input1.first()
    trips = input1.filter(lambda row: row != header1).map(lambda cell: cell.split(","))

    # RDD for the STATIONS (k,v) where the key is the Station ID (first row)
    input2 = sc.textFile("../data/stations/*")
    header2 = input2.first()
    # Repartitioning stations - pyspark uses Hash Partiotioner as defaulf partition function
    stations = input2.filter(lambda row: row != header2).map(lambda cell: cell.split(",")).keyBy(lambda x: x[0])\
        .partitionBy(trips.getNumPartitions())

    # RDD joining TRIPS and STATIONS
    startTrips = stations.join(trips.keyBy(lambda x: x[4]))
    endTrips = stations.join(trips.keyBy(lambda x: x[7]))

    print(startTrips.toDebugString())
    print(endTrips.toDebugString())

    print("[LAB2] startTrips count: " + str(startTrips.count()))
    print("[LAB2] endTrips count: " + str(endTrips.count()))

    print("[LAB2] trips partitions: " + str(trips.getNumPartitions()))
    print("[LAB2] default number of partitions: " + str(sc.defaultParallelism))

    # while True:
    #    foo = True

    sc.stop()
