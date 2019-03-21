# Import statements

import time
from pyspark import SparkContext
from pyspark import StorageLevel
from utils import Trip
import sys

if __name__ == "__main__":
    # Create the SparkContext
    sc = SparkContext(appName="Lab 4")

    # RDD for the TRIPS
    input1 = sc.textFile("../data/trips/*")
    header1 = input1.first()
    # Map each row as Trip object. The constructor parse each row
    trips = input1.filter(lambda row: row != header1).map(lambda x: x.split(",")).map(Trip)
    trips.collect()

    # Calculate the average duration of trips both by start and end terminals in the trips RDD
    durationsByStart = trips.keyBy(lambda trip: trip.startTerminal).mapValues(lambda trip: float(trip.duration))
    durationsByEnd = trips.keyBy(lambda trip: trip.endTerminal).mapValues(lambda trip: float(trip.duration))

    resultsStart = durationsByStart.aggregateByKey((0, 0), lambda acc, value: (acc[0] + value, acc[1] + 1),
                                              lambda acc1, acc2: (acc1[0] + acc1[1], acc2[0] + acc2[1]))
    avgStart = resultsStart.mapValues(lambda x: x[0] / x[1])

    resultsEnd = durationsByStart.aggregateByKey((0, 0), lambda acc, value: (acc[0] + value, acc[1] + 1),
                                              lambda acc1, acc2: (acc1[0] + acc1[1], acc2[0] + acc2[1]))
    avgEnd = resultsStart.mapValues(lambda x: x[0] / x[1])

    start_time = time.time()

    avgStart.collect()
    avgEnd.collect()

    end_time = time.time()

    print("[LAB4] Execution time without persistence: " + str(end_time - start_time))

    trips.persist(StorageLevel.MEMORY_ONLY)  # is the same as trips.cache()
    trips.collect()

    start_time = time.time()

    avgStart.collect()
    avgEnd.collect()

    end_time = time.time()

    print("[LAB4] Execution time with persistence (MEMORY_ONLY): " + str(end_time - start_time))

    # https://spark.apache.org/docs/2.3.3/rdd-programming-guide.html#rdd-persistence
    # "Note: In Python, stored objects will always be serialized with the Pickle library, so it does not matter whether
    # you choose a serialized level. The available storage levels in Python include MEMORY_ONLY, MEMORY_ONLY_2,
    # MEMORY_AND_DISK, MEMORY_AND_DISK_2, DISK_ONLY, and DISK_ONLY_2."
    #
    # You will see that the both objects are SERIALIZED in the tab Storage in the Spark UI

    # Map each row as Trip object. The constructor parse each row
    trips2 = input1.filter(lambda row: row != header1).map(lambda x: x.split(",")).map(Trip)
    trips2.collect()

    # Calculate the average duration of trips both by start and end terminals in the trips RDD
    durationsByStart2 = trips2.keyBy(lambda trip: trip.startTerminal).mapValues(lambda trip: float(trip.duration))
    durationsByEnd2 = trips2.keyBy(lambda trip: trip.endTerminal).mapValues(lambda trip: float(trip.duration))

    resultsStart2 = durationsByStart2.aggregateByKey((0, 0), lambda acc, value: (acc[0] + value, acc[1] + 1),
                                              lambda acc1, acc2: (acc1[0] + acc1[1], acc2[0] + acc2[1]))
    avgStart2 = resultsStart2.mapValues(lambda x: x[0] / x[1])

    resultsEnd2 = durationsByStart2.aggregateByKey((0, 0), lambda acc, value: (acc[0] + value, acc[1] + 1),
                                              lambda acc1, acc2: (acc1[0] + acc1[1], acc2[0] + acc2[1]))
    avgEnd2 = resultsStart2.mapValues(lambda x: x[0] / x[1])

    trips2.persist(StorageLevel.MEMORY_ONLY_SER)
    trips2.collect()

    avgStart2.collect()
    avgEnd2.collect()

    start_time = time.time()

    avgStart2.collect()
    avgEnd2.collect()

    end_time = time.time()

    print("[LAB4] Execution time with persistence (MEMORY_ONLY_SER)): " + str(end_time - start_time))

    # Using Kyro to serialize the RDD
    # See https://stackoverflow.com/questions/36278574/do-you-benefit-from-the-kryo-serializer-when-you-use-pyspark
    # "Kryo won't make a major impact on PySpark because it just stores data as byte[] objects, which are fast to
    # serialize even with Java."

    # Stop here in order to be possible access the Spark UI and see the Storage tab
    print("[LAB4] Program stopped. Go to your browser and access Spark UI (127.0.0.1:4040) and see the Storage tab.")
    print("[LAB4] Type ctrl+c to end the program.")
    while True:
        pass

    sc.stop()
