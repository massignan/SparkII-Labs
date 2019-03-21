# Import statements

import time
from pyspark import SparkContext
from pyspark import SparkConf
from utils import Trip

if __name__ == "__main__":
    # EXTRA - PySpark compressing and execution time
    # PySpark 2.3.3 use spark.rdd.compress setting as TRUE (DEFAULT_CONFIGS) - see context.py

    # Use 2 to uncompressed data: ")
    x = 2

    # Create a specific SparkConf
    if x == 2:
        my_conf = SparkConf().set("spark.rdd.compress", False)
        # Create the SparkContext
        sc = SparkContext(appName="Lab 4", conf=my_conf)
    else:
        # Create the SparkContext
        sc = SparkContext(appName="Lab 4")

    # RDD for the TRIPS
    input1 = sc.textFile("../data/trips/*")
    header1 = input1.first()
    # Map each row as Trip object. The constructor parse each row
    trips = input1.filter(lambda row: row != header1).map(lambda x: x.split(",")).map(Trip)

    # Calculate the average duration of trips both by start and end terminals in the trips RDD
    durationsByStart = trips.keyBy(lambda trip: trip.startTerminal).mapValues(lambda trip: float(trip.duration))
    durationsByEnd = trips.keyBy(lambda trip: trip.endTerminal).mapValues(lambda trip: float(trip.duration))

    resultsStart = durationsByStart.aggregateByKey((0, 0), lambda acc, value: (acc[0] + value, acc[1] + 1),
                                              lambda acc1, acc2: (acc1[0] + acc1[1], acc2[0] + acc2[1]))
    avgStart = resultsStart.mapValues(lambda x: x[0] / x[1])

    resultsEnd = durationsByStart.aggregateByKey((0, 0), lambda acc, value: (acc[0] + value, acc[1] + 1),
                                              lambda acc1, acc2: (acc1[0] + acc1[1], acc2[0] + acc2[1]))
    avgEnd = resultsStart.mapValues(lambda x: x[0] / x[1])

    trips.cache()

    start_time = time.time()

    avgStart.collect()
    avgEnd.collect()

    end_time = time.time()

    print("[LAB4-EXTRA] spark.rdd.compress: " + str(sc.getConf().get("spark.rdd.compress")))

    if sc.getConf().get("spark.rdd.compress"):
        print("[LAB4-EXTRA] Execution time compressed: " + str(end_time - start_time))
    else:
        print("[LAB4-EXTRA] Execution time uncompressed: " + str(end_time - start_time))
    start_time = time.time()

    # Stop here in order to be possible access the Spark UI and see the Storage tab
    print("[LAB4-EXTRA] Program stopped. Go to your browser and access Spark UI (127.0.0.1:4040) and see the Storage "
          "tab.")
    print("[LAB4-EXTRA] Type ctrl+c to end the program.")
    while True:
        pass

    sc.stop()
