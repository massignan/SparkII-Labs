# Import statements

from pyspark import SparkContext
from datetime import datetime
# It was necessary put my helper classes in other file because currently pyspark uses pickles to serialize the objects,
# and it don't support pickle an object in the current script ('main'). The workaround suggested by Davies Liu is to
# put the classes into a separate module and import it (http://stackoverflow.com/questions/28569374).
# Other issue with pickle is that it can not support serialize Inner Classes, this is the reason why I did not put the
# Trip and Station classes under a Utils classes. This was my first try until I have discovered the issue with pickle.
# (Juliana Oliveira - https://medium.com/@jwnx/multiprocessing-serialization-in-python-with-pickle-9844f6fa1812)
from utils import *


# Helper functions
def average(iterable):
    acc = 0
    count = 0
    for each in iterable:
        acc = acc + each
        count = count + 1
    return acc/count


def getKey(trip):
    return datetime.strptime(trip.startDate, '%m/%d/%Y %H:%M')


if __name__ == "__main__":
    # Create the SparkContext
    sc = SparkContext(appName="Lab 3")

    # RDD for the TRIPS
    input1 = sc.textFile("../data/trips/*")
    header1 = input1.first()
    # Map each row as Trip object. The constructor parse each row
    trips = input1.filter(lambda row: row != header1).map(lambda x: x.split(",")).map(Trip)

    # RDD for the STATIONS
    input2 = sc.textFile("../data/stations/*")
    header2 = input2.first()
    # Map each row as Station object. The constructor parse each row
    stations = input2.filter(lambda row: row != header2).map(lambda x: x.split(",")).map(Station)

    # Create a <key,value> data, where the key is the start station and the value is only the duration
    byStartTerminal = trips.keyBy(lambda x: x.startStation)
    durationByStart = byStartTerminal.mapValues(lambda x: float(x.duration))

    # Calculating the average trip duration by start station
    # Using groupByKey - It should be avoided because it cause unnecessary shuffles
    grouped = durationByStart.groupByKey().mapValues(lambda x: average(x))

    # Using aggregateByKey
    result = durationByStart.aggregateByKey((0, 0), lambda acc, val: (acc[0] + val, acc[1] + 1),
                                            lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))
    finalAvg = result.mapValues(lambda x: x[0]/x[1])

    # Find the first trip at each terminal
    # Using groupByKey - It should be avoided because it cause unnecessary shuffles
    # Sort the tuples of values by getKey (startDate) and get the first object.
    firstGrouped = byStartTerminal.groupByKey().mapValues(lambda x: (sorted(x, key=getKey))[0])

    # Using reduceByKey

    # Print results
    print("[LAB3] trips rows count: " + str(trips.count()))
    print("[LAB3] stations rows count: " + str(stations.count()))

    print("[LAB3] byStartTerminal rows count: " + str(byStartTerminal.count()))
    print("[LAB3] durationByStart rows count: " + str(durationByStart.count()))

    print("[LAB3] grouped rows count: " + str(grouped.count()))
    print("[LAB3] groupedByKey average: " + str(grouped.take(10)))

    print("[LAB3] result rows count: " + str(result.count()))
    print("[LAB3] result: " + str(result.take(10)))

    print("[LAB3] finalAvg rows count: " + str(finalAvg.count()))
    print("[LAB3] finalAvg: " + str(finalAvg.take(10)))

    print("[LAB3] firstGrouped rows count: " + str(firstGrouped.count()))
    firstGroupedList = firstGrouped.take(10)
    for each in firstGroupedList:
        print("[LAB3] firstGrouped: " + str(each[0]) + " - " + str(each[1]))
    print("[LAB3] " + firstGrouped.toDebugString())

    sc.stop()
