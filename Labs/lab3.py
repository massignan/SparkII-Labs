# Import statements

from pyspark import SparkContext

# Helper Classes Definition


class Utils(object):
    def __init__(self):
        pass

    class Trip(object):
        # Trip constructor parse input data
        def __init__(self, x):
            self.id = x[0]
            self.duration = x[1]
            self.startDate = x[2]
            self.startStation = x[3]
            self.startTerminal = x[4]
            self.endDate = x[5]
            self.endStation = x[6]
            self.endTerminal = x[7]
            self.bike = x[8]
            self.subscriberType = x[9]
            self.zipCode = x[10]

    class Station(object):
        # Station constructor parse input data
        def __init__(self, x):
            self.id = x[0]
            self.name = x[1]
            self.lat = x[2]  # latitude
            self.lon = x[3]  # longitude
            self.docks = x[4]
            self.landmark = x[5]
            self.installDate = x[6]

# Helper functions


def average(iterable):
    acc = 0
    count = 0
    for each in iterable:
        acc = acc + each
        count = count + 1
    return acc/count


if __name__ == "__main__":
    # Create the SparkContext
    sc = SparkContext(appName="Lab 3")

    # RDD for the TRIPS
    input1 = sc.textFile("../data/trips/*")
    header1 = input1.first()
    # Map each row as Trip object. The constructor parse each row
    trips = input1.filter(lambda row: row != header1).map(lambda x: x.split(",")).map(Utils.Trip)

    # RDD for the STATIONS
    input2 = sc.textFile("../data/stations/*")
    header2 = input2.first()
    # Map each row as Station object. The constructor parse each row
    stations = input2.filter(lambda row: row != header2).map(lambda x: x.split(",")).map(Utils.Station)

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
    firstGrouped = byStartTerminal.groupByKey().mapValues(lambda x: x.startDate)

    # Using reduceByKey

    # Print results
    print("[LAB3] trips rows count: " + str(trips.count()))
    print("[LAB3] stations rows count: " + str(stations.count()))

    print("[LAB3] byStartTerminal rows count: " + str(byStartTerminal.count()))
    print("[LAB3] durationByStart rows count: " + str(durationByStart.count()))

    print("[LAB3] grouped rows count: " + str(grouped.count()))
    print("[LAB3] groupedByKey average" + str(grouped.take(10)))

    print("[LAB3] result rows count: " + str(result.count()))
    print("[LAB3] result" + str(result.take(10)))

    print("[LAB3] finalAvg rows count: " + str(finalAvg.count()))
    print("[LAB3] finalAvg" + str(finalAvg.take(10)))

    sc.stop()
