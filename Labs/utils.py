# Helper Classes Definition
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

    def __str__(self):
        return "[" + str(self.id) + "," + \
               str(self.duration) + "," + \
               str(self.startDate) + "," + \
               str(self.startStation) + "," + \
               str(self.startTerminal) + "," + \
               str(self.endDate) + "," + \
               str(self.endStation) + "," + \
               str(self.endTerminal) + "," + \
               str(self.bike) + "," + \
               str(self.subscriberType) + "," + \
               str(self.zipCode) + "," + "}"

    # def __cmp__(self, other):
    #     me = datetime.strptime(self.startDate, '%m/%d/%Y %H:%M')
    #     other = datetime.strptime(other.startDate, '%m/%d/%Y %H:%M')
    #     if me < other:
    #         return me
    #     else:
    #         return other


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
