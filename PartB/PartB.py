from mrjob.job import MRJob
import math
import time
import re

class PartB(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7 :
                mon_value = float(fields[3])
                address=fields[2]
                yield(address, mon_value)
        except:
            pass

    def combiner(self, address, mon_value):
        yield(address,sum(mon_value))

    def reducer(self, address, mon_value):
        yield(address,sum(mon_value))

if __name__ == '__main__':
    PartB.JOBCONF={'mapreduce.job.reduces':'10'}
    PartB.run()
