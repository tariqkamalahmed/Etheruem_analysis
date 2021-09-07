from mrjob.job import MRJob
import math
import time
import re

class PartA(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7 :
                time_epoch=int(fields[6])
                month = time.strftime("%Y-%m",time.gmtime(time_epoch))
                mon_value = float(fields[3])
                yield(month,(mon_value,1))
        except:
            pass

    def reducer(self, month, values):
        count = 0
        total = 0
        for v in values:
            count = count+ v[1]
            total = total + v[0]
        avg_trans=(total/count)
        yield(month,avg_trans)
if __name__ == '__main__':
    PartA.run()
