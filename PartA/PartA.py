from mrjob.job import MRJob
import math
import time
import re


class PartA(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7 :
                time_epoch=float(fields[6])
                month =time.strftime("%Y-%m",time.gmtime(time_epoch))
                yield (month,1)
        except:
            pass



    def reducer(self, month, counts):
        yield (month,sum(counts))

if __name__ == '__main__':
    PartA.run()
