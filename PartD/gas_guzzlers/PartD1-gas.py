from mrjob.job import MRJob
import math
import time
import re

class PartD_gas(MRJob):

    def mapper(self, _, line):
        try:
            if(len(line.split(','))==7):
                fields=line.split(',')
                gas_price=float(fields[5])
                time_epoch=int(fields[6])
                month = time.strftime("%Y-%m",time.gmtime(time_epoch))
                yield (month,(gas_price,1))
        except:
            pass



    def reducer(self, month, values):
        count = 0
        total = 0
        for v in values:
            count = count+ v[1]
            total = total + v[0]
        avg_gas=(total/count)
        yield(month,avg_gas)




if __name__ == '__main__':
    PartD_gas.run()
