from mrjob.job import MRJob
import math
import time
import re

class PartD_gas(MRJob):

    def mapper(self, _, line):
        try:
            if(len(line.split(','))==7):
                fields=line.split(',')
                join_key=fields[2]
                join_value=float(fields[4])
                yield (join_key,(join_value,1))

            elif(len(line.split(','))==5):
                fields=line.split(',')
                join_key=fields[0]
                yield (join_key,(join_key,2))
        except:
            pass



    def reducer(self, address, values):
        contract =[]
        addresses = 0

        for value in values:
            if value[1]==1:
                addresses=value[0]
                gas=value[1]
            elif value[1]==2:
               contract.append(value[0])
        if  addresses > 0 and len(contract) != 0:
            yield(addresses,contract) 
            contract =[]



if __name__ == '__main__':
    PartD_gas.JOBCONF={'mapreduce.job.reduces':'10'}
    PartD_gas.run()
