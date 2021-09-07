from mrjob.job import MRJob

class repartition_stock_join(MRJob):

    def mapper(self, _, line):
        try:
            if(len(line.split('\t'))==2):
                fields=line.split('\t')
                join_key=fields[0]
                join_value=float(fields[1])
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
            elif value[1]==2:
               contract.append(value[0])
        if  addresses > 0 and len(contract) != 0:
            yield (address, addresses)
            contract =[]



if __name__=='__main__':
    repartition_stock_join.JOBCONF={'mapreduce.job.reduces':'10'}
    repartition_stock_join.run()
