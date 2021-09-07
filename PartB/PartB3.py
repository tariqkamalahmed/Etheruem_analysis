from mrjob.job import MRJob

class PartB3(MRJob):

    def mapper(self, _, line):
        try:
            if(len(line.split('\t'))==2):
                fields=line.split('\t')
                address=fields[0]
                addresses=float(fields[1])
                yield (1,(address,addresses))
        except:
            pass





    def reducer(self, address, addresses):
        sorted_values = sorted(addresses, reverse=True, key=lambda x: x[0])[:10]
        key = 0
        for i in sorted_values:
            key+=1
            yield(key,('{}-{}'.format((i[0]),float(i[1]))))



if __name__ == '__main__':
    PartB3.JOBCONF={'mapreduce.job.reduces':'3'}
    PartB3.run()
