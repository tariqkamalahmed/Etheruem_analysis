from mrjob.job import MRJob

class PartC(MRJob):

    def mapper(self, _, line):
        try:
            if(len(line.split(','))==9):
                fields=line.split(',')
                miner =fields[2]
                size=float(fields[4])
                yield (miner,size)
        except:
            pass



    def reducer(self, miner, size):
        total=sum(size)
        yield(miner,total)




if __name__ == '__main__':
    PartC.JOBCONF={'mapreduce.job.reduces':'3'}
    PartC.run()
