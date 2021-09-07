from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import time

class Scam(MRJob):


	scam_dict={}

	def mapper_1(self):

		with open('scams.json') as json_file:
			data = json.load(json_file)
			input = data['result']
			for line in input:
				scams_fields = input[line]
				addresses = scams_fields['addresses']
				id = scams_fields['id']
				name = scams_fields['name']
				category = scams_fields['category']
				try:
					subcategory = scams_fields['subcategory']
				except:
					subcategory=''
					continue

				status = scams_fields['status']
				json_list = [id, name, category, subcategory, status]
				for address in addresses:
					json_list.append(address)
					self.scam_dict[address] = json_list

	def mapper_2(self, _, line):

		try:
			fields=line.split(',')
			if len(fields)==7:
				time_epoch = int(fields[6])
				month = time.strftime("%Y-%m", time.gmtime(time_epoch))
				address = fields[2]
				mon_value = int(fields[3])
				if address in self.scam_dict:
					scam_info = self.scam_dict[address]
					yield((scam_info, month), mon_value)
		except:
			pass


	def reducer_total(self, key, mon_value):
		total=sum(mon_value)
		yield(key, total)


	def mapper_swap(self, key, total):
		yield(key[1], (total, key[0]))


	def reducer(self, key, total):
		sorted_values =sorted(total, reverse=True, key=lambda x:x[0])
		yield(key, sorted_values[0])

	def steps(self):

		return([MRStep(mapper_init=self.mapper_1, mapper=self.mapper_2,  reducer=self.reducer_total),
			MRStep(mapper=self.mapper_swap,  reducer=self.reducer)])


if __name__ == '__main__':
	Scam.JOBCONF={'mapreduce.job.reduces':'10'}
	Scam.run()
