#!/usr/bin/env python3
'''
Name: Kartikey Sharma
Date: 04/14/2022
PMG Digital Agency
Coding Challenge for Graduate Leadership Program
Scalable approach
'''
import os
import sys
from pathlib import Path
import dask.dataframe as dd

#created a PMG_Challenge class with the solution function 
class PMG_Challenge:
	#input: argument list of all csv files.
	#output: a combined csv file with all the data.
	def combine_csv(self, arg):
		files = list(dict.fromkeys(arg[:-1]))

		#input: None.
		#output: Boolean.
		def validate_csv():
			#check if at least 2 files are provided.
			if len(files) < 2:
				print('Not enough csv files provided')

			for i in files:
				#check if file exists. Return False if not.
				if not os.path.exists(i):
					raise IOError (os.path.basename(i) + ' was not found on the system.')
					return False
				#check if file is a .csv type. Return False if not.
				if Path(i).suffix != '.csv':
					print(os.path.basename(i) + ' is not a csv file.')
					return False

			return True

		#input: a csv file.
		#output: a dask dataframe with csv data.
		def create_dataframe(file):
			#initiate a dask dataframe with csv data
			data_frame = dd.read_csv(file)
			#added another column to denote the filename
			data_frame['filename'] = os.path.basename(file)
			data_frame['filename'] = data_frame['filename'].astype(str)

			return data_frame

		#if vavlidate_csv returns false, exit.
		if not validate_csv():
			return 
		#A list of dataframe holding csv data
		file_data = [create_dataframe(i) for i in files]

		#combine all csv file into one file that is the last string of argument value (argv[-1])
		combined_csv = dd.concat(file_data, axis = 0)
		combined_csv.to_csv(arg[-1], single_file = True, index = False)

#main function
if __name__ == '__main__':
	solution = PMG_Challenge()
	solution.combine_csv(sys.argv[1:])


