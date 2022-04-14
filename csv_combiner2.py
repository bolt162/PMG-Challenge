#!/usr/bin/env python3
'''
Name: Kartikey Sharma
Date: 04/14/2022
PMG Digital Agency
Coding Challenge for Graduate Leadership Program
Faster Approach
'''
import os
import sys
from pathlib import Path
import pandas as pd

chunk_size = 100000

#created a PMG_Challenge class with the solution function 
class PMG_Challenge:
	#input: argument list of all csv files.
	#output: a combined csv file with all the data.
	def combine_csv(self, arg):
		files = list(dict.fromkeys(arg))
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

				file_name = os.path.basename(i)
				#iterating over and reading every 100000 rows of the file
				for chunk in pd.read_csv(i, chunksize=chunk_size):

					chunk['filename'] = file_name
					#prints every chunk to a new csv file
					print (chunk.to_csv(index = False, line_terminator='\n', header = True if i == 0 else False), end = '')

			return True

		#if validate_csv returns false, exit.
		if not validate_csv():
			return 

#main function
if __name__ == '__main__':
	solution = PMG_Challenge()
	solution.combine_csv(sys.argv[1:])

