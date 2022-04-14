# PMG-Challenge

This repository contains my approach to solve the PMG Graduate Leadership Program Challenge. The python program "csv_combiner[1-2].py" combines multiple CSV filess into a single CSV file.

Setup:

pip3 install dask
pip3 install pandas

Scalable Apprach (Using Dask):
  
  Run using command:
  python3 csv_combiner1.py inputFile1.csv inputFile2.csv inputFile3.csv outputFile.csv

Faster Approach (Using pandas):

  Run using command:
  python3 csv_combiner2.py inputFile1.csv inputFile2.csv inputFile3.csv > outputFile.csv

