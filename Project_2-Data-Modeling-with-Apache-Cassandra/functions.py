# import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


def preprocessing_csv_file(pwd):
    """
    Iterate through each event file in event_data to process 
    and create a new csv file.
    """
    
    ## 1. Create list of filepaths to process original event csv data files.
    # Get current folder and subfolder event data
    filepath = pwd + '/event_data'
    
    # Create a for-loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        
        # Join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
        
    ## 2. Process the files to create the new csv file that will be used for Apache Casssandra tables.    
    # Initiating an empty list of rows that will be generated from each file
    full_data_rows_list = [] 
    
    # For every filepath in the file path list ...
    for f in file_path_list:
        
        # ... read csv file 
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            csvreader = csv.reader(csvfile) 
            next(csvreader)
        
             # extract each data row one by one and append it        
            for line in csvreader:
                full_data_rows_list.append(line) 

    # Create a smaller event data csv file called event_datafile_full csv 
    # that will be used to insert data into the Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',
                         'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))