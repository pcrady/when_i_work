import pandas as pd
import numpy as np
import urllib
import string
import multiprocessing
import os
import argparse
import sys

# Base url and debugging stuff for pandas
pd.set_option('max_colwidth', 2000)
pd.set_option('display.width', 20000)
BASE_URL = 'https://s3-us-west-2.amazonaws.com/cauldron-workshop/data/'

# Generates the file names a.csv, b.csv ....
def generate_file_names():
    return  [letter + '.csv' for letter in list(string.ascii_lowercase)]

# Downloads a single file given by the url
def download_file(url):
    downloaded_file = urllib.URLopener()
    downloaded_file.retrieve(url, url.split('/')[-1])

# Downloads multiple files in parallel. Must use multiprocessing here
# because threading isnt really threading in python.
# Note: would be more performant if wasnt writing the files to disk.
def download_files(base_url):
    urls = [base_url + name for name in generate_file_names()]
    pool = multiprocessing.Pool(processes=26)
    pool.map(download_file, urls)

# Concatinates all the dataframes from the downloaded csvs 
def concatinate_dataframes():
    file_names = generate_file_names()
    df_list = [pd.read_csv(file_name) for file_name in file_names]
    return pd.concat(df_list)

# This is like several hundred times faster. I knew there was a function
# for this I just didnt know what it was called.
def transform_dataframe(df):
    df = df[['user_id', 'path','length']]
    return df.pivot(index="user_id", columns="path", values="length").fillna(0)

def delete_files():
    for f in generate_file_names():
        os.remove(f)

def main_wrapper():
    df = concatinate_dataframes()
    df = transform_dataframe(df)
    df.to_csv("output.csv")
    delete_files()

# Main function with a really crappy commandline argument parser.
def main():
    if len(sys.argv) == 2:
        download_files(sys.argv[1])
    elif len(sys.argv) == 1:
        download_files(BASE_URL)
    else:
        return
 
    main_wrapper()



if __name__ == "__main__":
    main()
