import pandas as pd
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

# Gets a list of unique elemnts from a column
def get_unique_elements(df, column_name):
    return df[column_name].unique()

# Gets all rows from a given user id.
def get_rows_by_user_id(df, user_id):
    return df.loc[df['user_id'] == user_id]

# Creates rows on a per user id basis. This is not very efficent
# and by far the slowest part of the whole thing. I'm not very 
# good at database manipulation so the performance really suffers here.
def create_row(user_df, path_list):
    user_id = user_df['user_id'].iloc[0]
    df = user_df[['path', 'length']].reset_index(drop=True)
    df = df.transpose().reset_index(drop=True)
    df = df.rename(index=str, columns=df.iloc[0])[1:].reset_index(drop=True)
    df['user_id'] = user_id
    for path in path_list:
        if path not in df:
            df[path] = 0
    return df.reindex_axis(sorted(df.columns), axis=1)

# Main function with a really crappy commandline argument parser.
def main():
    if len(sys.argv) == 2:
        download_files(sys.argv[1])
    elif len(sys.argv) == 1:
        download_files(BASE_URL)
    else:
        print "Invalid number of arguments"
        return

    df = concatinate_dataframes()
    path_list = get_unique_elements(df, 'path')
    user_id_list = get_unique_elements(df, 'user_id')
    user_dfs = [create_row(get_rows_by_user_id(df, i), path_list) for i in range(1, user_id_list.size)]
    df = pd.concat(user_dfs).set_index('user_id')
    df.to_csv("output.csv")
    for f in generate_file_names():
        os.remove(f)


if __name__ == "__main__":
    main()
