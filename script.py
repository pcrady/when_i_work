#!/usr/bin/python3
"""Reformating a set of downloaded csvs."""

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
OUTPUT_FILE = 'output.csv'


def generate_file_names():
    """Return a list of filenames."""
    return [letter + '.csv' for letter in list(string.ascii_lowercase)]


def download_file(url):
    """Return a downloaded file as a pandas dataframe."""
    return pd.read_csv(url)


def download_files(base_url=BASE_URL):
    """Return a list of pandas dataframes.

    Keyword arguments:
    base_url -- the base url for downloading a list of files
    """
    urls = [base_url + name for name in generate_file_names()]
    pool = multiprocessing.Pool(processes=len(urls))
    return pool.map(download_file, urls)


def concatinate_dataframes(df_list):
    """Concatinate a list of dataframes into one dataframe."""
    return pd.concat(df_list)


def transform_dataframe(df):
    """Return a transformed pandas dataframe."""
    df = df[['user_id', 'path', 'length']]
    return df.pivot(index="user_id", columns="path", values="length").fillna(0)


def write_csv(df_list, output_file="output.csv"):
    """Write a formated csv to disk.

    Keyword arguments:
    output_file -- the name of the csv file to be written to disk
    """
    df = concatinate_dataframes(df_list)
    df = transform_dataframe(df)
    df.to_csv(output_file)


def get_args():
    """Return argparse arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", dest="output_file",
                        default=OUTPUT_FILE, help="Name of the output file")
    parser.add_argument("-u", "--url", dest="base_url",
                        default=BASE_URL,
                        help="Base url for downloading files")
    return parser.parse_args()


# Main function with a really crappy commandline argument parser.
def main():
    """The main function."""
    args = get_args()
    files = download_files(args.base_url)
    write_csv(files, args.output_file)


if __name__ == "__main__":
    main()
