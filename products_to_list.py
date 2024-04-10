import pandas as pd
import numpy as np
import os


def filestolist(file):
    files_to_download = []

    ## for wave stuff
    df1 = pd.read_csv(file, sep='/', skiprows=1, header=None)
    rows, cols = df1.shape

    for i in range(rows):
        filemeta = df1.iloc[i][0]
        filesize = int(filemeta.split(" ")[-4])
        if filesize > 0:  # 120 MB

            fullpath = r'/mnt/hdfs/'
            for col in range(1, cols - 1):
                fullpath += df1.iloc[i][col] + '/'

            files_to_download.append({"link": fullpath, "fileName":  df1.iloc[i][cols-1]})

    return files_to_download


def ocn_parse_to_filelist(file):
    files_to_download = []
    with open(file) as f:
        line = f.readlines()

    lines = line[0].split('name=')
    for i in range(len(lines)):
        # skip first one
        # line_i = lines[i]
        if i == 0:
            continue
        else:
            product_name = lines[i].split('>')[0]
            product_name = product_name[1:-1]
            timestr = product_name.split('_')[5]
            yyyy = timestr[0:4]
            mm = timestr[4:6]
            dd = timestr[6:8]
            fullpath = "/mnt/hdfs/calvalus/eodata/S1_OCN_L2/v1/" + yyyy + '/' + mm + '/' + dd
            files_to_download.append({"link": fullpath, "fileName": product_name})
    return files_to_download


if __name__ == '__main__':

    file = r'filelists/esa_ew_files.txt'
    files_to_download = filestolist(file)


