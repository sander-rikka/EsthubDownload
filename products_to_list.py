import pandas as pd
import numpy as np
import os


def filestolist(file):
    files_to_download = []

    ## for wave stuff
    df1 = pd.read_csv(file, sep='/', skiprows=1, header=None)
    for i in range(df1.shape[0]):
        filemeta = df1.iloc[i][0]
        filesize = np.int(filemeta.split(" ")[-4])
        # if filesize > 120000000:  # 120 MB
        fullpath = "/mnt/hdfs/calvalus/home/sander.rikka/s1_wave2/" + \
            df1.iloc[i][5] + "/" + \
            df1.iloc[i][6] + "/" + \
            df1.iloc[i][7]

        files_to_download.append({"link": fullpath, "fileName":  df1.iloc[i][8]})

    # ## for heltermaa stuff
    # df1 = pd. read_csv(file, header=None)
    # for i in range(df1.shape[0]):
    #     # filemeta = df1.iloc[i][0]
    #     # filesize = np.int(filemeta.split(" ")[-4])
    #     # if filesize > 120000000:  # 120 MB
    #     filepath = df1.iloc[i][0]
    #     path, filename = os.path.split(filepath)
    #
    #     fullpath = "/mnt/hdfs" + path
    #
    #     files_to_download.append({"link": fullpath, "fileName":  filename})
    #
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

    file = 'nbp_files.txt'
    files_to_download = filestolist(file)


