
Code for downloading files from ESTHub server. Using paramiko package. 

generate files list in your home catalogue using 
```
hdfs dfs -ls /calvalus/home/<user.name>/path/to/files > files_to_download.txt

```
Download the files_to_download.txt into the project and update download_main.py __main__ function. 