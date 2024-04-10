# -*- coding: utf-8 -*-
import subprocess
import json
import logging
import os
from collections import OrderedDict
from datetime import datetime, timedelta
from os.path import exists, join

import paramiko
from sentinelsat.sentinel import SentinelAPI
from shapely.geometry import Polygon  # https://towardsdatascience.com/install-shapely-on-windows-72b6581bb46c
from products_to_list import filestolist, ocn_parse_to_filelist


class SentinelDownload:

    def __init__(self, download_directory, archive_directory):
        logging.basicConfig(format='%(message)s', level='INFO')  # Enables sentinelsat logging on info level
        self.logger = logging.getLogger('SentinelDownload')
        self.config_files = os.path.dirname(os.path.realpath(__file__)) + "/download_configs"
        self.api = None
        self.data = {}
        self.download_directory = download_directory
        self.archive_directory = archive_directory

    @staticmethod
    def get_polygons(coordinates: str):
        list_of_tuple_coordinates = []
        coordinates_split_by_left_parenthesis = coordinates.split("(")
        coordinates_without_overhead_left = \
            coordinates_split_by_left_parenthesis[len(coordinates_split_by_left_parenthesis) - 1]
        coordinates_without_overhead = coordinates_without_overhead_left.split(")")[0]

        for coordinate_pair in coordinates_without_overhead.split(","):
            split_coordinates = coordinate_pair.split()
            coordinate_tuple = (float(split_coordinates[0]), float(split_coordinates[1]))
            list_of_tuple_coordinates.append(coordinate_tuple)

        return Polygon(list_of_tuple_coordinates)

    def remove_files_with_not_enough_area(self, products):
        if self.data.get("minimal_overlap_percentage") is None:
            return products

        minimal_overlap = float(self.data.get("minimal_overlap_percentage")) / 100.0

        filtered_products = OrderedDict()
        for product in products.values():
            search_polygon = self.get_polygons(self.data.get("location_data"))
            query_polygon = self.get_polygons(product.get("footprint"))
            intersection = search_polygon.intersection(query_polygon)
            overlap_percentage = (intersection.area / search_polygon.area)

            if overlap_percentage >= minimal_overlap:
                filtered_products[product.get("uuid")] = product
            else:
                self.logger.info("Removed product %s, which didn't cover enough area "
                                 "(covered only %s percent of given area)!",
                                 product.get("title"), overlap_percentage)

        if len(products.values()) != len(filtered_products.values()):
            self.logger.info("Removed %s products, as they did not cover enough important area!",
                             str(len(products.values()) - len(filtered_products.values())))

        return filtered_products

    @staticmethod
    def remove_snake_case_from_keywords(keywords):
        validated_keywords = {}
        for keyword, value in keywords.items():
            validated_keywords[str(keyword).replace("_", "")] = value

        return validated_keywords

    def remove_already_downloaded_products(self, products):
        filtered_products = OrderedDict()
        for product in products.values():
            path_to_downloads = join(self.download_directory, product.get("title") + ".zip")
            path_to_archive = join(self.archive_directory, product.get("title") + ".zip")
            if not exists(path_to_downloads) and not exists(path_to_archive):
                filtered_products[product.get("uuid")] = product
            else:
                self.logger.info("Removed product %s, as it already exists in download or archive directory!",
                                 product.get("title"))

        return filtered_products

    def query_and_filter_results(self):
        if self.data.get("date") is None:
            search_time = ((datetime.now() - timedelta(hours=self.data.get("search_from_hours_ago"))), datetime.now())
        else:
            search_time = (self.data.get("date")[0], self.data.get("date")[1])

        keywords = self.remove_snake_case_from_keywords(self.data.get("optional_parameters"))
        products = self.api.query(area=self.data.get("location_data"),
                                  date=search_time,
                                  platformname=self.data.get("platform"),
                                  **keywords)

        products_filtered_by_area = self.remove_files_with_not_enough_area(products)
        products_filtered_by_existence = self.remove_already_downloaded_products(products_filtered_by_area)

        return products_filtered_by_existence

    def scpFileFromServer(self, server, username, password, localpath, remotepath):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.load_host_keys(os.path.expanduser(os.path.join("~", ".ssh", "known_hosts")))
        k = paramiko.RSAKey.from_private_key_file('C:/Users/sander.rikka/rsa_key_test01')
        ssh.connect(server, username=username, pkey=k)
        sftp = ssh.open_sftp()
        sftp.get(remotepath, localpath)
        sftp.close()
        ssh.close()

    def scpFilesFromServer(self, server, username, password, download_directory, downloads, remote_username, remote_password):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.load_host_keys(os.path.expanduser(os.path.join("~", ".ssh", "known_hosts")))
        k = paramiko.RSAKey.from_private_key_file('C:/Users/sander.rikka/rsa_key_test01')
        ssh.connect(server, username=username, pkey=k)
        sftp = ssh.open_sftp()

        for download in downloads:
            remote_path = download.get("link") + "/" + download.get("fileName")
            local_path = download_directory + "/" + download.get("fileName")

            try:
                sftp.get(remote_path, local_path)
                print("Downloading finished for {}".format(download.get("fileName")))
            except FileNotFoundError:
                print("\n File not found {}".format(download.get("fileName")))
                pass

        sftp.close()
        ssh.close()

    def download_all_files(self, download_directory, downloads, server, remote_username, remote_password):

        self.scpFilesFromServer(server, remote_username, remote_password, download_directory, downloads, remote_username, remote_password)

    def convertProductsToDirectDownloadLinks(self, products_to_download):
        links = []
        for product_key, product_value in products_to_download.items():
            path = ("/mnt/hdfs/calvalus/eodata/S1_GRD_L1/v1/" + str(product_value.get('beginposition').year) + "/" +
                    str(product_value.get('beginposition').month).zfill(2) + "/" +
                    str(product_value.get('beginposition').day).zfill(2))
            if not any(x.get("link") == path for x in links):
                links.append({ "link": path, "fileName":  product_value["title"] + ".zip"})
        return links

    def run_all_downloads(self, server, remote_user, remote_password):
        for file in os.listdir(self.config_files):
            if '.json' in file:
                with open(self.config_files + "/" + file) as f:
                    self.data = json.load(f)
                    self.api = SentinelAPI(self.data.get("username"), self.data.get("password"), self.data.get("url"))
                    products_to_download = self.query_and_filter_results()
                    files_to_download = self.convertProductsToDirectDownloadLinks(products_to_download)
                    print(files_to_download)
                    self.download_all_files(self.download_directory, files_to_download, server, remote_user, remote_password)

    def run_seleceted_downloads(self, server, remote_user, remote_password, files_to_download):
        self.download_all_files(self.download_directory, files_to_download, server, remote_user, remote_password)


def access_network_drive(username, password, drive_letter, network_path):
    # Unmount the drive if it's already mounted
    subprocess.run(['net', 'use', f'{drive_letter}:', '/delete'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Mount the network drive with credentials
    subprocess.run(['net', 'use', f'{drive_letter}:', f'{network_path}', f'/user:{username}', f'{password}'],
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Check if the drive is mounted successfully
    drive_list = subprocess.run(['net', 'use'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True).stdout
    if f'{drive_letter}:' in drive_list:
        print(f"Successfully connected to {drive_letter}: drive.")
    else:
        print("Failed to connect to the network drive.")


if __name__ == '__main__':

    username = input("Enter your username for network drive X: ")
    password = input("Enter your password for network drive X: ")

    drive_letter = "X"  # Choose any available drive letter
    network_path = r"\\172.17.91.26\sander"  # Replace with your network path

    access_network_drive(username, password, drive_letter, network_path)

    # download files to X mount
    filepath = rf'./filelists'
    for file in os.listdir(filepath):
        localpath = rf'./filelists/{file}'

        download_directory = r'X:\2023-ESA\data\EW'
        archive_directory = r'X:\2023-ESA\archive'
        if not os.path.exists(download_directory):
            print('Creating download folder:', download_directory)
            os.makedirs(download_directory, exist_ok=True)

        print("Started downloading script")
        downloader = SentinelDownload(download_directory, archive_directory)

        downloader.run_seleceted_downloads("ehproduction02.kemit.ee", "sander.rikka", 'password',
                                           filestolist(localpath))
        print("Finished downloading script")
