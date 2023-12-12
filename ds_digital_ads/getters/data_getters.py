"""
Data getters (and savers)
"""
from fnmatch import fnmatch
import json
import pickle
import gzip
import os

import pandas as pd
from pandas import DataFrame
import boto3
from decimal import Decimal
import numpy
import yaml
import io
from io import BytesIO

from ds_digital_ads import logger, PROJECT_DIR, BUCKET_NAME
from typing import List
import requests


def save_images_to_s3(
    image_urls: List[str],
    output_folder: str,
):
    """Save a list of image urls to S3.

    Args:
        image_urls (List[str]): List of image urls.
    """
    for image_url in image_urls:
        request = requests.get(image_url)
        if request.status_code == 200:
            file_name = image_url.split("/")[-1]
            images_file_path = os.path.join(output_folder, "images", file_name)
            save_to_s3(BUCKET_NAME, request.content, images_file_path)
        else:
            print(f"Image {image_url} could not be downloaded.")


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        elif isinstance(obj, set):
            return list(obj)
        return super(CustomJsonEncoder, self).default(obj)


def get_s3_resource():
    s3 = boto3.resource("s3")
    return s3


def save_to_s3(bucket_name, output_var, output_file_dir):
    s3 = get_s3_resource()

    obj = s3.Object(bucket_name, output_file_dir)

    if fnmatch(output_file_dir, "*.csv"):
        output_var.to_csv("s3://" + bucket_name + "/" + output_file_dir, index=False)
    elif fnmatch(output_file_dir, "*.parquet"):
        output_var.to_parquet(
            "s3://" + bucket_name + "/" + output_file_dir, index=False
        )
    elif fnmatch(output_file_dir, "*.pkl") or fnmatch(output_file_dir, "*.pickle"):
        obj.put(Body=pickle.dumps(output_var))
    elif fnmatch(output_file_dir, "*.gz"):
        obj.put(Body=gzip.compress(json.dumps(output_var).encode()))
    elif fnmatch(output_file_dir, "*.txt"):
        obj.put(Body=output_var)
    elif (
        fnmatch(output_file_dir, "*.jpg")
        or fnmatch(output_file_dir, "*.png")
        or fnmatch(output_file_dir, "*.jpeg")
    ):
        image_data = BytesIO(output_var)
        obj.put(Body=image_data)
    else:
        obj.put(Body=json.dumps(output_var, cls=CustomJsonEncoder))

    logger.info(f"Saved to s3://{bucket_name} + {output_file_dir} ...")


def load_s3_data(bucket_name, file_name):
    """
    Load data from S3 location.

    bucket_name: The S3 bucket name
    file_name: S3 key to load
    """
    s3 = get_s3_resource()

    obj = s3.Object(bucket_name, file_name)
    if fnmatch(file_name, "*.jsonl.gz"):
        with gzip.GzipFile(fileobj=obj.get()["Body"]) as file:
            return [json.loads(line) for line in file]
    if fnmatch(file_name, "*.yml") or fnmatch(file_name, "*.yaml"):
        file = obj.get()["Body"].read().decode()
        return yaml.safe_load(file)
    elif fnmatch(file_name, "*.jsonl"):
        file = obj.get()["Body"].read().decode()
        return [json.loads(line) for line in file]
    elif fnmatch(file_name, "*.json.gz"):
        with gzip.GzipFile(fileobj=obj.get()["Body"]) as file:
            return json.load(file)
    elif fnmatch(file_name, "*.json"):
        file = obj.get()["Body"].read().decode()
        return json.loads(file)
    elif fnmatch(file_name, "*.csv"):
        return pd.read_csv("s3://" + bucket_name + "/" + file_name)
    elif fnmatch(file_name, "*.parquet"):
        return pd.read_parquet("s3://" + bucket_name + "/" + file_name)
    elif fnmatch(file_name, "*.pkl") or fnmatch(file_name, "*.pickle"):
        file = obj.get()["Body"].read().decode()
        return pickle.loads(file)
    elif (
        fnmatch(file_name, "*.jpg")
        or fnmatch(file_name, "*.png")
        or fnmatch(file_name, "*.jpeg")
    ):
        # Download the image from S3 into a BytesIO object
        image_data = BytesIO()
        obj.download_fileobj(image_data)
        return image_data

    else:
        logger.error(
            'Function not supported for file type other than "*.csv", "*.parquet", "*.jsonl.gz", "*.jsonl", or "*.json"'
        )


def dictionary_to_s3(data_dict: dict, s3_bucket: str, s3_folder: str, file_name: str):
    """
    Transforms a dictionary into a json and uploads to S3.
    Args:
        data_dict: dictionary with the data
        s3_bucket: S3 bucket name where to upload the file
        s3_folder: folder where to store the file within the S3 bucket
        file_name: name of the file
    """
    s3_client = boto3.client("s3")
    obj = io.BytesIO(json.dumps(data_dict).encode("utf-8"))
    s3_client.upload_fileobj(obj, s3_bucket, os.path.join(s3_folder, file_name))


def save_json_to_local_inputs_folder(data_dict: dict, folder: str, file_name: str):
    """
    Saves json file to local inputs folder.

    Args:
        data_dict: dictionary with the data
        folder: path to local folder, within the inputs/ folder, where to store data
        file_name: name of the file
    """

    path_local_inputs = os.path.join(PROJECT_DIR, "inputs/")
    full_path = os.path.join(path_local_inputs, folder)

    # Creates local folder path if it does not exist
    os.makedirs(full_path, exist_ok=True)

    obj = json.dumps(data_dict)
    # Writing to sample.json
    with open(os.path.join(full_path, file_name), "w") as outfile:
        outfile.write(obj)


def read_json_from_s3(bucket: str, file_path: str) -> dict:
    """
    Reads a json file from S3 without downloading it.

    Args:
        bucket: S3 bucket name
        file_path: file path (including file name)
    Returns:
        dictionary with json file data
    """
    s3_resource = boto3.resource("s3")
    json_file = s3_resource.Object(bucket, file_path)
    json_file = json_file.get()["Body"].read().decode("utf-8")
    return json.loads(json_file)


def read_json_from_local_path(file_path: str) -> dict:
    """
    Reads a json file from a local folder.

    Args:
        file_path: file path (including file name)
    Returns:
        dictionary with json file data
    """
    with open(file_path, "r") as f:
        data = json.load(f)

    return data


def get_s3_data_paths(bucket_name, root, file_types=["*.jsonl"]):
    """
    Get all paths to particular file types in a S3 root location

    bucket_name: The S3 bucket name
    root: The root folder to look for files in
    file_types: List of file types to look for, or one
    """
    s3 = get_s3_resource()
    if isinstance(file_types, str):
        file_types = [file_types]

    bucket = s3.Bucket(bucket_name)

    s3_keys = []
    for files in bucket.objects.filter(Prefix=root):
        key = files.key
        if any([fnmatch(key, pattern) for pattern in file_types]):
            s3_keys.append(key)

    return s3_keys
