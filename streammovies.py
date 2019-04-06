#!/usr/bin/python
# -*- coding: utf-8 -*-

# For the upload to the bucket to work there must be a keyfile and
# the environment variable GOOGLE_APPLICATION_CREDENTIALS must point to this

import os
import json
import datetime
import time
from google.cloud import storage


source_bucket="big-data-course-datasets"
source_folder="moviereviews"
target_bucket="streaming-examples"
files_location="./data"

if not os._exists(files_location):
    os.mkdir(files_location)
    bucket_name=source_bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    for filename in ["users.dat", "movies.dat", "ratings.dat"]:
        blobs = bucket.list_blobs(prefix=source_folder+"/"+filename)
        for blob in blobs:
            blob.download_to_filename(files_location+"/"+filename)

users={}
with open(os.path.join(files_location, "users.dat"), "r") as f:
    for line in f.readlines():
        linesplit=line.split("::")
        users[linesplit[0]]=linesplit[1].strip()

movietitles={}
moviegenre={}
with open(os.path.join(files_location, "movies.dat"), "r") as f:
    for line in f.readlines():
        linesplit=line.split("::")
        movietitles[linesplit[0]]=linesplit[1]
        if len(linesplit[2].strip())>0:
            moviegenre[linesplit[0]]=linesplit[2].strip().split("|")
        else:
            moviegenre[linesplit[0]]=[]

ratings=[]
with open(os.path.join(files_location, "ratings.dat"), "r") as f:
    for line in f.readlines():
        linesplit=line.split("::")
        ratings.append({
            "userid": linesplit[0],
            "movieid": linesplit[1],
            "rating": int(linesplit[2]),
            "time": int(linesplit[3])
        })

ratings.sort(key=lambda x: x["time"])

def pretty_line(x):
    return json.dumps({
        "user": users[x["userid"]],
        "movie": movietitles[x["movieid"]],
        "rating": x["rating"],
        "timestamp": x["time"],
        "time": datetime.datetime.utcfromtimestamp(x["time"]).strftime('%Y-%m-%d %H:%M:%S')
    })


bucket_name=target_bucket
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)
blobs = bucket.list_blobs()
for blob in blobs:
    blob.delete()

i=0
items_per_file=20
while i<len(ratings)-items_per_file:
    filename="tweets_"+str(i)
    nextfile=""
    for j in range(items_per_file):
        nextfile+=pretty_line(ratings[i])+"\n"
        i+=1
    with open(filename, "w") as f:
        f.write(nextfile)
    blob = bucket.blob(filename)
    blob.upload_from_filename(filename)
    os.remove(filename)
    print(filename)
    time.sleep(2)




