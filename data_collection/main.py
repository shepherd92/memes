#!/usr/bin/env python3


import pandas as pd
import datetime as dt
import os
import glob
from queue import Queue

# ONLINE PROCESSING
# os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
# import tensorflow as tf
# tf.get_logger().setLevel('ERROR')

from data_collection_config import DataCollectionConfig
from data_collection_worker import DataCollectionWorker

import cProfile, pstats, io


def main():

    # EDIT BELOW THIS LINE

    profile      = False
    numOfThreads = 8
    downloadData = True
    mergeData    = False

    outputDirectory = 'output'
    continueDownload = True  # do not download already downloaded data again

    # download interval
    startDate = '2021/08/16'  # format: YYYY/MM/DD
    endDate   = '2021/08/17'  # format: YYYY/MM/DD

    startTime = '00:00:00'  # start time to download for each day
    endTime   = '23:59:59'  # end   time to download for each day

    numOfPostsPerDay = 50  # maximum number of posts to download for each day
    frequency = 1            # number of days to skip between two days to collect (in days)

    subReddits = ['memes',
                  'dankmemes',
                  'memeeconomy',
                  'adviceanimals',
                  'aww',
                  'comedycemetery',
                  'comedyheaven',
                  'funny',
                  'historymemes',
                  'lastimages',
                  'okbuddyretard',
                  'pewdiepiesubmissions',
                  'prequelmemes',
                  'raimimemes',
                  'teenagers',
                  'terriblefacebookmemes',
                  'wholesomememes'
                  ]

    # EDIT ABOVE THIS LINE

    dataCollectionConfig = DataCollectionConfig()
    dataCollectionConfig.setDateInterval(startDate, endDate)
    dataCollectionConfig.setTimeInDayInterval(startTime, endTime)
    dataCollectionConfig.numOfPostsPerDay = numOfPostsPerDay
    dataCollectionConfig.frequency = frequency
    dataCollectionConfig.continueDownload = continueDownload
    dataCollectionConfig.subReddits = subReddits
    dataCollectionConfig.outputDirectory = outputDirectory

    if profile:
        assert(1 == numOfThreads)
        pr = cProfile.Profile()
        pr.enable()

    if downloadData:
        downloadAndProcessPosts(numOfThreads, dataCollectionConfig)
    
    if mergeData:
        mergedData = loadData('output')
        mergedData.to_csv('merged_data.csv', float_format='%.4f')

    if profile:
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
        ps.print_stats(30)
        print(s.getvalue())


def downloadAndProcessPosts(numOfThreads: int, dataCollectionConfig):

    epochs = dataCollectionConfig.getEpochs()
    DataCollectionWorker._numOfEpochs = len(epochs)

    if dataCollectionConfig.continueDownload:
        epochs = dataCollectionConfig.removeDownloadedEpochs(epochs, dataCollectionConfig.outputDirectory)
        DataCollectionWorker._processedEpochsInTotal = DataCollectionWorker._numOfEpochs - len(epochs)

    if epochs.empty:
        print(f"Nothing to download.", flush=True)
        return

    print(f"{dt.datetime.now().strftime('%m/%d %H:%M:%S')}: Scraper is processing " +
          f"{DataCollectionWorker._numOfEpochs} epochs " +
          f"with {numOfThreads} threads.",
          flush=True)

    # initialization
    DataCollectionWorker.initialize(dataCollectionConfig.subReddits,
                                    dataCollectionConfig.outputDirectory)

    if numOfThreads != 1:
        startThreads(numOfThreads, epochs)
    else:
        startSingleThread(epochs)

    print(f"\n{dt.datetime.now().strftime('%m/%d %H:%M:%S')}: Scraper finished.", flush=True)


def startThreads(numOfThreads: int, epochs: pd.DataFrame):

    tokenFileName = 'config/token.txt'
    tokens = pd.read_csv(tokenFileName)
    assert(numOfThreads <= len(tokens))  # number of threads cannot be more than the number of reddit API keys
    queue = Queue()  # queue communicates with workers

    # create and start threads
    for threadId in range(numOfThreads):
        worker = DataCollectionWorker(threadId, tokens.iloc[threadId], queue)
        worker.daemon = True
        worker.start()

    # put tasks into queue
    for _, epoch in epochs.iterrows():
        queue.put(epoch)

    queue.join()  # main thread waits for queue


def startSingleThread(epochs: pd.DataFrame):

    tokenFileName = 'config/token.txt'
    tokens = pd.read_csv(tokenFileName)
    worker = DataCollectionWorker(0, tokens.iloc[0])

    for _, epoch in epochs.iterrows():

        postAttributes = worker._collect(epoch)

        if not postAttributes.empty:  # some images found
            processedAttributes = worker._process(postAttributes)
            DataCollectionWorker._save(epoch, processedAttributes)

        DataCollectionWorker._processedEpochsInThisRun += 1
        DataCollectionWorker._processedEpochsInTotal += 1
        DataCollectionWorker._processedImages += len(postAttributes)
        DataCollectionWorker._printProgressInformation()


def loadData(filesDirectory: str):

    allFiles = glob.glob(os.path.join(filesDirectory, "*.csv"))
    mergedData = pd.concat((pd.read_csv(f, index_col=0) for f in allFiles))
    mergedData.reset_index(drop=True, inplace=True)
    mergedData['imageobjects'] = mergedData['imageobjects'].apply(eval)
    mergedData['words'] = mergedData['words'].apply(eval)
    mergedData['created_local_time'] = mergedData['created_local_time'].astype('int64')
    mergedData['created_utc_time'] = mergedData['created_utc_time'].astype('int64')
    mergedData['likes'] = mergedData['likes'].fillna(0).astype('int64')

    return mergedData


if __name__ == '__main__':
    main()
