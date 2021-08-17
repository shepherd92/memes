#!/usr/bin/env python3

import pandas as pd
import datetime as dt
from threading import Thread, Lock
import os

from reddit_interface import Reddit

# ONLINE PROCESSING
# from online_processing.ocr import OCR
# from online_processing.colours import Colours
# from online_processing.imagecontent.imageai import ImageAIClassifier
# from online_processing.imagecontent.efficientnet import EfficientNetClassifier


class DataCollectionWorker(Thread):
    '''
    Worker thread to handle data collection.
    '''
    _lock = Lock()  # static lock object to access critical parts
    _processedEpochsInTotal = 0  # static variable to track progress
    _processedEpochsInThisRun = 0  # static variable to track progress
    _processedImages = 0  # static variable to track progress
    _startTime = 0
    _subReddits = []
    _numOfEpochs = 0
    _outputDirectory = ''


    def __init__(self, threadId: int, token, queue = None):

        self._threadId = threadId
        self._interface = Reddit(token)
        self._collectedData = None

        # ONLINE PROCESSING
        # self._ocr = OCR()
        # self._colours = Colours()
        # self._imageClassifier = EfficientNetClassifier(5)  # ImageAIClassifier()

        Thread.__init__(self)  # base class constructor
        self._queue = queue


    @classmethod
    def initialize(cls, subReddits: list, outputDirectory: str):
        cls._subReddits = subReddits
        cls._outputDirectory = outputDirectory
        cls._startTime = dt.datetime.now()

        # ONLINE PROCESSING
        # Colours.initializeColourRanges()

        if not os.path.exists(DataCollectionWorker._outputDirectory):
            os.makedirs(DataCollectionWorker._outputDirectory)


    def run(self):
        '''
        Function doing all tasks.

        Acquire tasks from queue object.
        '''
        while True:
            # Get the work from the queue and expand the tuple
            epoch = self._queue.get()

            try:
                postAttributes = self._collect(epoch)

                if not postAttributes.empty:  # some images found
                    # ONLINE PROCESSING
                    # processedAttributes = self._process(postAttributes)
                    # DataCollectionWorker._save(epoch, processedAttributes)

                    DataCollectionWorker._save(epoch, postAttributes)

                with DataCollectionWorker._lock:
                    DataCollectionWorker._processedEpochsInTotal += 1
                    DataCollectionWorker._processedEpochsInThisRun += 1
                    DataCollectionWorker._processedImages += len(postAttributes)
                    DataCollectionWorker._printProgressInformation()

            finally:
                self._queue.task_done()


    def _collect(self, epoch):
        listOfPostAttributes = []

        remainingMemesToDownload = epoch['numOfPosts']
        for subReddit in self._subReddits:
            epoch['numOfPosts'] = remainingMemesToDownload
            postAttributes = self._interface.getPosts(subReddit, epoch)

            numCollectedMemes = len(postAttributes)
            if 0 != numCollectedMemes:
                listOfPostAttributes.append(postAttributes)

            remainingMemesToDownload -= numCollectedMemes
            if remainingMemesToDownload <= 0:
                break

        if listOfPostAttributes:
            postAttributes = pd.concat(listOfPostAttributes, ignore_index=True)
        else:
            postAttributes = pd.DataFrame()

        return postAttributes


    # ONLINE PROCESSING
    '''
    def _process(self, postAttributes: pd.DataFrame):
        postAttributes['imageobjects'] = self._imageClassifier.classify(postAttributes['image'])
        postAttributes['words']        = self._ocr.extractText(postAttributes['image'])
        imageColours                   = self._colours.extractColours(postAttributes['image'])
        postAttributes = pd.concat([postAttributes, imageColours], axis=1)

        # do not keep image itself, all atributes were extracted
        postAttributes.drop('image', axis='columns', inplace=True)

        return postAttributes
    '''

    @classmethod
    def _save(cls, epoch, attributes):
        '''
        Print collected data into the file belonging to the epoch.
        '''
        outputFileName = f"{cls._outputDirectory}/" \
            + f"{epoch['start'].strftime('%Y%m%d%H%M%S')}" \
            + f"_{epoch['end'].strftime('%Y%m%d%H%M%S')}.csv"

        attributes.to_csv(outputFileName, float_format='%.4f')


    @classmethod
    def _printProgressInformation(cls):
        '''
        Print information on the progress of the data collection.
        '''
        totalProgress = cls._processedEpochsInTotal / cls._numOfEpochs
        thisRunProgress = cls._processedEpochsInThisRun / (cls._numOfEpochs - cls._processedEpochsInTotal + cls._processedEpochsInThisRun)

        deltaSinceStart = dt.datetime.now() - cls._startTime
        estimatedFinish = cls._startTime + dt.timedelta(seconds=deltaSinceStart.seconds / thisRunProgress)

        print(f"\rProgress: {cls._processedEpochsInThisRun} ({cls._processedEpochsInTotal}) epochs ({totalProgress * 100:.2f}%)"
              + f", ETA: {estimatedFinish.strftime('%Y-%m-%d %H:%M')}."
              , end='', flush=True)
