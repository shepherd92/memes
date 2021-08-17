#!/usr/bin/env python3

import pandas as pd
import datetime as dt
from os import listdir
from os.path import splitext


class DataCollectionConfig:

    def __init__(self):
        self._downloadDateInterval = None       # dates between which datacollection should take place
        self._downloadTimeInDayInterval = None  # specify hours between which download shall happen
        self._numOfPostsPerEpoch = -1           # number of posts to download in each epoch

        self.frequency = 1                      # number of days to skip between two days to collect (in days)
        self.numOfPostPerDay = 1000             # maximum number of posts to download for each day
        self.continueDownload = False           # if true, the already downloaded data is skipped
        self.subreddits = list()                # list of subreddits to use for downloading
        self.outputDirectory = str()            # output directory to download the data to


    def setDateInterval(self, startDateStr: str, endDateStr: str):
        '''
        Set dates to use for downloading.
        '''
        self._downloadDateInterval = {'startDate': dt.datetime.strptime(startDateStr, '%Y/%m/%d'),
                                      'endDate':   dt.datetime.strptime(endDateStr  , '%Y/%m/%d')}


    def getStartDate(self, startDateStr: str, endDateStr: str):
        '''
        Set dates to use for downloading.
        '''
        self._downloadDateInterval = {'startDate': dt.datetime.strptime(startDateStr, '%Y/%m/%d'),
                                      'endDate':   dt.datetime.strptime(endDateStr  , '%Y/%m/%d')}


    def setTimeInDayInterval(self, startTime: str, endTime: str):
        '''
        Set frequency of days to use for downloading.
        '''
        self._downloadTimeInDayInterval = {'startTime': dt.datetime.strptime(startTime, '%H:%M:%S'),
                                           'endTime':   dt.datetime.strptime(endTime,   '%H:%M:%S')}


    def getEpochs(self) -> pd.DataFrame:
        '''
        Return epochs in a dataframe.

        Calculate start time, end time, limit for each epoch.
        '''
        deltaStartTime = dt.timedelta(hours  =self._downloadTimeInDayInterval['startTime'].hour,
                                      minutes=self._downloadTimeInDayInterval['startTime'].minute,
                                      seconds=self._downloadTimeInDayInterval['startTime'].second)

        deltaEndTime = dt.timedelta(hours  =self._downloadTimeInDayInterval['endTime'].hour,
                                    minutes=self._downloadTimeInDayInterval['endTime'].minute,
                                    seconds=self._downloadTimeInDayInterval['endTime'].second)

        hoursInDayToDownload = int((deltaEndTime - deltaStartTime).seconds / 3600) + 1
        self._numOfPostsPerEpoch = int(self.numOfPostPerDay / hoursInDayToDownload) + 1

        listOfEpochs = []

        for day in range(0, int((self._downloadDateInterval['endDate'] - self._downloadDateInterval['startDate']).days), self.frequency):
            startOfDay = self._downloadDateInterval['startDate'] + dt.timedelta(day) + deltaStartTime

            for hour in range(hoursInDayToDownload):
                listOfEpochs.append({'start'     : startOfDay + dt.timedelta(hours=hour  ),
                                     'end'       : startOfDay + dt.timedelta(hours=hour+1),
                                     'numOfPosts': self._numOfPostsPerEpoch})

        return pd.DataFrame(listOfEpochs)


    def removeDownloadedEpochs(self, epochs: pd.DataFrame, outputDirectory: str) -> pd.DataFrame:
        '''
        Remove those epocs that are already downloaded.

        Help to continue data collection after it stopped.
        '''
        existingFileNames = [fileName for fileName in listdir(outputDirectory) if fileName.endswith('.csv')]

        if 0 != len(existingFileNames):
            # last file is probably only partially downloaded, remove that
            existingFileNames.sort()
            existingFileNames.pop()

        listOfDownloadedEpochs = []

        for fileName in existingFileNames:
            start, end = splitext(fileName)[0].split('_')
            downloadedEpoch = {'start'     : dt.datetime.strptime(start, '%Y%m%d%H%M%S'),
                               'end'       : dt.datetime.strptime(end  , '%Y%m%d%H%M%S'),
                               'numOfPosts': self._numOfPostsPerEpoch}
            listOfDownloadedEpochs.append(downloadedEpoch)

        downloadedEpochs = pd.DataFrame(listOfDownloadedEpochs)

        if not downloadedEpochs.empty:
            epochs = pd.merge(epochs, downloadedEpochs, indicator=True, how='outer') \
                     .query("_merge=='left_only'").drop('_merge', axis=1)

        return epochs
