#!/usr/bin/env python3

import cv2
import numpy as np
import pandas as pd
import pytesseract
import re
import string
import wordninja


class OCR:
    '''
    Extract textual information from images.
    '''
    def extractText(self, rawImages) -> list:
        '''
        Uses TesseractOCR to recognize text in images.

        @return: series with list of recognized words as elements
        '''
        wordLists = []

        for rawImage in rawImages:

            processedImage = self._prepareImage(rawImage)
            recognizedCharacters = self._recognizeCharacters(processedImage)
            filteredText = self._filterCharacters(recognizedCharacters)

            # split to words
            wordList = wordninja.split(filteredText)

            # remove empty strings
            wordList = list(filter(None, wordList))

            wordLists.append(wordList)

        return pd.Series(wordLists)

    def _recognizeCharacters(self, processedImage):
        tesseractConfig = r"--oem 3 --psm 11 -c tessedit_char_whitelist= 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.?!,: '"
        recognizedText = pytesseract.image_to_string(processedImage, lang='eng', config=tesseractConfig)
        return recognizedText

    def _filterCharacters(self, text: str) -> str:
        filteredText = text.replace('\n', '')
        filteredText = filteredText.encode('ascii', 'ignore').decode()
        filteredText = re.sub("[^A-Za-z0-9'.?!:,-]+", '', filteredText)
        return filteredText

    def _prepareImage(self, originalImage):
        processedImage = np.array(originalImage, dtype=np.uint8)

        # convert image to approriate bitmap
        processedImage = cv2.cvtColor(processedImage, cv2.COLOR_BGRA2BGR)

        # filter noise
        processedImage = cv2.bilateralFilter(processedImage, 5, 55, 60)

        # check number of channels to see if the image is grayscale or RGB
        numOfChannels = self._getNumberOfChannels(processedImage)

        if numOfChannels != 1:
            # project image to grayscale if it is not
            processedImage = cv2.cvtColor(processedImage, cv2.COLOR_BGR2GRAY)

        # apply binary transformation (pure black or white)
        _, processedImage = cv2.threshold(processedImage, 240, 255, 1)

        return processedImage

    def _getNumberOfChannels(self, image):
        if image.ndim == 2:
            channels = 1 # grayscale image
        elif image.ndim == 3:
            channels = image.shape[-1] # RGB image
        else:
            assert(False) # unknown image format
        return channels