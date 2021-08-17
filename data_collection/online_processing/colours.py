#!/usr/bin/env python3

import cv2
import numpy as np
import pandas as pd
import colorsys


class Colours:
    '''
    Extract color information from images.
    '''
    _colours = pd.DataFrame([['aqua'         , (127, 255, 212)],
                             ['black'        , (  0,   0,   0)],
                             ['blue'         , (  0,   0, 255)],
                             ['bright_yellow', (255, 255,   0)],
                             ['coral'        , (255, 127,  80)],
                             ['cyan'         , (  0, 255, 255)],
                             ['dark_green'   , (  0, 100,   0)],
                             ['dust_brown'   , (139, 115,  85)],
                             ['gold'         , (255, 215,   0)],
                             ['gray'         , (127, 127, 127)],
                             ['gray_blue'    , ( 83, 134, 139)],
                             ['green'        , (  0, 255,   0)],
                             ['light_blue'   , (173, 216, 230)],
                             ['maroon'       , (127,   0,   0)],
                             ['mustard'      , (227, 207,  87)],
                             ['off_white'    , (250, 235, 215)],
                             ['olive'        , (127, 127,   0)],
                             ['orange'       , (255,  97,   3)],
                             ['peach'        , (255, 229, 180)],
                             ['pink'         , (255,  20, 147)],
                             ['plum'         , (221, 160, 221)],
                             ['purple'       , (138,  43, 226)],
                             ['red'          , (255,   0,   0)],
                             ['red_brown'    , (165,  42,  42)],
                             ['salmon'       , (255, 127,  80)],
                             ['siena_brown'  , (165,  42,  42)],
                             ['teal'         , (  0, 127, 127)],
                             ['white'        , (248, 248, 255)],
                             ['yellow_green' , (173, 255,  47)]], columns=['colour', 'rgb'])

    _colourRanges = pd.DataFrame()

    @classmethod
    def initializeColourRanges(cls):

        ranges = []
        for _, colour in cls._colours.iterrows():
            red, green,      blue  = colour['rgb']
            hue, saturation, value = colorsys.rgb_to_hsv(red/255, green/255, blue/255)

            hue        *= 360
            saturation *= 255
            value      *= 255

            lowerHSVLimit = (hue-10, saturation-40, value-40)
            upperHSVLimit = (hue+10, saturation+40, value+40)

            ranges.append([lowerHSVLimit, upperHSVLimit])

        cls._colourRanges = pd.DataFrame(ranges, columns=['lowerHSV', 'upperHSV'])
        cls._colourRanges['colour'] = cls._colours['colour']

    @classmethod
    def extractColours(cls, rawImages: pd.Series) -> pd.DataFrame:
        '''
        Calculate presence of each color in the list of images.
        '''
        colourLists = []

        for image in rawImages:

            rgbImage = image.convert('RGB')
            imageArray = np.array(rgbImage)

            colourList = {}  # initialize dictionary for next image
            avgRed, avgGreen, avgBlue = cls._extractAverageRGB(imageArray)
            avgHue, avgSaturation, avgValue = cls._extractAverageHSV(imageArray)

            colourList.update({'red': avgRed, 'green': avgGreen, 'blue': avgBlue})
            colourList.update({'hue': avgHue, 'saturation': avgSaturation, 'value': avgValue})

            for _, colorRange in cls._colourRanges.iterrows():
                avgColourStrength = cls._extractColourStrength(imageArray, colorRange)
                colourList.update({colorRange['colour']: avgColourStrength})

            colourLists.append(colourList)

        return pd.DataFrame(colourLists)

    @staticmethod
    def _extractAverageRGB(image: np.array):
        red, green, blue = cv2.split(image)
        return red.mean(), green.mean(), blue.mean()

    @staticmethod
    def _extractAverageHSV(image: np.array):
        '''
        Compute hue, saturation and value.
        '''
        hue, saturation, value = cv2.split(cv2.cvtColor(image, cv2.COLOR_RGB2HSV))
        return hue.mean(), saturation.mean(), value.mean()

    @staticmethod
    def _extractColourStrength(image: np.array, colour):
        hsv_image = cv2.cvtColor(image, cv2.COLOR_RGB2HSV)
        mask = cv2.inRange(hsv_image, colour['lowerHSV'], colour['upperHSV'])
        return (np.sum(mask) / (image.shape[0] * image.shape[1]))
