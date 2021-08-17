#!/usr/bin/env python3

import numpy as np
import pandas as pd

import efficientnet.keras as en
from keras.applications.imagenet_utils import decode_predictions
from skimage.io import imread

from PIL import Image

from .imageclassifier import ImageClassifier


class EfficientNetClassifier(ImageClassifier):
    def __init__(self, version: int):
        if 0 == version:
            self._prediction = en.EfficientNetB0(weights='imagenet')
        elif 1 == version:
            self._prediction = en.EfficientNetB1(weights='imagenet')
        elif 2 == version:
            self._prediction = en.EfficientNetB2(weights='imagenet')
        elif 3 == version:
            self._prediction = en.EfficientNetB3(weights='imagenet')
        elif 4 == version:
            self._prediction = en.EfficientNetB4(weights='imagenet')
        elif 5 == version:
            self._prediction = en.EfficientNetB5(weights='imagenet')
        elif 6 == version:
            self._prediction = en.EfficientNetB6(weights='imagenet')
        elif 7 == version:
            self._prediction = en.EfficientNetB7(weights='imagenet')

        self._imageSize = self._prediction.input_shape[1]

    def classify(self, images: pd.Series) -> pd.Series:
        '''
        Take image and return object list recognized with probability higher than a threshold.
        '''
        preprocessedImages = self._preprocessInput(images)
        y = self._prediction.predict(preprocessedImages)
        predictions = decode_predictions(y)  # list of predictions for each image

        predictedObjects = []
        for prediction in predictions:
            # prediction: list of tuples (id, objectname, probability)
            predictiondf = pd.DataFrame(prediction, columns=['node', 'object', 'probability'])
            predictedObjects.append(predictiondf.loc[(predictiondf['probability'] >= 0.5)]['object'].to_list())

        return pd.Series(predictedObjects)
    
    def _preprocessInput(self, images):
    
        preprocessedImages = []

        for image in images:

            x = image.convert('RGB')
            x = np.array(x)

            x = en.center_crop_and_resize(x, image_size=self._imageSize)
            x = en.preprocess_input(x)
            preprocessedImages.append(x)

        return np.array(preprocessedImages)