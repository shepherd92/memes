#!/usr/bin/env python3

import numpy as np
import pandas as pd
from imageai.Classification import ImageClassification
from .imageclassifier import ImageClassifier


class ImageAIClassifier(ImageClassifier):

    probabilityThreshold = 0.25

    def __init__(self):
        self.prediction = ImageClassification()
        self.prediction.setModelTypeAsDenseNet121()
        self.prediction.setModelPath('imagecontent/models/DenseNet-BC-121-32.h5')
        self.prediction.loadModel()

    def classify(self, images: pd.Series) -> pd.Series:

        predictedObjectsList = []

        for image in images:
            rgbImage = image.convert('RGB')
            imageArray = np.array(rgbImage)

            predictions = self.prediction.classifyImage(imageArray, input_type='array')
            transposedPredictions = [list(x) for x in zip(*predictions)]
            predictedObjectsInImage = [row[0] for row in transposedPredictions if row[1] >= 100 * ImageAIClassifier.probabilityThreshold]
            predictedObjectsList.append(predictedObjectsInImage)

        return pd.Series(predictedObjectsList)