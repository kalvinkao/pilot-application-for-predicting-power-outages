# -*- coding: utf-8 -*-
"""
Created on Mon Nov 20 14:29:14 2017

@author: kalvi
"""

import numpy as np
import scipy as sp
from sklearn.linear_model import LogisticRegression

def random_prediction():
  num_examples = 1000
  num_features = 10
  features = np.random.normal(size=(num_examples,num_features))
  outcomes = np.random.randint(0,2,num_examples).reshape(num_examples,1)
    
  train_data = features[:900,]
  train_labels = outcomes[:900,]
  test_data = features[900:,]
  test_labels = outcomes[900:,]
    
  lr = LogisticRegression()
  lr.fit(train_data, train_labels)
  lrPredictions = lr.predict(test_data)
    
  return lrPredictions

def steel_thread_prediction(train_data, train_labels, test_data):
  lr = LogisticRegression()
  lr.fit(train_data, train_labels)
  lrPredictions = lr.predict(test_data)
    
  return lrPredictions

def lr_prediction(train_data, train_labels, test_data):
  lr = LogisticRegression()
  lr.fit(train_data, train_labels)
  lr_probabilities = lr.predict_proba(test_data)
  lr_predictions = lr.predict(test_data)

  return lr_probabilities, lr_predictions