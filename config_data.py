"""
Created on Wed Nov 21 15:11:27 2018

@author: Ravindra Babu

This module holds the default configuration parameters used in various functions.
"""
# Module imports
import logging

class Config(object):
    """
    Default config.
    """

    # Logging configuration
    LOG_PATH = './logs/processing_log.log'
    LOG_LEVEL = logging.DEBUG
    
    # Input data file. Only used if not provided as command line argument
    INPUT_DATA_FILE = './enron-event-history-all.csv'
    
    # Results configuration
    RESULTS_PATH = './results/'
    
