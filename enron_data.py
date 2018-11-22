"""
Created on Wed Nov 21 16:25:05 2018 
@author: Ravindra Babu

This module is a container class for the Enron event history email data. 
For the purpose of test, the class only contains the methods required for summarisation.
"""
# Module imports
import datetime
import time
import logging
import pandas as pd
import numpy as np
from config_data import Config

class EnronEventHistory():
    def __init__(self, input_data_path):
        '''
        This is a class initializer which reads the enron event history data as a pandas data frame.
        
        Input Parameters:
            input_data_path: location of the input data file containing Enron event history
        '''
        logging.debug('Instantiating enron event history object: begin')
        # Define columns to be used. 
        # Ignoring 'topic' and 'mode' as they always have same value.
        usecols = [0, 1, 2, 3]
        
        # Define column names
        column_names = ['event_timestamp', 'message_id', 'sender', 'recipient_list']

        # Read data from the input_data_path as a data frame
        self.event_history_df = pd.read_csv(
                                                input_data_path, 
                                                usecols=usecols, 
                                                names=column_names
                                            )
                                            
        # Parse unix format event time as datetime
        self.event_history_df['event_timestamp'] = pd.to_datetime(self.event_history_df['event_timestamp'], unit='ms')
        
        # As there could be multiple recipients for a message, 
        # expand the dataframe to contain one recipient per each record.
        self.event_history_df = pd.DataFrame(
                                                self.expand_recipient_list(self.event_history_df.values, 3),
                                                columns=self.event_history_df.columns
                                            )
        
        # Rename recipient_list to recipient for consistency
        self.event_history_df.rename(columns={'recipient_list':'recipient'}, inplace=True)
        
        logging.debug('Enron event history data shape: {}'.format(self.event_history_df.shape))
        
        logging.debug('Instantiating enron event history object: end')
        
        return
        
    def expand_recipient_list(self, data, col_idx, sep='|'):
        '''
        This function facilitates expansion of multiple recipients in a single column 
        to be expanded to different rows with the same values of other columns
        Input Parameters:
            data: input data containing all the features. This needs to be a numpy representation of the DataFrame
            col_idx: index of the column which needs to be expanded with list like values
            sep: delimiter of the column values to be expanded
        Returns:
            expanded data for the input column in the numpy array representation
        '''
        data = data.astype(str)
        n, m = data.shape
        col_value  = data[:, col_idx]
        other_cols = np.r_[0:col_idx, col_idx+1:m]
        asrt = np.append(col_idx, other_cols).argsort()
        other_cols_values = data[:, other_cols]
        col_value = np.core.defchararray.split(col_value, sep)
        expanded_col_values = np.concatenate(col_value)[:, None]
        counts = [len(x) for x in col_value.tolist()]
        rpt = np.arange(n).repeat(counts)
        
        return np.concatenate([expanded_col_values, other_cols_values[rpt]], axis=1)[:, asrt]
        
    def generate_email_count_stats(self):
        '''
        This function generates the summary count statistics for each person found in the input dataset
        
        Returns:
            count stats as a dataframe
            file path where these stats have been saved on the disk
        '''
        logging.debug('Generating email count stats: begin')
        
        # Get the number of emails sent by each person.
        # message_id is being used to identify unique messages with many recipient
        sender_count_stats_df = self.event_history_df\
                                    .groupby('sender')['message_id']\
                                    .nunique()\
                                    .rename('sent')\
                                    .reset_index()
        sender_count_stats_df.rename(
                                        columns={'sender':'person'}, 
                                        inplace=True
                                    )
        
        # Get the number of emails received by each person.
        recipient_count_stats_df = self.event_history_df\
                                        .groupby('recipient')['message_id']\
                                        .nunique()\
                                        .rename('received')\
                                        .reset_index()
        recipient_count_stats_df.rename(
                                            columns={'recipient':'person'}, 
                                            inplace=True
                                        )
        
        # Outer join both sent and received stats
        email_count_stats_df = pd.merge(
                                            sender_count_stats_df, 
                                            recipient_count_stats_df, 
                                            on='person', 
                                            how='outer'
                                        )
        
        # Replace null values with zero count value
        email_count_stats_df['sent']     = email_count_stats_df['sent'].fillna(value=0)
        email_count_stats_df['received'] = email_count_stats_df['received'].fillna(value=0)
        
        # Casting sent and received columns as integers
        email_count_stats_df[['sent','received']] = email_count_stats_df[['sent','received']].astype(int)
        
        # Sort records by the sent value in the descending order
        email_count_stats_df.sort_values(
                                            by='sent',
                                            ascending=False,
                                            inplace=True
                                        )
        
        #Save email count stats to disk
        time_str = time.strftime("%Y%m%d_%H%M%S")
        email_count_stats_file = Config.RESULTS_PATH + 'person_wise_email_count_stats_' + time_str + '.csv'
        
        logging.info('Saving email count stats to the file: {0}'.format(email_count_stats_file))
        email_count_stats_df.to_csv(
                                        email_count_stats_file, 
                                        index=False, 
                                        header=True
                                    )
    
        logging.debug('Generating email count stats: end')
        
        return email_count_stats_df, email_count_stats_file
        
