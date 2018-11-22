"""
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
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from config_data import Config
%matplotlib inline

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
                                            
        # As there could be multiple recipients for a message, 
        # expand the dataframe to contain one recipient per each record.
        self.event_history_df = pd.DataFrame(
                                                self.expand_recipient_list(self.event_history_df.values, 3),
                                                columns=self.event_history_df.columns
                                            )
        
        # Rename recipient_list to recipient for consistency
        self.event_history_df.rename(columns={'recipient_list':'recipient'}, inplace=True)

        # Parse unix format event time as datetime object
        self.event_history_df['event_timestamp'] = pd.to_datetime(
                                                                    self.event_history_df['event_timestamp'], 
                                                                    unit='ms'
                                                                 )
                                                                 
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
        
    def generate_sent_email_count_distribution(self, sender_list):
        '''
        This function generates the time series distribution of number of emails sent by the given senders
        
        Input Parameters:
            sender_list: List of senders to be considered for extracting distribution stats
        Returns:
            count distribution as a dataframe
            plot file path containing these distribution stats
        '''
        logging.debug('Generating sent email count distribution: begin')
        
        # Filter event history data to contain only details of senders list
        filtered_cols = ['event_timestamp','sender', 'message_id']
        senders_eeh_df = self.event_history_df.loc[
                                                    self.event_history_df['sender'].isin(sender_list)
                                                  ][filtered_cols]
        # Remove duplicates
        senders_eeh_df.drop_duplicates(inplace=True)
        
        # Set event_timestamp as index and sort records by it.
        senders_eeh_df.set_index('event_timestamp', inplace=True)
        senders_eeh_df.sort_index(inplace=True)
        
        # Get the number of emails sent by each person each month
        eeh_monthly_sent_count_df = senders_eeh_df.groupby('sender')['message_id']\
                                                  .resample('M')\
                                                  .count()\
                                                  .rename('sent')
                                                  
        # Unstack count data to convert individual sender as a column
        eeh_monthly_sent_count_df = eeh_monthly_sent_count_df.unstack('sender')
        eeh_monthly_sent_count_df.fillna(value=0, inplace=True)
        
        # Plot the count distribution as a line chart
        # Create a figure and get subplot axes
        sns.set()        
        fig = plt.figure()
        ax = fig.add_subplot(111)
        
        # Plot data
        event_month = eeh_monthly_sent_count_df.index.to_pydatetime()
        ax.plot(event_month, eeh_monthly_sent_count_df)

        # Get date formatting parameters
        years = mdates.YearLocator()
        months = mdates.MonthLocator()
        monthsFmt = mdates.DateFormatter('%b')
        yearsFmt = mdates.DateFormatter('\n\n%Y')  # add some space for the year label

        # Set plot formatting parameters
        ax.legend(list(eeh_monthly_sent_count_df.columns))
        ax.xaxis.set_minor_locator(months)
        ax.xaxis.set_minor_formatter(monthsFmt)
        plt.setp(ax.xaxis.get_minorticklabels(), rotation=90)
        ax.xaxis.set_major_locator(years)
        ax.xaxis.set_major_formatter(yearsFmt)
        ax.tick_params(which='major', length=7, labelcolor='b', color='b', labelsize='medium')
        ax.tick_params(which='minor', length=4, labelcolor='g', color='g', labelsize='small')
        
        # Set plot title and labels
        plt.title('Sent E-mail Count Distribution over Time')
        plt.xlabel('Event time (monthly)')
        plt.ylabel('Number of emails sent')
        
        #Save sent email count distribution plot to disk
        time_str = time.strftime("%Y%m%d_%H%M%S")
        sent_email_count_dist_plot_file = Config.RESULTS_PATH + 'senders_monthly_email_count_dist_plot_' + time_str + '.png'
        
        logging.info('Saving sent email count distribution plot to the file: {0}'.format(sent_email_count_dist_plot_file))
        plt.savefig(sent_email_count_dist_plot_file, bbox_inches='tight')
    
        logging.debug('Generating sent email count distribution: end')
        
        return eeh_monthly_sent_count_df, sent_email_count_dist_plot_file

    def generate_relative_unique_contacts_distribution(self, recipient_list):
        '''
        This function generates the time series distribution of relative unique contact count for given recipient list
        
        Input Parameters:
            recipient_list: List of recipients to be considered for extracting relative distribution stats
        Returns:
            relative unique contact distribution as a dataframe
            plot file path containing these distribution stats
        '''
        logging.debug('Generating relative unique contacts distribution: begin')
        
        # Filter event history data to contain only details of recipient list
        recipient_eeh_df = self.event_history_df.loc[
                                                        self.event_history_df['recipient'].isin(recipient_list)
                                                    ]
        
        # Set event_timestamp as index and sort records by it.
        recipient_eeh_df.set_index('event_timestamp', inplace=True)
        recipient_eeh_df.sort_index(inplace=True)
        
        # Get the number of unique senders for each person each month
        eeh_monthly_unq_contacts_df = recipient_eeh_df.groupby('recipient')['sender']\
                                                      .resample('M')\
                                                      .nunique()\
                                                      .rename('unique_contacts')
                                                  
        # Unstack count data to convert individual recipient as a column
        eeh_monthly_unq_contacts_df = eeh_monthly_unq_contacts_df.unstack('recipient')
        eeh_monthly_unq_contacts_df.fillna(value=0, inplace=True)
        
        # Generate relative distribution of unique contact count against all recipients
        eeh_monthly_unq_contacts_df = eeh_monthly_unq_contacts_df.div(eeh_monthly_unq_contacts_df.sum(axis=1), axis=0)
        
        # Plot the count distribution as a line chart
        # Create a figure and get subplot axes
        sns.set()        
        fig = plt.figure()
        ax = fig.add_subplot(111)
        
        # Plot data
        event_month = eeh_monthly_unq_contacts_df.index.to_pydatetime()
        ax.plot(event_month, eeh_monthly_unq_contacts_df)

        # Get date formatting parameters
        years = mdates.YearLocator()
        months = mdates.MonthLocator()
        monthsFmt = mdates.DateFormatter('%b')
        yearsFmt = mdates.DateFormatter('\n\n%Y')  # add some space for the year label

        # Set plot formatting parameters
        ax.legend(list(eeh_monthly_unq_contacts_df.columns))
        ax.xaxis.set_minor_locator(months)
        ax.xaxis.set_minor_formatter(monthsFmt)
        plt.setp(ax.xaxis.get_minorticklabels(), rotation=90)
        ax.xaxis.set_major_locator(years)
        ax.xaxis.set_major_formatter(yearsFmt)
        ax.tick_params(which='major', length=7, labelcolor='b', color='b', labelsize='medium')
        ax.tick_params(which='minor', length=4, labelcolor='g', color='g', labelsize='8')
        
        # Set plot title and labels
        plt.title('Relative Unique Contact Distribution over Time')
        plt.xlabel('Event time (monthly)')
        plt.ylabel('Relative unique contacts')
        
        #Save relative unique contact count distribution plot to disk
        time_str = time.strftime("%Y%m%d_%H%M%S")
        rel_unq_contacts_count_dist_plot_file = Config.RESULTS_PATH + 'recipient_monthly_unique_contact_count_relative_dist_plot_' + time_str + '.png'
        
        logging.info('Saving relative unique contact count distribution plot to the file: {0}'.format(rel_unq_contacts_count_dist_plot_file))
        plt.savefig(rel_unq_contacts_count_dist_plot_file, bbox_inches='tight')
    
        logging.debug('Generating relative unique contacts distribution: end')
        
        return eeh_monthly_unq_contacts_df, rel_unq_contacts_count_dist_plot_file        
