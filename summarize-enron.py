"""
@author: Ravindra Babu

This module holds the wrapper function `main` to parse and process command line request.
"""
# Module imports
import sys, os
import argparse
import logging

from config_data import Config
from enron_data import EnronEventHistory

# configure logger
logging.basicConfig(level=Config.LOG_LEVEL,
                    format='%(asctime)-15s %(name)-5s %(levelname)-8s %(message)s',
                    filename=Config.LOG_PATH)
console = logging.StreamHandler()
console.setLevel(Config.LOG_LEVEL)
formatter = logging.Formatter('%(asctime)-15s %(name)-5s %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

def process_args(args):
    '''
    This function identifies the requested operation on entrant data and parses the input arguments extracting additional 
    or missing information from the Config module.
    
    Input Parameters:
        args: input arguments received from command line.
        defaults: Config object containing default values for various command arguments.
    
    Returns:
        parsed parameters.
    '''
    logging.debug('Parsing commandline arguments: begin')
    parser = argparse.ArgumentParser(description='summarise enron event history data')

    # Get input data file location
    parser.add_argument('enron_data_file', metavar='enron_data_file',
                        type=str, default=Config.INPUT_DATA_FILE,
                        help=('Location of the input enron data file'))

    parameters = parser.parse_args(args)
    
    logging.debug('Input enron data file: {}'.format(parameters.enron_data_file))
    
    logging.debug('Parsing commandline arguments: end')
    
    return parameters

def main(args=None):
    '''
    The main function is the entry point for processing enron summarization tasks.
    Input Parameters:
        args: list of command line arguments passed while requesting for a data summarization. 
              In this case it is only the input data file
    '''
    logging.info('Enron data summarisation: begin')
    
    try:
        # parse input arguments
        parameters = process_args(args)

        # Create EnronEventHistory object
        input_data_loc = parameters.enron_data_file
        eeh = EnronEventHistory(input_data_loc)
		
		# Create results folder if doesn't exist already
		if not os.path.exists(Config.RESULTS_PATH):
			os.makedirs(Config.RESULTS_PATH)
        
        # Generate the number of emails sent and received by each person
        # This corresponds to the task 1 of the technical exam
        email_count_stats_df, email_count_stats_file = eeh.generate_email_count_stats()
        
        # Generate sent email count distribution on monthly basis
        # This corresponds to the task 2 of the technical exam
        person_list = email_count_stats_df['person'][:Config.TASK_2_NUM_SENDERS].tolist()
        
        # Remove email names that doesn't represent real persons
        person_list = [x for x in person_list if x not in Config.TASK_2_EXCLUDE_SENDERS]
        
        # Generate sent email count plot
        eeh_monthly_sent_count_df, sent_email_count_dist_plot_file = eeh.generate_sent_email_count_distribution(person_list)
        
        # Generate relative unique contact count plot for the same persons
        eeh_monthly_unq_contacts_df, rel_unq_contacts_count_dist_plot_file = eeh.generate_relative_unique_contacts_distribution(person_list)
        
    except Exception as e:
        logging.error("Generation of Enron data summary tasks failed. Error details are: ")
        logging.error("Error {0}".format(str(e).encode("utf-8")))
        
        raise e
    
    logging.info('Enron data summarisation: end')
    
    return  

# Invoke main method
main()
