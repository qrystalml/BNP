# This repository contains source code for BNP Paribas technical test.

In order to make this repository accessible for it's audience with different levels of technical expertise, it has been structured in its simplistic form with clear instructions on how to setup and execute functions in various module to achieve desired outcome at various steps.

## File Structure

```
├── .gitignore               <- Files that should be ignored by git.
├── README.md                <- The top-level README with description and instructions for using this project.
├── requirements.txt         <- The requirements file with list of additional libraries for reproducing the environment.
├── config_data.py           <- The module containing the definition of configurable parameters used in the repository.
├── enron_data.py            <- The module to hold Enron dataset processing logic.
├── summarize-enron.py       <- The module with main function to wrap the desired summarization functionality as a command line utility.
├── results			         <- Folder to contain generated result files with summarization data
├── logs			         <- Folder to hold processing logs
└── docs			         <- Folder with relevant documents related to the project
```
## Setup
   The source code in this repository has been created in the Anaconda environment. In order to get smooth execution, it is advised to run the code in a conda environment.
   Please make sure the input data file `enron-event-history-all.csv` has been placed in the same directory of summarize-enron.py script
   
## Install the project's development and runtime requirements:

    `pip install -r requirements.txt`

## To get summarization results, please run the following command

    `python summarize-enron.py enron-event-history-all.csv`
  
## Results
   All the result files generated with summary data will be placed in the `results` folder.
  
* Sent and received email count for each person is stored in the file with time stamp attached in the following format
    
    `person_wise_email_count_stats_timestamp.csv`

* Number of emails sent over time by top 10 prolific users are saved as
    
    `senders_monthly_email_count_dist_plot_timestamp.png`

* Number of unique contacts over time for the top 10 prolific users are saved as
    
    `recipient_monthly_unique_contact_count_relative_dist_plot_timestamp.png`
	
## Resolving known errors
	If you run into the  error on unix machine, run following commands as workaround to overcome
	'sudo apt update'
	'sudo apt install libgl1-mesa-glx'
	
## Assumptions, Adoptions & Extensions
   While developing the tasks for the technical test, following assumptions have been made and configurations are provided wherever possible
   * It has been assumed that each sender and recipient names/email addresses are unique and clean. There is no preprocessing done on them.
   * In order to produce visualizations of data over time, I have used month as unit and data has been aggregated for each month before plotting them
   * All the plots have been provided as line charts
   * For simplicity and clarity, I have taken top 5 prolific senders for generating visualizations. This can be configured simply by changing the config value.
   * An exclusion list has been added to the list of senders to make sure enterprise ids like 'notes', 'announcements' etc will not be considered for visualizations.
    
