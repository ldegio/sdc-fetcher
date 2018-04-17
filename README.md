# sdc-fetcher
a small python script to download timelines from Sysdig Monitor into a pandas dataframe. The repository includes a class called Fetcher that can be independently used as data fetcher for statistical and analytics apps based on the Sysdig Monitor Data. The fetcher class takes care of:
- aligning the ruest for best performance
- splitting requests that generare more than 1000 timelines into chunks
- segmenting long requests into smaller time intervals with a number of samples digestable by the Sysdig Monitor backend
- reassembling the results into a single pandas dataframe

## Installation
pip install pandas
pip install sdcclient

## Usage
python main.py <token>
  
Where toke is your Sysdig Monitor token that you can get from the product user interface.
