import logging
import os
import subprocess
import yaml
import pandas as pd
import datetime
import string
import gc
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import string
import dask.bag as db

################
# File Reading #
################

def read_config_file(filepath):
    with open(filepath, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)

def normalize_text(text):
    # Lowercase all letters
    text = text.lower()
    # Remove punctuation
    text = text.translate(str.maketrans("", "", string.punctuation))
    return text

def validate(df, table_config):
    file_type = table_config['file_type']
    source_file = "./" + table_config['file_name'] + f'.{file_type}' 
    file_size = os.path.getsize(source_file)
    file_size = file_size / (1024 * 1024 * 1024)
    
    '''
    Commented due to insufficient amount of memory of my pc
    checkpoint_dir = "C:/Users/BatuhanYILMAZ/Desktop/staj/week6/checkpoints" 
    df = df.persist(checkpoint=checkpoint_dir)
    '''
    min_size = int(table_config['validations']['file_size']['min_size'])
    max_size = int(table_config['validations']['file_size']['max_size'])
    if file_size < min_size or file_size > max_size:
        print("File size validation failed")
        print(f"""Expected file size should be between {min_size} - {max_size} GB """)
        print(f"""Size of the actual file : {file_size} GB""")
        return False

    text_length = (
        df.map(lambda text: text.replace(" ", ""))
        .flatten()
        .map(len)
        .sum()
        .compute()
    )
    min_length = int(table_config['validations']['text_length']['min_length'])
    max_length = int(table_config['validations']['text_length']['max_length'])
    if text_length < min_length or text_length > max_length:
        print("Text length validation failed")
        print(f"""Expected text length should be between {min_length} - {max_length} characters""")
        print(f"""Total text length of actual file : {text_length} characters""")
        return False
    
    
    word_count = (
        df.map(normalize_text)
        .str.split()
        .flatten()
        .count()
        .compute()
    )
    min_count = int(table_config['validations']['word_count']['min_count'])
    max_count = int(table_config['validations']['word_count']['max_count'])
    if word_count < min_count or word_count > max_count:
        print("Word count validation failed")
        print(f"""Expected word count should be between {min_count} - {max_count}""")
        print(f"""Total number of words in the actual file : {word_count}""")
        return False
    '''
    Commented due to insufficient amount of memory of my pc
    pattern = table_config['validations']['format_validation']['required_pattern']
    if not any(re.search(pattern, line) for line in df.compute()):
        print("Format validation failed")
        print("Expected file format should start with:", pattern)
        return False
    '''
    print("All validations passed successfully")
    print(f"""Size of the actual file : {file_size} GB""")
    print(f"""Total text length of actual file : {text_length} characters""")
    print(f"""Total number of words in the actual file : {word_count}""")
    #print(f"""Each paragraph of the file starts with the correct pattern : [pattern] """)
    return True

def get_data_summary(bag, table_config):
    print("---------------Data Summary---------------")
    # Normalize the text and count the total number of words
    total_words = (
        bag.map(normalize_text)
        .str.split()
        .flatten()
        .count()
        .compute()
    )
    print("Total number of words in file: ", total_words)
    
    # Total length of whole words in the file   
    total_length = (
        bag.map(lambda text: text.replace(" ", ""))
        .flatten()
        .map(len)
        .sum()
        .compute()
    )
    print("Total length of words in file: ", total_length)
    
    # Get the size of the file
    file_type = table_config['file_type']
    source_file = "./" + table_config['file_name'] + f'.{file_type}' 
    file_stats = os.stat(source_file)
    print(f'File Size in Bytes is {file_stats.st_size}')
    print(f'File Size in MegaBytes is {file_stats.st_size / (1024 * 1024)}')
    print(f'File Size in GigaBytes is {file_stats.st_size / (1024 * 1024 * 1024)}')
    
    word_counts = (
    bag.map(normalize_text)
    .str.split()
    .flatten()
    .frequencies()
    .compute()
    )

    # Print the word frequencies
    print("Frequency of words:")
    for word, count in word_counts:
        print(f"{word}: {count}")

def write(bag, table_config):
    # Convert the Bag to a DataFrame
    df = db.from_sequence(bag).to_dataframe()

    # Define the output file path
    output_file = "output_file.txt.gz"

    # Write the DataFrame to a single pipe-separated text file
    df.to_csv(output_file, sep=table_config['outbound_delimiter'], compression="gzip", single_file=True, index=False)
    
    print("Gzipped file created:", output_file)
