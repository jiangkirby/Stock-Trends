import time
import datetime
from tqdm import tqdm
import json
from pynytimes import NYTAPI
from collections import defaultdict
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import csv

import numpy as np
from textblob import TextBlob
from nltk.sentiment.vader import SentimentIntensityAnalyzer


def calculate_sentiment_scores(news_by_date):
    # Initialize VADER SentimentIntensityAnalyzer
    sid = SentimentIntensityAnalyzer()
    # Initialize an empty dictionary to store sentiment scores
    sentiment_scores_by_date = {}

    # Iterate over each date and its corresponding list of news headlines
    for date, headlines in news_by_date.items():
        # Initialize lists to store sentiment scores for each headline
        polarities = []
        subjectivities = []
        pos_scores = []
        neu_scores = []
        neg_scores = []

        # Iterate over each headline for the current date
        for headline in headlines:
            # Calculate sentiment scores using TextBlob
            blob = TextBlob(headline)
            polarities.append(blob.sentiment.polarity)
            subjectivities.append(blob.sentiment.subjectivity)

            # Calculate sentiment scores using VADER
            ss = sid.polarity_scores(headline)
            pos_scores.append(ss['pos'])
            neu_scores.append(ss['neu'])
            neg_scores.append(ss['neg'])

        # Calculate the average sentiment scores for the current date
        avg_polarity = np.mean(polarities)
        avg_subjectivity = np.mean(subjectivities)
        avg_pos_score = np.mean(pos_scores)
        avg_neu_score = np.mean(neu_scores)
        avg_neg_score = np.mean(neg_scores)

        # Store the sentiment scores for the current date in the dictionary
        sentiment_scores_by_date[date] = {
            'avg_polarity': avg_polarity,
            'avg_subjectivity': avg_subjectivity,
            'avg_pos_score': avg_pos_score,
            'avg_neu_score': avg_neu_score,
            'avg_neg_score': avg_neg_score
        }

    return sentiment_scores_by_date
    # Initialize VADER SentimentIntensityAnalyzer
    sid = SentimentIntensityAnalyzer()

    # Calculate sentiment scores for each date
    sentiments_by_date = {}
    for date, abstracts in abstracts_by_date.items():
        sentiments = {'pos': [], 'neu': [], 'neg': [], 'compound': []}  # Initialize sentiment scores
        for abstract in abstracts:
            sentiment_scores = sid.polarity_scores(abstract)
            for key, value in sentiment_scores.items():
                sentiments[key].append(value)
        sentiments_by_date[date] = sentiments

    return sentiments_by_date

def read_json_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def write_sentiment_scores_to_csv(sentiments, filename):
    # Open CSV file in write mode
    with open(filename, 'w', newline='') as csvfile:
        # Define CSV writer
        writer = csv.writer(csvfile)

        # Write header row
        writer.writerow(['Date', 'Avg_Polarity', 'Avg_Subjectivity', 'Avg_Pos_Score', 'Avg_Neu_Score', 'Avg_Neg_Score'])

        # Write sentiment scores data to CSV file
        for date, scores in sentiments.items():
            writer.writerow([date, scores['avg_polarity'], scores['avg_subjectivity'], scores['avg_pos_score'], scores['avg_neu_score'], scores['avg_neg_score']])


def main():
    abstracts = 'data/general_news_2019-2024'
    sentiments = calculate_sentiment_scores(read_json_file(abstracts))
    write_sentiment_scores_to_csv(sentiments, "data/sentiments/general_scores2024.csv")
    

if __name__ == "__main__":
    main()