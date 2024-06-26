'''
Module for querying, filtering, and prepping New York Times articles. Make sure to pip intall pynytimes
Python package which allows easier interaction with NYT API (python -m pip install --upgrade pynytimes).

Note: NYT API has a limit of 10 queries per minute or 4000 per day.
'''

import time
import datetime
from tqdm import tqdm
import json
from pynytimes import NYTAPI
from collections import defaultdict
from nltk.sentiment.vader import SentimentIntensityAnalyzer

API_KEY = "89Svqeqz1rDAtcQz3ZEKNwEudUkxAXMi"
nyt = NYTAPI(API_KEY, parse_dates=True)

# Query filters
NEWS_DESK_VALS = [
    "Business Day",
    "Business",
    "Energy",
    "Entrepreneurs",
    "Financial",
    "Flight",
    "Foreign",
    "Jobs",
    "Personal Investing",
    "Politics",
    "Science",
    "Technology",
    "U.S.",
    "Washington",
    "Wealth",
    "Working",
    "Workplace",
    "World",
    "Your Money"
]

MATERIALS = [
    "Article",
    "An Analysis",
    "An Appraisal",
    "Economic Analysis",
    "Editorial",
    "Interview",
    "News",
    "News Analysis",
    "Op-Ed",
    "Special Report"
]

def query_all_articles(start_date, end_date):
    query_result = []
    current_date = start_date
    total_days = (end_date - start_date).days + 1
    progress_bar = tqdm(total=total_days, desc="Progress") # show query progress
    
    day_count = 0
    while current_date <= end_date:

        # Skip weekends (Saturday and Sunday)
        if current_date.weekday() >= 5:  # Saturday is 5, Sunday is 6
            current_date += datetime.timedelta(days=1)
            continue
        
        # Make query
        query_result.extend(nyt.article_search(
            dates={
                "begin": current_date, 
                "end": current_date + datetime.timedelta(days=1)
                },
            options={
                "sort": "oldest",
                "news_desk": NEWS_DESK_VALS, 
                "type_of_material": MATERIALS
                }
        ))
        current_date += datetime.timedelta(days=1)
        day_count += 1
        progress_bar.update(1)

        
        # Sleep program 
        if day_count % 9 == 0:
            print("Waiting for 1 minute...")
            time.sleep(60)
    
    progress_bar.close()
    
    return extract_data(query_result)

def query_stock_articles(start_date, end_date, stock):
    query_result = []
    current_date = start_date
    total_days = (end_date - start_date).days + 1
    progress_bar = tqdm(total=total_days, desc="Progress") # show query progress
    
    day_count = 0
    while current_date <= end_date:

        # Skip weekends (Saturday and Sunday)
        if current_date.weekday() >= 5:  # Saturday is 5, Sunday is 6
            current_date += datetime.timedelta(days=1)
            continue
        
        # Make query
        article = nyt.article_search(
            query=stock,
            dates={
                "begin": current_date, 
                "end": current_date + datetime.timedelta(days=1)
                },
            options={
                "sort": "oldest",
                "news_desk": NEWS_DESK_VALS, 
                "type_of_material": MATERIALS,
                }
        )

        query_result.extend(article)
        current_date += datetime.timedelta(days=1)
        day_count += 1
        progress_bar.update(1)
        
        # Sleep program 
        if day_count % 9 == 0:
            print("Waiting for 1 minute...")
            time.sleep(60)
    
    progress_bar.close()
    
    return extract_data(query_result)


def extract_data(article_or_list):
    if isinstance(article_or_list, list):
        return [extract_data(article) for article in article_or_list]
    else:
        return {
            "abstract": article_or_list["abstract"],
            "web_url": article_or_list["web_url"],
            "source": article_or_list["source"],
            "headline": article_or_list["headline"]["main"],
            "keywords": [keyword["value"] for keyword in article_or_list["keywords"]],
            "pub_date": article_or_list["pub_date"],
            "document_type": article_or_list["document_type"],
            "news_desk": article_or_list["news_desk"],
            "section_name": article_or_list["section_name"],
            "type_of_material": article_or_list["type_of_material"],
            "_id": article_or_list["_id"],
            "word_count": article_or_list["word_count"],
            "uri": article_or_list["uri"]
        }


def get_pubdate_abstract(data_or_filename):
    if isinstance(data_or_filename, str):  # If a filename is provided
        with open(data_or_filename, 'r') as file:
            data = json.load(file)
    elif isinstance(data_or_filename, list):  # If a list of data is provided
        data = data_or_filename
    else:
        raise ValueError("Invalid input. Please provide either a filename (str) or a list of data.")
    
    # Create a set to store unique (pub_date, abstract) tuples
    unique_articles = set()
    filtered_data = []

    for article in data:
        # Extract pub_date and abstract
        pub_date = article["pub_date"]
        abstract = article["abstract"]
        
        # Check if the (pub_date, abstract) tuple is already in the set
        if (pub_date, abstract) not in unique_articles:
            # If not, add it to the set and append the article to the filtered data
            unique_articles.add((pub_date, abstract))
            filtered_data.append({"pub_date": pub_date, "abstract": abstract})

    return filtered_data

'''
Compared a list of stock specific news with a list of general news. Removes stock specific news from
general news and return the new list of general news.
'''
def remove_stock_news_from_general_news(general_news, stock_news):
    # Create a set of abstracts from stock_news for faster lookup
    stock_abstracts_set = {article["abstract"] for article in stock_news}
    
    # Filter general_news to remove articles with matching abstracts from stock_news
    filtered_general_news = [article for article in general_news
                             if article["abstract"] not in stock_abstracts_set]
    
    return filtered_general_news

'''
Groups abstracts of articles by common date and sorts by date.
'''
def group_data(data):
    # Group abstracts by date
    abstracts_by_date = defaultdict(list)
    for article in data:
        pub_date = str(article['pub_date']).split('T')[0]  # Convert to string and extract only the date part
        abstracts_by_date[pub_date].append(article['abstract'])
    
    # Sort the abstracts by date
    sorted_abstracts_by_date = dict(sorted(abstracts_by_date.items()))
    
    return sorted_abstracts_by_date


def merge_json_files(file1, file2):
    merged_data = {}

    # Read and parse data from the first file
    with open(file1, 'r') as f1:
        data1 = json.load(f1)
        merged_data.update(data1)

    # Read and parse data from the second file
    with open(file2, 'r') as f2:
        data2 = json.load(f2)
        merged_data.update(data2)

    return merged_data


def serialize_date(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def write_to_json(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, default=serialize_date, indent=4)


def read_json_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def main():
    start_date = datetime.datetime(2024, 1, 1)
    end_date = datetime.datetime(2024, 12, 31)

    # Generate the file dates
    filedates = f"{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}"


    # Query general news data and remove redundant info
    # gen_query = query_all_articles(start_date, end_date)
    # write_to_json(gen_query, "gen_news_" + filedates)
    # grouped_gen_data = group_data(get_pubdate_abstract('gen_news_2024-01-01_2024-12-31'))
    # write_to_json(grouped_gen_data, "gen_data_" + filedates)
    # merged_files = merge_json_files('data/general_news_2019-2023','gen_data_2024-01-01_2024-12-31')
    # write_to_json(merged_files, 'data/general_news_2019-2024')


    # Query stock news
    # stock_query = query_stock_articles(start_date, end_date, 'Apple')
    # stock_data = extract_data(stock_query)
    # write_to_json(stock_data, "apple_news_" + filedates)
    # filtered_stock_data = get_pubdate_abstract(stock_data)
    # write_to_json(filtered_stock_data, "apple_data_" + filedates)

    # filtered_gen_data = read_json_file("gen_data_2020-01-01_2020-12-31")
    # filtered_stock_data = read_json_file("apple_data_2020-01-01_2020-12-31")
    # filtered_gen_data = remove_stock_news_from_general_news(filtered_gen_data, filtered_stock_data)
    # write_to_json(filtered_gen_data, "gen_data_" + filedates)


if __name__ == "__main__":
    main()