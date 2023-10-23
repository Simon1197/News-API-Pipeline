# import configparser
# import datetime
# import pandas as pd
# import pathlib
# import sys
# import requests
# import hashlib
# import nltk
# from nltk.sentiment.vader import SentimentIntensityAnalyzer

# nltk.download('vader_lexicon')

# # Initialize the sentiment analyzer
# analyzer = SentimentIntensityAnalyzer()

# # Read Configuration File
# parser = configparser.ConfigParser()
# script_path = pathlib.Path(__file__).parent.resolve()
# config_file = "configuration.conf"
# parser.read(f"{script_path}/{config_file}")

# # Use command line argument as output file
# # name and also store as column value
# try:
#     output_name = sys.argv[1]
# except Exception as e:
#     print(f"Error with file input. Error {e}")
#     sys.exit(1)
# date_dag_run = datetime.datetime.strptime(output_name, "%Y%m%d")


# def main():
#     """Extract News data and load to CSV"""
#     api_key = parser.get("news_api_config", "api_key")
#     base_url, headers = api_connect(api_key)
    
#     # Calculate yesterday's date
#     yesterday = date_dag_run - datetime.timedelta(days=1)
#     yesterday_str = yesterday.strftime("%Y-%m-%d")
    
#     # Define a list of keywords
#     keywords = ["tesla"] #, "Apple", "Microsoft"
    
#     # Fetch news data without specifying sources
#     news_data = fetch_news_data(base_url, headers, keywords, yesterday_str)
#     transformed_data = transform_basic(news_data)
    
#     # Sort the data by popularity
#     transformed_data = transformed_data.sort_values(by='popularity', ascending=False)

#     # Save the combined data to a CSV file
#     output_file = f"/tmp/{output_name}.csv"
#     transformed_data.to_csv(output_file, index=False)

#     print(f"Data sorted by popularity saved to {output_file}")


# def api_connect(api_key):
#     """Connect to the News API"""
#     base_url = "https://newsapi.org/v2/"
#     headers = {"Authorization": f"Bearer {api_key}"}
#     return base_url, headers


# def fetch_news_data(base_url, headers, keywords, yesterday_str):
#     """Fetch news data from the News API"""
#     params = {
#         "q": keywords,
#         "from": yesterday_str,  # Set the 'from' parameter to yesterday's date
#         "to": yesterday_str,    # Set the 'to' parameter to yesterday's date
#         "sortBy": "popularity"  # Sort by popularity
#         # Add other parameters as needed
#     }

#     try:
#         response = requests.get(f"{base_url}everything?", headers=headers, params=params)
#         response.raise_for_status()
#         return response.json()
#     except Exception as e:
#         print(f"Error fetching news data: {e}")
#         sys.exit(1)


# def transform_basic(news_data):
#     """Some basic transformation of news data."""
#     # Assuming news_data is a dictionary containing relevant data

#     # Initialize an empty list to store the transformed data
#     data_list = []

#     for article in news_data.get("articles", []):
#         # Create a unique ID based on title and published_at
#         id_str = article.get("title", "") + str(article.get("publishedAt", ""))
#         article_id = hashlib.md5(id_str.encode()).hexdigest()

#         # Extract the article's title, description, and published date
#         author = article.get("author", "")
#         title = article.get("title", "")
#         description = article.get("description", "")
#         published_at = article.get("publishedAt", "")

#         # Extract additional fields
#         url = article.get("url", "")
#         urlToImage = article.get("urlToImage", "")
#         source = article.get("source", {})
#         source_id = source.get("id", "")
#         source_name = source.get("name", "")
#         content = article.get("content", "")

#         # Extract additional fields
#         category = article.get("category", "")
#         language = article.get("language", "")
#         country = article.get("country", "")
#         popularity = article.get("popularity", 0)  # Added popularity field

#         # Calculate sentiment scores using VADER
#         sentiment_scores = analyzer.polarity_scores(description)

#         # Determine the sentiment category with the highest score
#         max_sentiment = max(sentiment_scores, key=sentiment_scores.get)

#         data_dict = {
#             "article_id": article_id,
#             "author": author,
#             "title": title,
#             "published_at": published_at,
#             "description": description,
#             "compound_sentiment": sentiment_scores['compound'],
#             "positive_sentiment": sentiment_scores['pos'],
#             "negative_sentiment": sentiment_scores['neg'],
#             "neutral_sentiment": sentiment_scores['neu'],
#             "sentiment_category": max_sentiment,
#             "url": url,
#             "id": source_id,  # Add "id" field
#             "name": source_name,  # Add "name" field
#             "urlToImage": urlToImage,  # Add "urlToImage" field
#             "content": content,  # Add "content" field
#             "category": category,
#             "language": language,
#             "country": country,
#             "popularity": popularity  # Add popularity field
#         }
#         data_list.append(data_dict)

#     transformed_data = pd.DataFrame(data_list)

#     # Convert published_at to datetime
#     transformed_data["published_at"] = pd.to_datetime(
#         transformed_data["published_at"]
#     )

#     return transformed_data

# if __name__ == "__main__":
#     main()


















































import configparser
import datetime
import pandas as pd
import pathlib
import sys
import numpy as np
import requests
import hashlib
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

nltk.download('vader_lexicon')

# Initialize the sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

# Read Configuration File
parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
config_file = "configuration.conf"
parser.read(f"{script_path}/{config_file}")

# Use command line argument as output file
# name and also store as column value
try:
    output_name = sys.argv[1]
except Exception as e:
    print(f"Error with file input. Error {e}")
    sys.exit(1)
date_dag_run = datetime.datetime.strptime(output_name, "%Y%m%d")

def main():
    """Extract News data and load to CSV"""
    api_key = parser.get("news_api_config", "api_key")
    base_url, headers = api_connect(api_key)
    
    # Define a list of keywords and sources
    keywords = ["Tesla", "Apple", "Microsoft"]
    sources = ["bbc-news", "bloomberg", "financial-post", "google-news-uk", "abc-news"]
    
    # Initialize an empty list to store data from all sources
    all_data = []

    # Loop through each keyword and source and fetch data separately
    for keyword in keywords:
        for source in sources:
            news_data = fetch_news_data(base_url, headers, keyword, source)  # Use both keyword and source
            transformed_data = transform_basic(news_data, keyword)  # Pass the keyword
            all_data.append(transformed_data)

    # Concatenate data from all sources and keywords into a single DataFrame
    final_data = pd.concat(all_data, ignore_index=True)

    # Save the combined data to a CSV file
    output_file = f"/tmp/{output_name}.csv"
    final_data.to_csv(output_file, index=False)

    print(f"Data from all sources saved to {output_file}")


def api_connect(api_key):
    """Connect to the News API"""
    base_url = "https://newsapi.org/v2/"
    headers = {"Authorization": f"Bearer {api_key}"}
    return base_url, headers


def fetch_news_data(base_url, headers, keyword, source):
    """Fetch news data from the News API for a specific keyword and source"""
    params = {
        "q": keyword,
        "sources": source,
        # Add other parameters as needed
    }

    try:
        response = requests.get(f"{base_url}everything?", headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching news data: {e}")
        sys.exit(1)


def transform_basic(news_data, keyword):
    """Some basic transformation of news data."""
    # Initialize an empty list to store the transformed data
    data_list = []

    for article in news_data.get("articles", []):
        # Create a unique ID based on title and published_at
        id_str = article.get("title", "") + str(article.get("publishedAt", ""))
        article_id = hashlib.md5(id_str.encode()).hexdigest()

        # Extract the article's title, description, and published date
        author = article.get("author", "")
        title = article.get("title", "")
        description = article.get("description", "")
        published_at = article.get("publishedAt", "")

        # Check if "published_at" field exists before accessing it
        if published_at:
            published_at = pd.to_datetime(published_at)

        # Extract additional fields
        url = article.get("url", "")
        urlToImage = article.get("urlToImage", "")
        source = article.get("source", {})
        source_id = source.get("id", "")
        source_name = source.get("name", "")
        content = article.get("content", "")

        # Extract additional fields
        category = article.get("category", "")
        language = article.get("language", "")
        country = article.get("country", "")
                # Check if "description" field exists and is not empty before analyzing sentiment
        if description and description.strip():
            sentiment_scores = analyzer.polarity_scores(description)

            # Determine the sentiment category with the highest score
            max_sentiment = max(sentiment_scores, key=sentiment_scores.get)
        else:
            # Handle the case where description is None or empty
            sentiment_scores = {
                'compound': None,
                'pos': None,
                'neg': None,
                'neu': None
            }
            max_sentiment = None

        # Create a single row for each article with the corresponding keyword
        data_dict = {
            "article_id": article_id,
            "author": author,
            "title": title,
            "published_at": published_at,
            "description": description,
            "compound_sentiment": sentiment_scores['compound'],
            "positive_sentiment": sentiment_scores['pos'],
            "negative_sentiment": sentiment_scores['neg'],
            "neutral_sentiment": sentiment_scores['neu'],
            "sentiment_category": max_sentiment,
            "url": url,
            "id": source_id,
            "name": source_name,
            "urlToImage": urlToImage,
            "content": content,
            "category": category,
            "language": language,
            "country": country,
            "Keywords": keyword,
            "NumKeywordsInTitle": 1 if keyword.lower() in title.lower() else 0,
            "NumKeywordsInContent": 1 if keyword.lower() in content.lower() else 0
        }
        data_list.append(data_dict)

    transformed_data = pd.DataFrame(data_list)

    return transformed_data

if __name__ == "__main__":
    main()

















































# import configparser
# import datetime
# import pandas as pd
# import pathlib
# import sys
# import numpy as np
# import requests
# from validation import validate_input

# import nltk

# nltk.download('vader_lexicon')

# from nltk.sentiment.vader import SentimentIntensityAnalyzer

# # Initialize the sentiment analyzer
# analyzer = SentimentIntensityAnalyzer()

# # Read Configuration File
# parser = configparser.ConfigParser()
# script_path = pathlib.Path(__file__).parent.resolve()
# config_file = "configuration.conf"
# parser.read(f"{script_path}/{config_file}")

# # Configuration Variables
# # SECRET = parser.get("news_config", "secret")
# # CLIENT_ID = parser.get("news_config", "client_id")

# # Use command line argument as output file
# # name and also store as column value
# try:
#     output_name = sys.argv[1]
# except Exception as e:
#     print(f"Error with file input. Error {e}")
#     sys.exit(1)
# date_dag_run = datetime.datetime.strptime(output_name, "%Y%m%d")


# def main():
#     """Extract News data and load to CSV"""
#     api_key = parser.get("news_api_config", "api_key")
#     base_url, headers = api_connect(api_key)
#     keywords = "Tesla"                 #["Tesla", "Apple", "Microsoft"]
#     sources = "bbc-news"                  #["bbc-news", "bloomberg", "financial-post", "google-news-uk", "abc-news"]
#     news_data = fetch_news_data(base_url, headers, keywords, sources)
#     transformed_data = transform_basic(news_data)
#     load_to_csv(transformed_data)



# def api_connect(api_key):
#     """Connect to the News API"""
#     base_url = "https://newsapi.org/v2/"
#     headers = {"Authorization": f"Bearer {api_key}"}
#     return base_url, headers


# def fetch_news_data(base_url, headers, keywords, sources):
#     """Fetch news data from the News API"""
#     params = {
#         "q": keywords,
#         "sources": sources,
#         # Add other parameters as needed
#     }

#     try:
#         response = requests.get(f"{base_url}everything?", headers=headers, params=params)
#         response.raise_for_status()
#         return response.json()
#     except Exception as e:
#         print(f"Error fetching news data: {e}")
#         sys.exit(1)

# import hashlib


# def transform_basic(news_data):
#     """Some basic transformation of news data."""
#     # Assuming news_data is a dictionary containing relevant data

#     # Initialize an empty list to store the transformed data
#     data_list = []

#     for article in news_data.get("articles", []):
#         # Create a unique ID based on title and published_at
#         id_str = article.get("title", "") + str(article.get("publishedAt", ""))
#         article_id = hashlib.md5(id_str.encode()).hexdigest()

#         # Extract the article's title, description, and published date
#         author = article.get("author", "")
#         title = article.get("title", "")
#         description = article.get("description", "")
#         published_at = article.get("publishedAt", "")

#         # Extract additional fields
#         url = article.get("url", "")
#         urlToImage = article.get("urlToImage", "")
#         source = article.get("source", {})
#         source_id = source.get("id", "")
#         source_name = source.get("name", "")
#         content = article.get("content", "")

#         # Extract additional fields
#         category = article.get("category", "")
#         language = article.get("language", "")
#         country = article.get("country", "")

#         # Calculate sentiment scores using VADER
#         sentiment_scores = analyzer.polarity_scores(description)

#         # Determine the sentiment category with the highest score
#         max_sentiment = max(sentiment_scores, key=sentiment_scores.get)

#         data_dict = {
#             "article_id": article_id,
#             "author": author,
#             "title": title,
#             "published_at": published_at,
#             "description": description,
#             "compound_sentiment": sentiment_scores['compound'],
#             "positive_sentiment": sentiment_scores['pos'],
#             "negative_sentiment": sentiment_scores['neg'],
#             "neutral_sentiment": sentiment_scores['neu'],
#             "sentiment_category": max_sentiment,
#             "url": url,
#             "id": source_id,  # Add "id" field
#             "name": source_name,  # Add "name" field
#             "urlToImage": urlToImage,  # Add "urlToImage" field
#             "content": content,  # Add "content" field
#             "category": category,
#             "language": language,
#             "country": country,
#         }
#         data_list.append(data_dict)

#     transformed_data = pd.DataFrame(data_list)

#     # Convert published_at to datetime
#     transformed_data["published_at"] = pd.to_datetime(
#         transformed_data["published_at"]
#     )

#     return transformed_data




# def load_to_csv(transformed_data):
#     """Save transformed data to CSV file in /tmp folder"""
#     transformed_data.to_csv(f"/tmp/{output_name}.csv", index=False)


# if __name__ == "__main__":
#     main()