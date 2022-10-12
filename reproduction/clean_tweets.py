import pandas as pd
import numpy as np
import codecs
import re
import unicodedata
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

tweets = pd.read_json(codecs.open('tweets.json', 'r', 'utf-8'), dtype={"topic_id": np.int32}, orient="records")

# Fix unicode characters
tweets["text"] = tweets["text"].map(lambda x: unicodedata.normalize("NFKD", x))
tweets["text"] = tweets["text"].map(lambda x: unicodedata.normalize("NFC", x))

# Strip emojis from tweets
emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
    "]+", flags=re.UNICODE)

emojiless_tweets = tweets
emojiless_tweets["text"] = emojiless_tweets["text"].map(lambda x: re.sub(emoji_pattern, r'', x))

total_tweets = tweets["tweet_id"].count()

# Remove redundant whitespace characters
emojiless_tweets["text"] = emojiless_tweets["text"].map(lambda x: " ".join(x.split()))

# Remove links of attachments
attachment_pattern = re.compile("https:\\/\\/t\.co\\/.*")
emojiless_tweets["text"] = emojiless_tweets["text"].map(lambda x: re.sub(attachment_pattern, r'', x))

# Remove redundant whitespace characters
emojiless_tweets["text"] = emojiless_tweets["text"].map(lambda x: " ".join(x.split()))

# Remove hashtags at the end of tweets
print("remove hashtag")
hashtag_pattern = re.compile("(#.*)+$")
emojiless_tweets["text"] = emojiless_tweets["text"].map(lambda x: re.sub(hashtag_pattern, r'', x))

# Remove mentions at the beginning of the tweet
mention_pattern = re.compile("^(@.*?\ )*")
emojiless_tweets["text"] = emojiless_tweets["text"].map(lambda x: re.sub(mention_pattern, r'', x))

# Remove '#' and '@' characters in text
hashtag_pattern = re.compile("#|@")
emojiless_tweets["text"] = emojiless_tweets["text"].map(lambda x: re.sub(hashtag_pattern, r'', x))
emojiless_tweets["text"] = emojiless_tweets["text"].map(lambda x: " ".join(x.split()))

# Remove punctuation
punctuation_pattern = re.compile(r'[^\w\s\']+')
emojiless_tweets["text"] = emojiless_tweets["text"].map(lambda x: re.sub(punctuation_pattern, ' ', x))

# Remove redundant whitespace characters
emojiless_tweets["text"] = emojiless_tweets["text"].map(lambda x: " ".join(x.split()))

# Remove exact duplicate tweets
emojiless_tweets.drop_duplicates(subset='text', keep="first", inplace=True)
dedup_tweets = emojiless_tweets

print("# of tweets in total: ", total_tweets)
print("# of tweets without duplicates: ", dedup_tweets["tweet_id"].count())

# Remove tweets with less than 7 words
minimum_words = 7
dedup_tweets = dedup_tweets[dedup_tweets['text'].apply(lambda x: len(x.split()) >= minimum_words)]

# Remove tweets with 0.8 or greater similarity to other tweet
topic_groups = tweets.groupby('topic')
for name, group in topic_groups:
    topic = name
    event_tweets = dedup_tweets.copy()
    event_tweets = event_tweets[event_tweets["topic_id"] == topic]
    tweet_ids = event_tweets["tweet_id"].tolist()

    count_vectorizer = CountVectorizer()
    sparse_matrix = count_vectorizer.fit_transform(event_tweets['text'].tolist())

    # Convert Sparse Matrix to Pandas Dataframe if you want to see the word frequencies.
    doc_term_matrix = sparse_matrix.todense()
    df = pd.DataFrame(doc_term_matrix, 
                    columns=count_vectorizer.get_feature_names_out(),
                    index = tweet_ids)

    # Compute Cosine Similarity
    cosine_matrix = cosine_similarity(df, df)
    ids_to_remove = []

    # Delete similar tweets
    for i in range(0, len(cosine_matrix[0])):
        for j in range (i+1, len(cosine_matrix[0])):
            if cosine_matrix[i][j] >= 0.8:
                ids_to_remove.append(tweet_ids[i])
                dedup_tweets = dedup_tweets[dedup_tweets["tweet_id"] != tweet_ids[i]]
                break

print("# of tweets after removing similar ones: ", dedup_tweets["tweet_id"].count())

# Write clean tweets to file
with open('tweets_clean.json', 'w', encoding='utf-8') as file:
    dedup_tweets.to_json(file, force_ascii=False, orient="records", lines=False)

with open('tweets_clean.jsonl', 'w', encoding='utf-8') as file:
    dedup_tweets.to_json(file, force_ascii=False, orient="records", lines=True)