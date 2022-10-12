# Tweet2Event-PT-Dataset

Tweet2Event-PT is a Portuguese dataset containing tweets related to events from the Wikipedia page "2021 in Portugal".

In the *dataset* directory, we make available the list of twitter IDs and their relevance (annotated by the BM25 function), as well as a CSV/JSONL file with the 12 events selected for the dataset, with manually curated keywords. We also make available a sample of the dataset (473 tweets) manually annotated with their relevance (0 for non-relevant, 1 for relevant).

If you want to reproduce our method for retrieving all the events and related tweets, you can follow steps in the sections below. With this method, the keywords will be extracted automatically using YAKE, so the retrieved tweets will be different than the ones in our dataset. Make sure to run the scripts while inside the *reproduction* directory.


## Extract events from Wikipedia:

 1. Register in Wikipedia and change and set your username in *userconfig.py*:
         
        usernames['wikipedia']['pt'] = 'your-username'
        usernames['wikinews']['pt'] = 'your-username'
          
 2.  To create a CSV with events from the Wikipedia page run:

         python retrieve-events.py

For more information regarding Pywikibot, the library used to obtain Wikipedia content, check their [manual](https://www.mediawiki.org/wiki/Manual:Pywikibot).


## Extract related tweets
          
 1.  Set your Twitter API credentials in *.env*

 2.  Retrieve tweets and create a CSV with:

         python retrieve-tweets.py
 
 3.  Clean the tweets with:

         python clean-tweets.py


# Contact

For more information about the dataset or any problems with it, contact mafalda.r.castro@inesctec.pt
