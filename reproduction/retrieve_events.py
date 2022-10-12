import json
import spacy
import yake
import re
import pywikibot
import pandas as pd
from datetime import date
import json
import codecs 

# Convert string to month number in order to obtain the date of an event
MONTHS_PORTUGUESE = {
    "janeiro": 1,
    "fevereiro": 2,
    "março": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12
}

def spaCy_extraction(text):
    nlp = spacy.load("pt_core_news_sm")
    doc = nlp(text)
    return (" ").join([t.text for t in doc.ents])

def YAKE_extraction(text):
    language = "pt"
    max_ngram_size = 3
    deduplication_threshold = 0
    numOfKeywords = 3
    custom_kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size, dedupLim=deduplication_threshold, top=numOfKeywords, features=None)
    keywords = custom_kw_extractor.extract_keywords(text)
    keywords = (" ").join([word[0] for word in keywords])
    return keywords

# Wikipedia text has the notation [[A|B|...|C]] for synonyms
# We only want the last entity, since it's the one used in the actual text
# [[A|B|...|C]] -> C
def replace_brackets(matchobj):
    # Remove '[[' and ']]'
    str = matchobj.group(0)[2:-2]
    if "|" in str:
        entities = str.split("|")
        return entities[len(entities)-1]

    return str

# Remove apostrophes from italic words
# ''A'' -> A
def remove_italic(matchobj):
    return matchobj.group(0)[2:-2]

# Parse wikipedia page "2021 in Portugal"
# Extract events, dates and source articles
def get_events_from_page(text, extractor="yake"):

    global events_df

    # Retrieve only the "Events" section
    # Starts with "=== Eventos ===" and ends with "==="
    result = re.search("^(== Eventos ==)(?:\r?\n(?!\r?\n)*.*?)*== (.*) ==$", text, re.MULTILINE)
    text = result.group(0).split("\n")[1:-2] # Cut header and blank lines
    count = 0
    month = 0
    for line in text:
        
        # === NameOfMonth ===
        if line.startswith("==="):
            month = re.search("=== .*? ===", line).group(0)[4:-4]
            month = MONTHS_PORTUGUESE[month.lower()]
            continue

        if not line.startswith("*"):
            continue

        
        # Retrieve date of event - DD/MM/YYYY
        day = re.search("\|[0-9]+\]", line).group(0)[1:-1]
        event_date = date(2021, month, int(day))

        # Retrieve event summary
        event = re.search("\—.*", line).group(0)
        
        # Crop references
        event = re.sub("\<.*?\>.*?\</ref\>","",event)
        event = event[2:]

        # Fix [[ ]] and '' '' notations
        event = re.sub("\[\[.*?\]\]", replace_brackets, event)
        event = re.sub("\'\'.*?\'\'", remove_italic, event)
        
        # Retrieve urls of the source news article
        ref_urls = re.finditer("https*://.*?(?= )", line)
        ref_dates = re.findall("(?<!acesso)data=.*?\d\d\d\d", line)
        refs = []
        for index, ref_url in enumerate(ref_urls):
            ref_url = ref_url.group(0) if ref_url else ""

            # Retrieve publication date of news article
            if ref_dates and ref_dates[index]:
                ref_date = ref_dates[index][5:]
                ref_day = ref_date.split(" ")[0]
                ref_month = MONTHS_PORTUGUESE[re.search("\w\w\w\w+", ref_date).group(0)]
                ref_year = re.search("\d\d\d\d+", ref_date).group(0)
                ref_date = date(int(ref_year), int(ref_month), int(ref_day))
                refs.append({"url": ref_url, "date": str(ref_date)})

        # Use YAKE to get keywords to use as the Twitter search query
        topic = YAKE_extraction(event) if extractor == 'yake' else spaCy_extraction(event)
        
        # Add event to dataframe
        events_df = events_df.append({
            "topic": topic,
            "summary": event,
            "date": str(event_date),
            "refs": refs
        }, ignore_index=True)

        events.append({
            "id": count,
            "topic": topic,
            "summary": event,
            "date": str(event_date),
            "refs": refs,
        })

        count += 1
        

events = []
events_df = pd.DataFrame(columns=["topic","summary","date","refs"])

site = pywikibot.Site()
page_name = "2021 em Portugal"
page = pywikibot.Page(site, page_name)
text = page.get()

get_events_from_page(text)
events_df.to_csv("events.csv", index=True, index_label="id", sep="|")

with codecs.open("events.jsonl", "w", encoding="utf-8") as f:
    for event in events:
        f.write(json.dumps(event, ensure_ascii=False) + '\n')