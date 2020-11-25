#!/usr/bin/env python
# coding: utf-8

# In[276]:


import pandas as pd
import numpy as np
import os
import requests
import tweepy
import json
import matplotlib.pyplot as plt
import datetime as dt
import pytz


# ## Gather

# In[2]:


#Reading the Twitter archive csv
tweets = pd.read_csv('files/twitter-archive-enhanced.csv')


# In[3]:


#Requesting image-predictions.tsv

url = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'

r = requests.get(url)

if not os.path.exists('files'):
    os.makedirs('files')

with open(os.path.join('files', 'image_predictions.tsv'), mode='wb') as file:
        file.write(r.content)

image_pr = pd.read_csv('files/image_predictions.tsv', sep='\t')


# In[4]:


#Requesting access to Twitter API

consumer_key = 'XXX'
consumer_secret = 'XXX'
access_token = 'XXX'
access_secret = 'XXX'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, parser=tweepy.parsers.JSONParser())


# In[24]:


#Adding the JSON details of each tweet to a txt file

tweet_ids = tweets.tweet_id

open('files/tweet_json.txt', 'w').close()

i = 0

for tweet_id in tweet_ids:
    try:
        print(i)
        with open('files/tweet_json.txt', mode='a') as file:
            json.dump(api.get_status(tweet_id, tweet_mode = 'extended'), file)
            file.write('\n')
    
    except Exception as e:
        print(str(i) + "_" + str(tweet_id) + ": " + str(e))   #prints out error for deleted tweets
    i+=1


# ## Assess

# In[6]:


tweets.describe()


# In[7]:


tweets.info()


# In[38]:


tweets.sample(5)


# In[9]:


tweets[tweets.tweet_id.duplicated()]


# In[10]:


tweets[tweets.text.duplicated()]


# In[14]:


tweets.rating_numerator.sort_values()


# In[18]:


tweets[tweets.rating_numerator == 0].text


# In[19]:


image_pr.head()


# In[20]:


image_pr.info()


# In[21]:


image_pr.describe()


# In[31]:


image_pr[((image_pr.p1_dog == False) & (image_pr.p2_dog == False) & (image_pr.p3_dog == False))].count()


# ### Quality Issues
# ##### `tweets` table
# - Some dogs have their names included in the tweets but are listed as None in the dataset
# - Some dogs have their names listed as 'a', 'all', 'quite', 'the', etc. instead of an actual name possibly because they appear after the phrase 'This is'
# - A rating of 9.75/10 is read as 75/10
# - The phrases '9/11', '7/11' and '50/50' were mistaken as ratings
# - A rating of 960/00 was incorrectly assumed as the right rating
# - Tweets that have a newline are broken off after the newline, however this does not affect the rating
# - IDs are stored as ints or floats when they should be strings
# - timestamp stored as object
# - rating_numerator and rating_denominator stored as ints not floats
# - 59 expanded_urls are missing
# - Some dogs are included in two or more dog types (i.e doggo and pupper)
# - Tweets should not include retweets
# - Some tweets were deleted from Twitter
# 
# ##### `image_pr` table
# - tweet_id should be string
# 
# ### Tidiness Issues
# ##### `tweets` table
# - Type of dog is separated into four columns (doggo, floofer, pupper, puppo)
# - Columns for number of favorites and retweets should be added from the JSON file
# - Column for dog breed prediction should be added from `image_pr`
# 
# ##### `image_pr` table
# - Should add a column signifying whether or not this is a dog

# ## Clean

# In[207]:


#create copy of dataset
tweets_c = tweets.copy()


# #### Define
# Change data types of rating_numerator and rating_denominator from `tweets` into floats
# 
# #### Code

# In[208]:


tweets_c.rating_numerator = tweets_c.rating_numerator.astype('float64')
tweets_c.rating_denominator = tweets_c.rating_denominator.astype('float64')


# #### Define
# Change timestamp in `tweets` to datetime
# 
# #### Code

# In[278]:


tweets_c.timestamp = tweets_c.timestamp.astype('datetime64[ns, US/Eastern]')


# #### Define
# - Fix ratings that were entered as ints instead of floats (i.e 9.75)
# 
# #### Code

# In[210]:


#Find the tweets with a .5 or .75 rating
tweet_ids = tweets_c.tweet_id

for i in range(len(tweets) - 1):
    if(tweets_c.iloc[i].rating_numerator == 5) or (tweets_c.iloc[i].rating_numerator == 75):
        print(i)
        print(tweets_c.iloc[i].tweet_id)
        print(tweets_c.iloc[i].text)


# In[211]:


#Since there are only a few, they can be changed manually

tweets_c.at[tweets_c.text[tweets_c.tweet_id == 883482846933004288].index[0],'rating_numerator'] = 13.5
tweets_c.at[tweets_c.text[tweets_c.tweet_id == 786709082849828864].index[0],'rating_numerator'] = 9.75
tweets_c.at[tweets_c.text[tweets_c.tweet_id == 681340665377193984].index[0],'rating_numerator'] = 9.5

#The dog mistakenly rated as 960/00 was removed as the tweet was a reply


# In[212]:


tweets_c.rating_numerator


# #### Define
# Remove Tweets that were deleted from Twitter by using the Tweets found in the JSON file
# 
# #### Code

# In[213]:


#To access each dictionary, a list of dictionaries was created from the JSON file

jsonTweets = []
for line in open('files/tweet_json.txt', 'r'):
    jsonTweets.append(json.loads(line))


# In[214]:


#Remove tweets from the tweets dataset that are not foun in the JSON file (deleted from Twitter)

listTweets = []
for i in range(len(jsonTweets)):
    listTweets.append(jsonTweets[i].get('id'))

tweets_c = tweets_c[tweets_c.tweet_id.isin(listTweets)]


# #### Define
# Remove columns related to retweeting and replying since they are of no use
# 
# #### Code

# In[215]:


tweets_c = tweets_c.drop(['retweeted_status_id', 'retweeted_status_user_id', 'retweeted_status_timestamp', 'in_reply_to_status_id', 'in_reply_to_user_id', 'expanded_urls'], axis=1)


# In[216]:


len(tweets_c)


# #### Define
# Add favorite and retweet count columns to `tweets` table from JSON file
# 
# #### Code

# In[217]:


favCount = []
retCount = []

for i in range(len(tweets)):
    try:
        favCount.append(jsonTweets[i].get('favorite_count'))
        retCount.append(jsonTweets[i].get('retweet_count'))
    except Exception as e:
        pass
print(len(favCount))


# In[218]:


tweets_c['favorite_count'] = favCount
tweets_c['retweet_count'] = retCount


# In[219]:


tweets_c.head()


# #### Define
# Remove retweeted tweets and replies because we only need original ratings
# 
# #### Code

# In[220]:


nonRetweets = []

for i in range(len(jsonTweets)):
    if((jsonTweets[i].get('full_text')[0:2] != 'RT') and (jsonTweets[i].get('full_text')[0:1] != '@')):
        nonRetweets.append(jsonTweets[i].get('id'))
        
tweets_c = tweets_c[tweets_c.tweet_id.isin(nonRetweets)]


# #### Define
# Remove fake dog names from `tweets` table
# 
# #### Code

# In[221]:


#Fake dog names like 'a', 'the' etc. all start will lowercase characters

for i in range(len(tweets)):
    try:
        if(tweets_c.name[i][0].islower()):
            tweets_c.at[i,'name'] = 'None'
              
    except Exception as e:
        pass


# In[222]:


tweets_c.name.value_counts()
#We start at 759 Nones


# #### Define
# Change dog names from 'None' to their real names if possible. Their real names are often included after the words 'named' or 'called'
# 
# #### Code

# In[223]:


#This code looks for the next character after the word 'named' until the period that ends the sentence

for i in range(len(tweets)):
    try:
        if(tweets_c.name[i] == 'None'):
            if(tweets_c.text[i].find('named') > 0):
                firstLet = tweets_c.text[i][(tweets_c.text[i].find('named') + 6)]
                print(tweets_c.text[i][tweets_c.text[i].find(firstLet):tweets_c.text[i].find('.', (tweets_c.text[i].find('named') + 6), len(tweets_c.text[i]))])
              
    except Exception as e:
        pass
    


# In[224]:


for i in range(len(tweets)):
    try:
        if(tweets_c.name[i] == 'None'):
            if(tweets_c.text[i].find('named') > 0):
                firstLet = tweets_c.text[i][(tweets_c.text[i].find('named') + 6)]
                dogName = tweets_c.text[i][tweets_c.text[i].find(firstLet):tweets_c.text[i].find('.', (tweets_c.text[i].find('named') + 6), len(tweets_c.text[i]))]
                tweets_c.at[i,'name'] = dogName
              
    except Exception as e:
        pass


# In[225]:


tweets_c.name.value_counts()
#None counts decreased to 736


# #### Define
# Combine the four dog type columns (doggo, floofer, pupper and puppo) into one dog_type column
# 
# #### Code

# In[226]:


#create column dog_type
tweets_c['dog_type'] = ""

tweets_c.head()


# In[227]:


for i in range(len(tweets) - 1):
    try:
        if(tweets_c.doggo[i] == 'doggo'):
            tweets_c.at[i, 'dog_type'] = 'Doggo'
        elif(tweets_c.floofer[i] == 'floofer'):
            tweets_c.at[i, 'dog_type'] = 'Floofer'
        elif(tweets_c.pupper[i] == 'pupper'):
            tweets_c.at[i, 'dog_type'] = 'Pupper'
        elif(tweets_c.puppo[i] == 'puppo'):
            tweets_c.at[i, 'dog_type'] = 'Puppo'
        else:
            tweets_c.at[i, 'dog_type'] = 'None'
    
    except Exception as e:
        pass


# In[242]:


tweets_c.sample(10)


# In[229]:


tweets_c = tweets_c.drop(['doggo', 'floofer', 'pupper', 'puppo'], axis=1)


# In[230]:


tweets_c.head()


# #### Define
# Add the three predictions from `image_pr` to the `tweets` table
# 
# #### Code

# In[231]:


image_pr.head()


# In[232]:


tweets_c = tweets_c.merge(image_pr, on='tweet_id')


# #### Define
# Select the most confident prediction to set as dog_breed
# 
# #### Code

# In[233]:


tweets_c['dog_breed'] = ""


# In[234]:


for i in range(len(tweets) - 1):
    try:
        if(tweets_c.p1_dog[i]):
            tweets_c.at[i, 'dog_breed'] = tweets_c.p1[i]
        elif(tweets_c.p2_dog[i]):
            tweets_c.at[i, 'dog_breed'] = tweets_c.p2[i]
        elif(tweets_c.p3_dog[i]):
            tweets_c.at[i, 'dog_breed'] = tweets_c.p3[i]
        else:
            tweets_c.at[i, 'dog_breed'] = 'Not a dog'
    
    except Exception as e:
        pass


# In[236]:


tweets_c.sample(10)


# #### Define
# Remove unnecessary columns
# 
# #### Code

# In[237]:


tweets_c = tweets_c.drop(['img_num', 'p1', 'p1_conf', 'p1_dog', 'p2', 'p2_conf', 'p2_dog', 'p3', 'p3_conf', 'p3_dog'], axis=1)


# In[238]:


tweets_c.dog_breed.value_counts()


# In[239]:


tweets_c.source.value_counts()


# In[240]:


tweets_c = tweets_c.drop(['source'], axis=1)


# In[244]:


tweets_c.head()


# In[243]:


tweets_c.to_csv('twitter_archive_master.csv', index=False)


# ## Analysis

# In[269]:


plt.figure(figsize=(20,10))
plt.plot_date(tweets_c.timestamp.dt.date, tweets_c.favorite_count)


# In[270]:


tweets_c.describe()


# In[273]:


tweets_c[tweets_c.favorite_count == max(tweets_c.favorite_count)]


# In[279]:


plt.figure(figsize=(20,10))
plt.plot_date(tweets_c.timestamp.dt.time, tweets_c.favorite_count)


# In[303]:


plt.figure(figsize=(10,10))
plt.pie(tweets_c.dog_breed.value_counts(), labels=tweets_c.dog_breed.value_counts().index)


# In[304]:


plt.figure(figsize=(10,10))
plt.pie(tweets_c.dog_type.value_counts(), labels=tweets_c.dog_type.value_counts().index)


# In[318]:


plt.figure(figsize=(20,10))
plt.plot(tweets_c[tweet_c.dog_type == 'Doggo'].timestamp.dt.date, tweets_c[tweet_c.dog_type == 'Doggo'].favorite_count)
plt.plot(tweets_c[tweet_c.dog_type == 'Puppo'].timestamp.dt.date, tweets_c[tweet_c.dog_type == 'Puppo'].favorite_count)
plt.plot(tweets_c[tweet_c.dog_type == 'Pupper'].timestamp.dt.date, tweets_c[tweet_c.dog_type == 'Pupper'].favorite_count)
plt.plot(tweets_c[tweet_c.dog_type == 'Floofer'].timestamp.dt.date, tweets_c[tweet_c.dog_type == 'Floofer'].favorite_count)
plt.legend(['Doggo', 'Puppo', 'Pupper', 'Floofer'])


# In[ ]:




