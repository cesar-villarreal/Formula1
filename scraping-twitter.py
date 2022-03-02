#!/usr/bin/python3
from tweepy import API, Client
from sqlite3 import connect
from pandas import read_sql_query
from datetime import datetime
from time import sleep
import schedule

api = Client(consumer_key="",
             consumer_secret="",
             access_token="",
             access_token_secret="",
             bearer_token="")

conn = connect('./tweeter_F1.db')
c = conn.cursor()



def get_data():
    drivers_ids = [69008563,
                   78502161,
                   556260847,
                   213969309,
                   1143472657,
                   516464106,
                   214413743,
                   262230432,
                   353786894,]

    users = api.get_users(ids= drivers_ids, user_fields=['public_metrics'])
    print('Getting data...', datetime.now())

    for i, user in enumerate(users.data):
        driver = int(drivers_ids[i])
        created_at = str(datetime.utcnow())
        followers_count = int(user.public_metrics['followers_count'])
        following_count = int(user.public_metrics['following_count'])
        tweet_count = int(user.public_metrics['following_count'])
        listed_count = int(user.public_metrics['listed_count'])

        sql_insert = """INSERT INTO drivers_public_metrics (drivers_ids, created_at, followers_count, following_count, tweet_count, listed_count)
                     VALUES (?, ?, ?, ?, ?, ?)"""
        c.execute(sql_insert, [driver, created_at, followers_count, following_count, tweet_count, listed_count])
    print('Public metrics inserted.')


    for driver_id in drivers_ids:
        mentions = api.get_users_mentions(id=driver_id, max_results=100, tweet_fields=['author_id', 'created_at', 'geo', 'lang'])

        for mention in mentions.data:
            mention_driver_id = int(driver_id)
            mention_created_at = str(mention.created_at)
            mention_author_id = int(mention.author_id)
            mention_geo = str(mention.geo)
            mention_lang = str(mention.lang)
            mention_id = int(mention.id)

            sql_insert = """INSERT INTO tweets_data (tweets, created_at, author_id, geo, lang, mention_id, drivers_ids)
                            VALUES (?, ?, ?, ?, ?, ?, ?)"""
            c.execute(sql_insert, [str(mention), mention_created_at, mention_author_id, mention_geo, mention_lang, mention_id, mention_driver_id])
        print(driver_id, len(mentions.data))
    print('Tweets inserted.')

    conn.commit()
    print('Commit done.')



schedule.every().hour.at("00:00").do(get_data)

while True:
    schedule.run_pending()
    sleep(60)



################################################################################


#sql_create_table = """ CREATE TABLE IF NOT EXISTS tweets_checo (
                                        #id integer PRIMARY KEY AUTOINCREMENT,
                                        #tweets text NOT NULL,
                                        #created_at text,
                                        #author_id integer,
                                        #geo text,
                                        #lang text,
                                        #mention_id integer);"""
#c.execute(sql_create_table)
#conn.commit()


#sql_create_table = """ CREATE TABLE IF NOT EXISTS drivers_public_metrics (
                                        #id integer PRIMARY KEY AUTOINCREMENT,
                                        #drivers_ids integer NOT NULL,
                                        #created_at text,
                                        #followers_count integer,
                                        #following_count integer,
                                        #tweet_count integer,
                                        #listed_count integer); """


#sql_create_table = """ALTER TABLE tweets_checo RENAME TO tweets_data;"""
#sql_create_table = """ALTER TABLE tweets ADD COLUMN drivers_ids integer ;"""
#c.execute(sql_create_table)
#conn.commit()


#con = connect('tweeter_F1.db')
#tables = read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", con)
#print(tables)



#tweets_df = read_sql_query("SELECT * from tweets_data", conn)
#drivers_public_metrics_df = read_sql_query("SELECT * from drivers_public_metrics", conn)
#tweets_df.to_excel('tweets_data.xlsx')
#drivers_public_metrics_df.to_excel('drivers_public_metrics.xlsx')



################################################################################


#drivers_names = ['SChecoPerez',
                 #'Max33Verstappen',
                 #'LewisHamilton',
                 #'ValtteriBottas',
                 #'LandoNorris',
                 #'danielricciardo',
                 #'Charles_Leclerc',
                 #'Carlossainz55',]
#drivers_ids = api.get_users(usernames = drivers_names)
#for driver_id in drivers_ids:
    #print(driver_id)

#https://dev.to/twitterdev/a-comprehensive-guide-for-using-the-twitter-api-v2-using-tweepy-in-python-15d9
#https://docs.tweepy.org/en/stable/client.html#id13


#nohup python3 -u scraping-twitter.py &
