import tweepy
import psycopg2
import datetime
import time

class TwitterCrawler:
  def api(self):
    consumer_key = 'uSMJlPGXkOohEvNymIdeQWwWv'
    consumer_secret = 'TWJer0TZt84jaZXEyu7XAAB2hVGNk5KU9KbWxMhSbr5GGR8CmA'
    access_token = '16283056-7VHSnQOZFoe2ZGWP2ERymykYlYd1djCkYnyR3Cf7N'
    access_secret = 'xSZtKyeWKhil2nmMbycpNdaHFfh0IueATpKObS0MBqM0D'

    authentication = tweepy.OAuthHandler(consumer_key, consumer_secret)
    authentication.set_access_token(access_token, access_secret)
    return tweepy.API(authentication, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

  def fetch_user_tweets(self, user_id):
    return self.api().user_timeline(user_id)

  def fetch_containing_tag(self, hashtag):
    tweets_list = []

    tweetsPerQry = 100
    maxTweets = 100
    # hashtag = "#codete"
    maxId = -1
    tweetCount = 0
    while tweetCount < maxTweets:
      if(maxId <= 0):
        newTweets = self.api().search(q=hashtag, count=tweetsPerQry, result_type="recent", tweet_mode="extended")
      else:
        newTweets = self.api().search(q=hashtag, count=tweetsPerQry, max_id=str(maxId - 1), result_type="recent", tweet_mode="extended")

      if not newTweets:
        print("Tweet Habis")
        break

      for tweet in newTweets:
        # print(tweet.full_text.encode('utf-8'))
        # print(tweet.full_text)
        tweets_list.append(tweet)

      tweetCount += len(newTweets)
      maxId = newTweets[-1].id

    return tweets_list

while(True):
  print("Iteration started")
  time.sleep(60)

  conn = psycopg2.connect(
      host="localhost",
      database="python_crawler_development",
      user="",
      password="")

  conn.autocommit = True

  # Tags
  cursor = conn.cursor()
  cursor.execute("SELECT * from tags")

  for row in cursor:
      print('cursor: ', row[1])
      tag_id = row[0]
      tag = row[1]
      currDate = datetime.datetime.now()
      another_cursor = conn.cursor()

      tweets = TwitterCrawler().fetch_containing_tag(tag)

      for tweet in tweets:
        print("INSERT INTO tag_messages(tag_id, text, created_at, updated_at) VALUES(%s, %s)", (tag_id, tweet.full_text, currDate, currDate))
        # cursor.execute("INSERT INTO tag_messages(tag_id, text) VALUES('Audi', 52642)")
        another_cursor.execute("INSERT INTO tag_messages(tag_id, text, created_at, updated_at) VALUES(%s, %s, %s, %s) ON CONFLICT ON CONSTRAINT tag_messages_text_key DO NOTHING", (tag_id, tweet.full_text, currDate, currDate))

      another_cursor.close()
  cursor.close()

  # Users
  cursor = conn.cursor()
  cursor.execute("SELECT * from users")

  for row in cursor:
      print('cursor: ', row[1])
      user_id = row[0]
      user = row[1]
      currDate = datetime.datetime.now()
      another_cursor = conn.cursor()

      tweets = TwitterCrawler().fetch_user_tweets(user)

      for tweet in tweets:
        print("INSERT INTO user_messages(user_id, text, created_at, updated_at) VALUES(%s, %s)", (user_id, tweet.text, currDate, currDate))
        # cursor.execute("INSERT INTO tag_messages(tag_id, text) VALUES('Audi', 52642)")
        another_cursor.execute("INSERT INTO user_messages(user_id, text, created_at, updated_at) VALUES(%s, %s, %s, %s) ON CONFLICT ON CONSTRAINT user_messages_text_key DO NOTHING", (user_id, tweet.text, currDate, currDate))

      another_cursor.close()
  cursor.close()

  conn.commit()
  conn.close()
