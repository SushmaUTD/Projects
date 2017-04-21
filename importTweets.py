from __future__ import absolute_import, print_function
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler,API
from tweepy import Stream
import json
from kafka import SimpleProducer, KafkaClient

consumer_key="aongZXR6ZRqvmttYnMLKAuD3n"
consumer_secret="J56kwHWN4dd7HKfwuA2RQ1fnphslnXOrxpooCOPZQoJDpDyZuR"
access_token="1543366946-lEWKSWc1M7L6lI5txJ5XTEiaaq0trdaQbhKuC0d"
access_token_secret="FtbRA9vZvdxDWjWdmjzcOxWN0E3niCH6zehqLIyGxomf3"

class TweetListener(StreamListener):
    def __init__(self, api):
        self.api = api
        super(StreamListener, self).__init__()
        client = KafkaClient("localhost:9092")
        self.producer = SimpleProducer(client, async = True, batch_send_every_n = 1000, batch_send_every_t = 1)
    
    def on_status(self, status):
        msg = status.text.encode('utf-8')

        if status.coordinates != None:
            msg = msg+"::"+ status.coordiantes
        else:
            msg
        try:
            self.producer.send_messages(b'tweets', msg)
        except Exception as e:
            print(e)
            return False
        print("#####################################",status.text,status.coordinates, status.place)
        return True
        
    def on_error(self, status_code):
        print("Error received in kafka producer")
        return True 

    def on_timeout(self):
        return True
    


        
if __name__ == '__main__':
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = API(auth)
    l = TweetListener(api)
    stream = Stream(auth, l)
    stream.filter(track=['#trump'])
