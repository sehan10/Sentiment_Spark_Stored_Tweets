import socket
import sys
import requests
import requests_oauthlib
import json
# Replace the values below with yours
## get the stream of tweets from the twitter #

def send_tweets_to_spark(tweets, tcp_connection):
    while True:
        print(tweets)
        tcp_connection.send(tweets + '\n')
# 01 define sockets configurations ##
TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
## 02 start binding and start listening at deine port and IP ##
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
## 03 accept the connection #
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
#resp = get_tweets()
file = open('pos.txt','r')
tweets = file.read()
send_tweets_to_spark(tweets,conn)
