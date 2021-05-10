import json
import pandas as pd
import pyodbc
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from pandas.io import sql
from sqlalchemy import create_engine
from pandas.io.json import json_normalize
import urllib
import re
import pprint
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
#from sentiment_analysis_spanish import sentiment_analysis
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

#Instanciamos el objeto de procesamiento de lenguaje natural
sentiment = SentimentIntensityAnalyzer()




#Twitter APPI Credentials
cKey = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
cSecret = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
aToken = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
aTokenSecret = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# Get credentials DB


database = "XXXXXXXXXXXXXXX"
username = "XXXXXXXXXXXXXX"
password = "XXXXXXXXXXXXXXXX"
server = "XXXXXXXXXXXXXXXX"


params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 f"SERVER={server};"
                                 f"DATABASE={database};"
                                 f"UID={username};"
                                 f"PWD={password}")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))





#conexion = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
#cursor = conexion.cursor()
#engine = create_engine("mssql+pyodbc://DRIVER={SQL Server};SERVER="+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

#create_engine("mssql+pyodbc://MyDbUser:MyPassword@MySQLServer/TwitterDB?driver=SQL+Server+Native+Client+11.0")

#Define after how many twitts we do a insert in the data base.
bufferSize = 5

#Defina an array to store the tweets readed from the stream api
twittsBuffer = []

def deep_search(needles, haystack):
    found = {}
    if type(needles) != type([]):
        needles = [needles]

    if type(haystack) == type(dict()):
        for needle in needles:
            if needle in haystack.keys():
                found[needle] = haystack[needle]
            elif len(haystack.keys()) > 0:
                for key in haystack.keys():
                    result = deep_search(needle, haystack[key])
                    if result:
                        for k, v in result.items():
                            found[k] = v
    elif type(haystack) == type([]):
        for node in haystack:
            result = deep_search(needles, node)
            if result:
                for k, v in result.items():
                    found[k] = v
    return found




def cleanTxt(text):
    """
    Definimos una función que realice un preprocesamiento de los tweets recibidos
    """
    try:
        a = deep_search(["full_text"], text)
        print(a)
        text['full_text'] = a['full_text']
    except:
        text['full_text'] = text['text']
    text['text_with_stopwords'] = re.sub(r'@[A-Za-z0-9]+', '', text['full_text'])        
    text['text_with_stopwords'] = re.sub(r'#', '', text['text_with_stopwords'])
    text['text_with_stopwords'] = re.sub(r'RT[\s]+','',text['text_with_stopwords'])
    text['text_with_stopwords'] = re.sub(r'https?:\/\/\S+','', text['text_with_stopwords'])
    text['text_with_stopwords'] = re.sub(r':','',text['text_with_stopwords'])
    #print('Texto original: ',text['text'])
    text['text_with_stopwords'] = re.sub(r'[^a-zA-Zá-ú]',' ', text['text_with_stopwords'])
    text['text_with_stopwords'] = text['text_with_stopwords'].lower()
    
    text_split = text['text_with_stopwords'].split()
    ps = PorterStemmer()
    text_split = [ps.stem(word) for word in text_split if not word in set(stopwords.words('spanish'))] 
    
    #print('Texto limpiado: ',' '.join(text_split))
    text['text_with_stopwords'] = ' '.join(text_split)
    text['score_with_stopwords'] = sentiment.polarity_scores(text['text_with_stopwords'])['compound']
    print(text['score_with_stopwords'])
    text['text_without_stopwords'] = re.sub(r'@[A-Za-z0-9]+', '', text['full_text'])
    text['text_without_stopwords'] = re.sub(r'#', '', text['text_without_stopwords'])
    text['text_without_stopwords'] = re.sub(r'RT[\s]+','',text['text_without_stopwords'])
    text['text_without_stopwords'] = re.sub(r'https?:\/\/\S+','', text['text_without_stopwords'])
    text['text_without_stopwords'] = re.sub(r':','',text['text_without_stopwords'])
    text['text_without_stopwords'] = re.sub(r'[^a-zA-Zá-ú]',' ', text['text_without_stopwords'])
    text['text_without_stopwords'] = text['text_without_stopwords'].lower()
    text['score_without_stopwords'] = sentiment.polarity_scores(text['text_without_stopwords'])['compound']
    
    AddTwittToBuffer(text)
    return

def score(text):
    return sentiment.sentiment(text['text'])

#--------------------------------------------------------------------------------
def AddTwittToBuffer(twitt):
    """
    Definimos una función que recibe un twitt por parametro y lo alamcena en la variable twittsBuffer.
    Si el twittsBuffer alcanza el máximo definido en la variable buffersize, llamamos a la función AddTwittsToDB para insertar los tweets
    en la base de datos y vaciamos la variable twittsBuffer
    """
    
    global twittsBuffer
    twittsBuffer.append(twitt)
    
    if (len(twittsBuffer) == bufferSize):
        AddTwittsToDB(twittsBuffer)
        twittsBuffer = []
    print(twitt['coordinates'] if twitt['coordinates']!= None else 'no coordinates')
    return 


def AddTwittsToDB(twitts):

    """
    Esta función inserta el tweet almacenado en la variable twittsBuffer en SQl Server
    """
    tData = {'id': [], 
             'country': [], 
             'created_at': [], 
             'favorite_count': [],
             'followers_count': [], 
             'friends_count': [],
             'lang': [], 
             'latitude': [], 
             'longitude':[], 
             'retweet_count':[], 
             'screen_name':[], 
             'text':[],
             'text_with_stopwords':[],
             'text_without_stopwords':[],
             'score_with_stopwords':[],
             'score_without_stopwords':[]
             }
    
    for t in twitts:
        tData['id'].append(t['id'])
        tData['text'].append(t['full_text'])
        tData['text_with_stopwords'].append(t['text_with_stopwords'])
        tData['text_without_stopwords'].append(t['text_without_stopwords'])
        tData['screen_name'].append(t['user']['screen_name'])
        tData['created_at'].append(t['created_at'])
        tData['retweet_count'].append(t['retweet_count'])
        tData['favorite_count'].append(t['favorite_count'])
        tData['friends_count'].append(t['user']['friends_count'])
        tData['followers_count'].append(t['user']['followers_count'])
        tData['score_with_stopwords'].append(t['score_with_stopwords'])
        tData['score_without_stopwords'].append(t['score_without_stopwords'])
        tData['lang'].append(t['lang'])
        if t['place'] != None :
            tData['country'].append(t['place']['country'])
        else :
            tData['country'].append(None)
        
        if t['coordinates'] != None :
            tData['longitude'].append(t['coordinates']['coordinates'][0])
            tData['latitude'].append(t['coordinates']['coordinates'][1])
        else :
            tData['longitude'].append(None)
            tData['latitude'].append(None)
    tweets = pd.DataFrame(tData)
    #print('tData \n',tData['text'])
    #print('twit \n',twitts)
    #print('DataFrame \n',tweets['text'])
    #tweets.set_index('id', inplace=True)
    #print("Array: ",tData['text'])
    #tweets.head()
    
    
    
    tweets.to_sql("Tweets",con=engine,if_exists='append', index=False)
    return True

#--------------------------------------------------------------------------------
#Creamos una clase listener que procesa los tweets recibidos
#Si hay un error imprimimos el satus
#--------------------------------------------------------------------------------
class StdOutListener(StreamListener):
    def on_data(self, data):
        t= json.loads(data)
        print(type(t))
        cleanTxt(t)
        return True
    def on_error(self, status):
        print(status)
#--------------------------------------------------------------------------------
#Define a main function, the entry point of the program
if __name__ == '__main__':
    #This object handles Twitter authetification and the connection to Twitter Streaming API
    myListener = StdOutListener()
    authenticator = OAuthHandler(cKey, cSecret)
    authenticator.set_access_token(aToken, aTokenSecret)
    stream = Stream(authenticator, myListener)
    #stream.filter(track=['Ecuador'])
    stream.filter(track=['@MSPowerBI',"@qlik","@tableau"])
#---------------------------------------------------------


