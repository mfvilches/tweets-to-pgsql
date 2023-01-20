import snscrape.modules.twitter as sntwitter
import pandas as pd
from datetime import datetime
from datetime import timedelta
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import time
import config

# Construimos los términos de búsqueda
santiago = '-33.45694, -70.64827, 10km'
caldera = '-27.06766, -70.82157, 10km'
punta_arenas = '-53.16308, -70.90858, 10km'
concepcion = '-36.82731, -73.04920, 10km'
la_serena = '-29.90581, -71.25021, 10km'
concon = '-32.94125, -71.54666, 10km'


desde = int(datetime.timestamp(datetime(año,mes,dia-ndias)))
hasta = int(datetime.timestamp(datetime(año,mes,dia)))

search_santiago = '"since_time:"{}" '.format(desde)+'"until_time:"{}" '.format(hasta)+'"geocode:"{}"'.format(santiago)
search_caldera = '"since_time:"{}" '.format(desde)+'"until_time:"{}" '.format(hasta)+'"geocode:"{}"'.format(caldera)
search_punta_arenas = '"since_time:"{}" '.format(desde)+'"until_time:"{}" '.format(hasta)+'"geocode:"{}"'.format(punta_arenas)
search_concepcion = '"since_time:"{}" '.format(desde)+'"until_time:"{}" '.format(hasta)+'"geocode:"{}"'.format(concepcion)
search_la_serena = '"since_time:"{}" '.format(desde)+'"until_time:"{}" '.format(hasta)+'"geocode:"{}"'.format(la_serena)
search_concon = '"since_time:"{}" '.format(desde)+'"until_time:"{}" '.format(hasta)+'"geocode:"{}"'.format(concon)

# hacemos las búsquedas por ciudad
tweets_santiago = sntwitter.TwitterSearchScraper(search_santiago).get_items()
tweets_caldera = sntwitter.TwitterSearchScraper(search_caldera).get_items()
tweets_punta_arenas = sntwitter.TwitterSearchScraper(search_punta_arenas).get_items()
tweets_concepcion = sntwitter.TwitterSearchScraper(search_concepcion).get_items()
tweets_la_serena = sntwitter.TwitterSearchScraper(search_la_serena).get_items()
tweets_concon = sntwitter.TwitterSearchScraper(search_concon).get_items()

list_santiago =[]
list_caldera =[]
list_punta_arenas=[]
list_concepcion=[]
list_la_serena=[]
list_concon=[]

for i,tweet in enumerate(tweets_santiago):
    if tweet.coordinates is None: 
        longitud = 0.0
        latitud = 0.0
    else:
        longitud = tweet.coordinates.longitude
        latitud = tweet.coordinates.latitude
    list_santiago.append([tweet.date,tweet.user.username,tweet.user.location,longitud,latitud,tweet.id,tweet.user.id,tweet.rawContent,tweet.lang,tweet.retweetedTweet,"Santiago"])
df_santiago = pd.DataFrame(list_santiago,columns=['fecha','username','location', 'long','lat','id','user_id','tweet','lang','retwiteado','ciudad'])
print(df_santiago.fecha.count())

for i,tweet in enumerate(tweets_caldera):
    if tweet.coordinates is None: 
        longitud = 0.0
        latitud = 0.0
    else:
        longitud = tweet.coordinates.longitude
        latitud = tweet.coordinates.latitude
    list_caldera.append([tweet.date,tweet.user.username,tweet.user.location,longitud,latitud,tweet.id,tweet.user.id,tweet.rawContent,tweet.lang,tweet.retweetedTweet,"Caldera"])
df_caldera = pd.DataFrame(list_caldera,columns=['fecha','username','location', 'long','lat','id','user_id','tweet','lang','retwiteado','ciudad'])
print(df_caldera.fecha.count())

for i,tweet in enumerate(tweets_punta_arenas):
    if tweet.coordinates is None: 
        longitud = 0.0
        latitud = 0.0
    else:
        longitud = tweet.coordinates.longitude
        latitud = tweet.coordinates.latitude
    list_punta_arenas.append([tweet.date,tweet.user.username,tweet.user.location,longitud,latitud,tweet.id,tweet.user.id,tweet.rawContent,tweet.lang,tweet.retweetedTweet,"Punta Arenas"])
df_punta_arenas = pd.DataFrame(list_punta_arenas,columns=['fecha','username','location', 'long','lat','id','user_id','tweet','lang','retwiteado','ciudad'])
print(df_punta_arenas.fecha.count())

for i,tweet in enumerate(tweets_concepcion):
    if tweet.coordinates is None: 
        longitud = 0.0
        latitud = 0.0
    else:
        longitud = tweet.coordinates.longitude
        latitud = tweet.coordinates.latitude
    list_concepcion.append([tweet.date,tweet.user.username,tweet.user.location,longitud,latitud,tweet.id,tweet.user.id,tweet.rawContent,tweet.lang,tweet.retweetedTweet,"Concepcion"])
df_concepcion = pd.DataFrame(list_concepcion,columns=['fecha','username','location', 'long','lat','id','user_id','tweet','lang','retwiteado','ciudad'])
print(df_concepcion.fecha.count())

for i,tweet in enumerate(tweets_la_serena):
    if tweet.coordinates is None: 
        longitud = 0.0
        latitud = 0.0
    else:
        longitud = tweet.coordinates.longitude
        latitud = tweet.coordinates.latitude
    list_la_serena.append([tweet.date,tweet.user.username,tweet.user.location,longitud,latitud,tweet.id,tweet.user.id,tweet.rawContent,tweet.lang,tweet.retweetedTweet,"La Serena"])
df_la_serena = pd.DataFrame(list_la_serena,columns=['fecha','username','location', 'long','lat','id','user_id','tweet','lang','retwiteado','ciudad'])
print(df_la_serena.fecha.count())

for i,tweet in enumerate(tweets_concon):
    if tweet.coordinates is None: 
        longitud = 0.0
        latitud = 0.0
    else:
        longitud = tweet.coordinates.longitude
        latitud = tweet.coordinates.latitude
    list_concon.append([tweet.date,tweet.user.username,tweet.user.location,longitud,latitud,tweet.id,tweet.user.id,tweet.rawContent,tweet.lang,tweet.retweetedTweet,"Con Con"])
df_concon = pd.DataFrame(list_concon,columns=['fecha','username','location', 'long','lat','id','user_id','tweet','lang','retwiteado','ciudad'])
print(df_concon.fecha.count())

df_caldera.replace({np.nan: None}, inplace = True)
df_punta_arenas.replace({np.nan: None}, inplace = True)
df_concepcion.replace({np.nan: None}, inplace = True)
df_la_serena.replace({np.nan: None}, inplace = True)
df_concon.replace({np.nan: None}, inplace = True)
df_santiago.replace({np.nan: None}, inplace = True)

db = create_engine(conn_string)
conn = db.connect()

start_time = time.time()
df_santiago.to_sql('tweets', con=conn, if_exists='append', index=False)
df_caldera.to_sql('tweets', con=conn, if_exists='append', index=False)
df_punta_arenas.to_sql('tweets', con=conn, if_exists='append', index=False)
df_concepcion.to_sql('tweets', con=conn, if_exists='append', index=False)
df_concon.to_sql('tweets', con=conn, if_exists='append', index=False)
df_la_serena.to_sql('tweets', con=conn, if_exists='append', index=False)

print("to_sql duration: {} seconds".format(time.time() - start_time))