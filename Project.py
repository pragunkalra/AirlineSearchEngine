from tokenize import Name
import pymongo
import pymongo
from pymongo import MongoClient
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from tokenize import Name
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from consolemenu import *
from consolemenu.items import *
from pyspark.sql.functions import desc, col

# connect to the mongoclient
client = pymongo.MongoClient('mongodb://localhost:27017')


df = pd.read_csv("airports.csv")
db = client["Flight"]
data = df.to_dict(orient="records")
db.Airports.insert_many(data)


df2 = pd.read_csv("routes.csv")
data2 = df2.to_dict(orient="records")
db.Routes.insert_many(data2)
Routes=db.get_collection("Routes")

df3 = pd.read_csv("airlines.csv")
data3 = df3.to_dict(orient="records")
db.Airlines.insert_many(data3)
Airlines=db.get_collection("Airlines")
# get the database
db = client['Flight']
Airports=db.get_collection("Airports")

#Find list of aiports operating in the country X
def findcountry(country_X):
    col1=db["Airports"]
    lista=[]
    print("Airport names for country X")
    for doc1 in col1.find({"Country":country_X}):
        lista.append(doc1["Name"])
    print(lista)
'''# Finding list that connects X and Y without 

def findairline(X,Y):
    col2=db["Routes"]
    IDX=[]
    for doc2 in col2.find({"Stops":1}):
        IDX.append(doc2["Airline_ID"])
    Names=[]
    col1=db['Airlines']
    for x in IDX:
        for doc in col1.find({"Airline_ID":int(x)}):
            Names.append(doc["Name"])
    return Names'''
  
#The list of airports having X stops
def findaiports(X):
    col2=db["Routes"]
    IDX=[]
    for doc2 in col2.find({"Stops":1}):
        IDX.append(doc2["Airline_ID"])


    "Finding name using Airline Id"
    Names=[]
    col1=db['Airlines']
    for x in IDX:
     for doc in col1.find({"Airline_ID":int(x)}):
         Names.append(doc["Name"])
    print(Names)
#list of Active airlines in US
def Usairlines():
    Names=[]
    col1=db['Airlines']
    for doc in col1.find({"Active":"Y","Country":"United States"}):
        Names.append(doc["Name"])
    print(Name)
#Country with most Aiports
def mostairports():
    agg_result= db.Airports.aggregate(
    [{
    "$group" : 
        {"_id" : "$Country", 
         "Count" : {"$sum" : 1}
         }}
    ])
    max=0
    for i in agg_result:
        if(max<i['Count']):
            max=i['Count']
            Name=i['_id']
    print(Name)

spark = SparkSession \
.builder \
.appName("mongodbtest1") \
.master('local')\
.config("spark.mongodb.input.uri", "mongodb://27017/Flight") \
.config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/Flight") \
.config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
.getOrCreate()
airport = spark.read\
.format('com.mongodb.spark.sql.DefaultSource')\
.option( "uri", "mongodb://127.0.0.1:27017/Flight.Airports") \
.load()
routes = spark.read\
.format('com.mongodb.spark.sql.DefaultSource')\
.option( "uri", "mongodb://127.0.0.1:27017/Flight.Routes") \
.load()

#Top K cities
def topK(K):
    airportWithRouteS = airport.join(routes,  (
            routes.Destination_airport_ID == airport.Airport_ID),
                                    "inner")
    airportWithRouteD = airport.join(routes, (routes.Source_airport_ID == airport.Airport_ID) ,
                                    "inner")
    airportWithRoute= airportWithRouteS.union(airportWithRouteD)
    airportWithRoute.groupBy("City").count().sort(desc("count")).show(K)

# Trip as a sequence
def seq(src,dst):
    
    # edges

    Sourcelist=list(routes.select('Source_airport').toPandas()['Source_airport'])
    Destinationlist=list(routes.select('Destination_airport').toPandas()['Destination_airport'])


    from collections import defaultdict
    '''Graph Creation'''
    graph = defaultdict(list)
    for (x,y) in zip(Sourcelist, Destinationlist):
        a=x
        b=y
        graph[a].append(b)
        graph[b].append(a)
    
    explored = []
    # Queue used to traverse the graph
    queue = [[src]]
    # Traversing the graph
    while queue:
        path = queue.pop(0)
        node = path[-1]
    # Adding node to neighbour if visited
    if node not in explored:
        neighbours = graph[node]
    #iterating over the neighbours to check if the destination is existing
    for neighbour in neighbours:
        new_path = list(path)
        new_path.append(neighbour)
        queue.append(new_path)
                 
                
    if neighbour == dst:
        print("Routes= ", *new_path)
        return
    explored.append(node)
    print("There is no route available")
    return



def main():
    print("**Welcome to Demo**")
    print("A: findcountry(country_X): B: findairline(X,Y): C Q: Exit")

    choice = input("""
                      A: Find list of aiports operating in the country X:
                      B: Find The list of airports having X stops:
                      C: list of Active airlines in US:
                      D: Country with most Aiports:
                      E: Find top K cities:
                      F: Find Trip as a sequence
                      G: source and destination
                      Q: Exit

                      Please enter your choice: """)

    if choice == "A" or choice =="a":
        print("enter country name")
        x="Iceland"
        findcountry(x)

    elif choice == "B" or choice =="b":
        X="1"
        findaiports(X)

    elif choice == "C" or choice =="c":
        Usairlines()

    elif choice == "D" or choice =="d":
        mostairports()

    elif choice == "E" or choice =="e":
        print("Enter Top k")
        K=2
        topK(K)

    elif choice == "F" or choice =="f":
        print("Enter source and destination ")
        src="SFO"
        dst="HYD"
        seq(src,dst)

    elif choice=="Q" or choice=="q":
        sys.exit
    else:
        print("You must only select either A or B")
        print("Please try again")
    

if __name__ == "__main__":
    main()