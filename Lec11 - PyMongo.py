#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymongo
import datetime
import pprint
from bson.son import SON


# ### Be sure to run mongod

# In[2]:


# Get a client connection, default connection information
client = pymongo.MongoClient()


# In[3]:


# Get a client connection, host and port
client = pymongo.MongoClient('localhost', 27017)


# In[4]:


# Another way of saying the same thing
client = pymongo.MongoClient('mongodb://localhost:27017/')


# In[5]:


# Just in case, we'll drop our test database
client.drop_database('test_database')


# In[6]:


# Get connection to database
db = client.test_database


# In[7]:


# Same thing said differently
db = client['test_database']


# In[8]:


# Get a collection in the DB
collection = db.test_collection


# In[9]:


# Same thing said differently
collection = db['test_collection']


# In[10]:


# Why does it not show up?  Created only when needed
db.list_collection_names()


# In[11]:


# Create some data
post = {"author": "Mike",
        "text": "My first blog post!",
        "tags": ["mongodb", "python", "pymongo"],
        "date": datetime.datetime.utcnow()}


# In[12]:


# Insert data
posts = db.posts
post_id = posts.insert_one(post).inserted_id
post_id


# In[13]:


# NOW the collection exists
db.list_collection_names()


# In[14]:


# Get some data
pprint.pprint(posts.find_one())


# In[15]:


# Query format
pprint.pprint(posts.find_one({"author": "Mike"}))


# In[16]:


posts.find_one({"author": "Eliot"})


# In[17]:


post_id


# In[18]:


# Get a specific item by ID
pprint.pprint(posts.find_one({"_id": post_id}))


# In[19]:


# What if we treat it as a string?
post_id_as_str = str(post_id)
posts.find_one({"_id": post_id_as_str})


# In[20]:


post_id_as_str


# In[21]:


# Bulk insert
new_posts = [{"author": "Mike",
                "text": "Another post!",
                "tags": ["bulk", "insert"],
                "date": datetime.datetime(2009, 11, 12, 11, 14)},
            {"author": "Eliot",
            "title": "MongoDB is fun",
            "text": "and pretty easy too!",
            "date": datetime.datetime(2009, 11, 10, 10, 45)}]
result = posts.insert_many(new_posts)
result.inserted_ids


# In[22]:


# Query 
for post in posts.find():
    pprint.pprint(post)
    print()


# In[23]:


# Find all that match some criterion
for post in posts.find({"author": "Mike"}):
    pprint.pprint(post)
    print()


# In[24]:


posts.count_documents({})


# In[25]:


posts.count_documents({"author": "Mike"})


# In[26]:


d = datetime.datetime(2009, 11, 12, 12)
for post in posts.find({"date": {"$lt": d}}).sort("author"):
    pprint.pprint(post)
    print()


# In[ ]:





# In[27]:


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
myclient.drop_database("mydatabase")
mydb = myclient["mydatabase"]


# In[28]:


print(myclient.list_database_names())


# In[29]:


mycol = mydb["customers"]


# In[30]:


print(mydb.list_collection_names())


# In[31]:


print(myclient.list_database_names())


# In[32]:


mydict = { "name": "Tim"}
x = mycol.insert_one(mydict)


# In[33]:


print(x.inserted_id)


# In[34]:


print(myclient.list_database_names())


# In[35]:


mylist = [
  { "name": "Amy", "address": "Apple st 652"},
  { "name": "Hannah", "address": "Mountain 21"},
  { "name": "Michael", "address": "Valley 345"},
  { "name": "Sandy", "address": "Ocean blvd 2"},
  { "name": "Betty", "address": "Green Grass 1"},
  { "name": "Richard", "address": "Sky st 331"},
  { "name": "Susan", "address": "One way 98"},
  { "name": "Vicky", "address": "Yellow Garden 2"},
  { "name": "Ben", "address": "Park Lane 38"},
  { "name": "William", "address": "Central st 954"},
  { "name": "Chuck", "address": "Main Road 989"},
  { "name": "Viola", "address": "Sideway 1633"}
]

x = mycol.insert_many(mylist)

print(x.inserted_ids)


# In[36]:


mylist = [
  { "_id": 1, "name": "John", "address": "Highway 37"},
  { "_id": 2, "name": "Peter", "address": "Lowstreet 27"},
  { "_id": 3, "name": "Amy", "address": "Apple st 652"},
  { "_id": 4, "name": "Hannah", "address": "Mountain 21"},
  { "_id": 5, "name": "Michael", "address": "Valley 345"},
  { "_id": 6, "name": "Sandy", "address": "Ocean blvd 2"},
  { "_id": 7, "name": "Betty", "address": "Green Grass 1"},
  { "_id": 
   8, "name": "Richard", "address": "Sky st 331"},
  { "_id": 9, "name": "Susan", "address": "One way 98"},
  { "_id": 10, "name": "Vicky", "address": "Yellow Garden 2"},
  { "_id": 11, "name": "Ben", "address": "Park Lane 38"},
  { "_id": 12, "name": "William", "address": "Central st 954"},
  { "_id": 13, "name": "Chuck", "address": "Main Road 989"},
  { "_id": 14, "name": "Viola", "address": "Sideway 1633"}
]

x = mycol.insert_many(mylist)

print(x.inserted_ids)


# In[37]:


x = mycol.find_one()

print(x)


# In[38]:


for x in mycol.find():
  print(x, "\n")


# In[39]:


for x in mycol.find({},{"_id": 0, "address": 1 }):
  print(x, "\n")


# In[40]:


myquery = { "address": {"$eq":None} }

mydoc = mycol.find(myquery)

for x in mydoc:
  print(x, "\n")


# In[41]:


myquery = { "_id": { "$gt": 10 } }

mydoc = mycol.find(myquery)

for x in mydoc:
  print(x, "\n")


# In[42]:


myquery = { "address": { "$regex": "^S" } }

mydoc = mycol.find(myquery)

for x in mydoc:
  print(x, "\n")


# In[43]:


mydoc = mycol.find().sort("name")

for x in mydoc:
  print(x, "\n")


# In[44]:


mydoc = mycol.find().sort("name", -1)

for x in mydoc:
  print(x, "\n")


# In[45]:


print(mydb.list_collection_names())


# In[46]:


myquery = { "address": { "$regex": "^S" } }
newvalues = { "$set": { "name": "Minnie" } }

x = mycol.update_many(myquery, newvalues)

print(x.modified_count, "documents updated.")


# In[47]:


myquery = { "address": { "$regex": "^S" } }
for x in mycol.find(myquery):
  print(x, "\n")


# In[48]:


myquery = { "address": "Mountain 21" }
mycol.delete_one(myquery)


# In[49]:


x = mycol.delete_many({})

print(x.deleted_count, " documents deleted.")


# In[50]:


mycol.drop()


# In[51]:


myclient.drop_database("aggregation_example")


# In[52]:


db = pymongo.MongoClient().aggregation_example
result = db.things.insert_many([{"x": 1, "tags": ["dog", "cat"]},
                                {"x": 2, "tags": ["cat"]},
                                {"x": 2, "tags": ["mouse", "cat", "dog"]},
                                {"x": 3, "tags": []}])
result.inserted_ids


# In[58]:


pipeline = [
    {"$unwind": "$tags"},
    {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
    {"$sort": SON([("count", -1), ("_id", -1)])}
]
pprint.pprint(list(db.things.aggregate(pipeline)))


# In[ ]:




