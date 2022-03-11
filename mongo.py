import pymongo
from pprint import pprint
import re
import json
import requests

client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client['test_book_mdb']

book_collection = db['books_json']

# print(client.list_database_names())
# print(db.list_collection_names())
# print(book_collection.find_one())

# Insert Doc from the Json file
with open('books.json') as f:
    doc_data = json.load(f)

# Insert docs from the url
# doc_data = requests.get('https://raw.githubusercontent.com/ozlerhakan/mongodb-json-files/master/datasets/books.json').json()

book_collection.insert_many(doc_data)

question_1 = book_collection.find().sort("title").limit(5)
# pprint(question_1.explain())
print("============Question 1===========")
for doc in question_1:
    print('title = ', doc['title'])

question_2 = book_collection.find({"authors":{"$size": 2}}).sort("title").limit(5)
# pprint(question_2.explain())
print("============Question 2===========")
for doc in question_2:
    print('title = {0}, authors = {1}'.format(doc['title'], doc['authors']))

question_3 = book_collection.find({"authors":{"$all": ["W. Frank Ableson"]}})
# pprint(question_3.explain())
print("============Question 3===========")
for doc in question_3:
    print('title = ', doc['title'])

search_str = "Web"
search_re = re.compile(f".*{search_str}.*", re.I)
question_4 = book_collection.find({"title": {"$regex": search_re}})
# pprint(question_4.explain())
print("============Question 4===========")
for doc in question_4:
    print('title = ', doc['title'])

question_5 = len(book_collection.distinct('authors'))
# pprint(question_5.explain())
print("============Question 5===========")
print('Number of authors = ', question_5)

client.close()