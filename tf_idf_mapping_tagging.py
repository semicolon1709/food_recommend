
from pymongo import MongoClient
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
import json
keyword_list =[]
data_list = []
corpus = []
articles = []

# with open("ipeen_store_list.json", "r", encoding="utf-8") as r:
#     store_list = json.loads(r.read())
# with open("raw_article.json", "r", encoding="utf-8") as r:
#     data_list = json.loads(r.read())
# with open("corpus.json", "r", encoding="utf-8") as r:
#     corpus = json.loads(r.read())
# with open("articles.json", "r", encoding="utf-8") as r:
#     articles = json.loads(r.read())

with open("dict_for_mapping.json", "r", encoding="utf-8") as r:
    map_dict = json.loads(r.read())    

client = MongoClient('localhost', 27017)
db = client['cuisines']
raw_data0 = db.foodPTT.find({"city": "桃園"}, {"title": 1, "raw_content":1, "content": 1, "_id": 0})
raw_data1 = db.foodPixnet.find({"city": "桃園"}, {"title": 1, "raw_content":1, "content": 1, "_id": 0})
raw_data2 = db.foodMobile01.find({"city": "桃園"}, {"title": 1, "raw_content":1, "content": 1, "_id": 0}) 

for task in [raw_data0,raw_data1,raw_data2]:
    data_list.extend([data for data in task])

print("raw_article:" + str(len(data_list)))
with open("raw_article.json", "w", encoding="utf-8") as w:
    w.write(json.dumps(data_list, ensure_ascii=False))



corpus = []
articles = []
for data in data_list:
    chk = True
    for store_mapping in map_dict.keys():
        if store_mapping in data["title"]:
            article_dict = {
                    "store_name": map_dict[store_mapping],
                    "title": data["title"]
                }
            articles.append(article_dict)
            corpus.append(data["content"])
            chk = False
            break
    if chk:
        for store_mapping in map_dict.keys():
            if store_mapping in data["raw_content"]:
                article_dict = {
                        "store_name": map_dict[store_mapping],
                        "title": data["title"]
                    }
                articles.append(article_dict)
                corpus.append(data["content"])
                break 

print("match_article:" + str(len(articles)))
with open("corpus.json", "w", encoding="utf-8") as w:
    w.write(json.dumps(corpus, ensure_ascii=False))
with open("articles.json", "w", encoding="utf-8") as w:
    w.write(json.dumps(articles, ensure_ascii=False))

vectorizer = CountVectorizer()
transformer = TfidfTransformer()
vector = vectorizer.fit_transform(corpus)
words = vectorizer.get_feature_names()
tf_idf = transformer.fit_transform(vector)

for article_tf_idf in tf_idf.toarray():
    keyword_list.append(sorted(list(zip(words, article_tf_idf)), key=lambda x: x[1], reverse=True)[:20])
for i in range(len(articles)):
    articles[i]["keyword"] = keyword_list[i]

matched_store_list = list(set([store["store_name"] for store in articles]))
matched_store_dict = {}
for matched_store in matched_store_list:
    matched_store_dict[matched_store] = []
for article in articles:
    article_tmp = {
        "title": article["title"],
        "keyword": article["keyword"]
    }
    matched_store_dict[article["store_name"]].append(article_tmp)

store_with_tag_list = []
for key in matched_store_dict:
    keyword_count = []
    keyword_count_sum = []
    for _dict in matched_store_dict[key]:
        for keyword in _dict["keyword"]:
            keyword_count.append((keyword[0], 1))
    keyword_count = sorted(keyword_count, key=lambda x: x[0])
    count = 1
    for i in range(len(keyword_count)):
        if i < len(keyword_count) - 1:
            if keyword_count[i][0] == keyword_count[i + 1][0]:
                count += 1
            else:
                keyword_count_sum.append((keyword_count[i][0], count))
                count = 1
        else:
            if count == 1:
                keyword_count_sum.append((keyword_count[i][0], 1))
            else:
                keyword_count_sum.append((keyword_count[i][0], count + 1))
    keyword_count_sum = sorted(keyword_count_sum, key=lambda x: x[1], reverse=True)[0:20]
    store_tag_dict = {
        "store_name": key,
        "tag": [tag[0] for tag in keyword_count_sum]
    }
    store_with_tag_list.append(store_tag_dict)
count_dict = {}
for key in set([tag for store in store_with_tag_list for tag in store["tag"]]):
    count_dict[key] = 0
for tag in [tag for store in store_with_tag_list for tag in store["tag"]]:
    count_dict[tag] = count_dict[tag] + 1
important_tag = [item[0] for item in count_dict.items() if item[1] > 4]
for store in store_with_tag_list:
    store["Tag"] = []
    for tag in store["tag"]:
        if tag in important_tag:
            store["Tag"].append(tag)
    store.pop("tag",None)
    store["tag"] = store["Tag"]
    store.pop("Tag",None)
    
with open("store_with_tags.json", "w", encoding="utf-8") as w:
    w.write(json.dumps(store_with_tag_list, ensure_ascii=False))



tag_list = list(set([tag  for store in store_with_tag_list for tag in store["tag"]]))
db.foodIPEEN.update({},{'$set': {'tag': []}}, multi=True, upsert=True)
db.foodIPEEN.update({},{"$set":{"tag_vector":[0 for i in range(len(tag_list))]}} ,multi=True, upsert=True)

for store in store_with_tag_list:
    tmp_dict = {}
    for _tag  in tag_list:
        tmp_dict[_tag] = 0
    for tag in store["tag"]:
        tmp_dict[tag] = 1
    vector = list(tmp_dict.values())
    db.foodIPEEN.update_one({"name":store["store_name"]}, {"$set":{"tag_vector":vector}}, upsert=False)
    db.foodIPEEN.update_one({"name":store["store_name"]}, {"$set":{"tag":store["tag"]}}, upsert=False)



user_dict = {}
for _tag  in tag_list:
    user_dict[_tag] = 0
for tag in ["咖啡","火鍋","起司","海鮮","壽司","拉麵","下午茶","豬排","咖哩","仙草","鬆餅","奶茶",
         "吐司","奶油","冰淇淋","鴨血","麵線","生魚片","味噌","明太子","丼飯","芒果","水餃","拿鐵","小吃",
          "麻辣","珍珠","干貝","鮭魚","水果茶","冰品","大腸","香蕉","豬肝","生蠔","豬腳","黑糖","花生醬",
          "雞腿","排骨","居酒","粉圓","炸豆腐","海膽","蒜泥"]:
    user_dict[tag] = 1
print(list(user_dict.values()))





