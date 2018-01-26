import re
import random
import requests as r
import pandas
import time
from datetime import datetime
from bs4 import BeautifulSoup as bs
from concurrent.futures import ThreadPoolExecutor, wait
from pymongo import MongoClient
import jieba_cut
import getIP
import gc

def crawler(location, page, IPlist):
    article_count = 0
    try:
        N_try = 0
        while True:
            if N_try > 5:
                print("page connection error")
                break
            try:
                proxies = {"https": "https://{}".format(IPlist[random.randint(0, len(IPlist) - 1)])}
                res = r.get(firsturl.format(location, page, headers=headers, proxies=proxies, timeout=5))
                break
            except:
                N_try += 1
        res.encoding = "utf-8"
        s = bs(res.text, "html.parser")
        for url in s.select(".search-text"):
            try:
                ba = url.select(".search-title > a")[0].get("href")
                back = re.findall("post%2F(.+)", ba)
                fr = url.select(".search-author")[0].get("href")
                front = re.findall("blog/profile/(.+)", fr)
                http = first + front[0] + domain + back[0]
                chk = True
                n_try = 0
                while chk:
                    if n_try > 5:
                        print("article connection error")
                        break
                    try:
                        proxies = {"https": "https://{}".format(IPlist[random.randint(0, len(IPlist) - 1)])}
                        res1 = r.get(http, headers=headers, proxies=proxies, timeout=10)
                        break
                    except:
                        n_try += 1
                res1.encoding = "utf-8"
                s1 = bs(res1.text, "html.parser")
                title = url.select(".search-title > a")[0].get("title")

                if any(word[-2:] in title for word in locations) and all(word not in title for word in exclusive_list):

                    reply_list = []
                    for i in range(0, len(s1.select("#comment-text > .single-post")) - 1):
                        reply = s1.select(".single-post")
                        content = {"author": reply[i].select("li > .user-name")[0].text,
                                   "floor": reply[i].select("li > .floor")[0].text.replace("#", ""),
                                   "content": reply[i].select(".post-text")[0].text.strip()}
                        reply_list.append(content)
                    data_dict = {"_id": front[0] + "-" + back[0],
                            "title": url.select(".search-title > a")[0].get("title"),
                            "author": front[0],
                            "city": location[0:2],
                            "date": url.select(".search-postTime")[0].text.strip(),
                            "forum": None,
                            "web": "pixnet",
                            "url": http,
                            "tag": None,
                            "reply_count": url.select(".search-comments")[0].text.strip(),
                            "click_count": url.select(".search-views > span")[0].text.strip(),
                            "raw_content": s1.select(".article-content-inner")[0].text.strip(),
                            "content": jieba_cut.cut(s1.select(".article-content-inner")[0].text.strip()),
                            "reply_content": reply_list
                            }
                    try:
                        db.foodPixnet.insert(data_dict)
                        article_count += 1
                    except:
                        pass
            except:
                article_error_count_list.append(1)
                print("article error")

        articles_grabbed.append(article_count)
        print("==========================================")
        print(location + " page No." + str(page) + " done")
        print("==========================================")
    except:
        print("page error")
        page_error_count_list.append(1)


if __name__ == "__main__":
    client = MongoClient("localhost", 27017)
    db = client["cuisines"]
    articles_grabbed = []
    page_error_count_list = []
    article_error_count_list = []
    firsturl = "https://www.pixnet.net/searcharticle?q={}美食&page={}"
    first = "https://"
    domain = ".pixnet.net/blog/post/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
    }
    locations = ["桃園", "桃園中壢", "桃園八德", "桃園平鎮", "桃園龜山", "桃園大溪", "桃園楊梅", "桃園蘆竹", "桃園大園", "桃園龍潭",
                 "桃園新屋", "桃園觀音", "新竹", "新竹竹北", "新竹竹東", "新竹新埔", "新竹關西", "新竹湖口", "新竹新豐", "新竹芎林", "新竹橫山", "新竹北埔",
                 "新竹寶山", "新竹香山", "苗栗", "苗栗竹南", "苗栗後龍", "苗栗頭份", "苗栗造橋", "苗栗三灣", "苗栗頭屋", "苗栗南庄", "苗栗公館",
                 "苗栗通霄", "苗栗苑裡", "苗栗銅鑼", "苗栗大湖", "苗栗三義", "苗栗卓蘭"]

    exclusive_list = ["宜蘭", "台北", "臺北", "板橋", "銅鑼灣"]
    numThread = 20
    IPlist = getIP.iplist()
    print(IPlist)
    thStart = datetime.now()
    for location in locations:
        futures = []  
        threads = ThreadPoolExecutor(numThread)  
        for page in range(1, 300):
            futures.append(threads.submit(crawler, location, page, IPlist))
        wait(futures)  
        del threads
        gc.collect()

        time.sleep(2)
    thEnd = datetime.now()


    timeSpent = str(thEnd - thStart).split('.')[0]
    print("\n=======================================")
    print("執行緒數量:" + str(numThread))
    print("文章爬取數:" + str(sum(articles_grabbed)))
    print("page errors:" + str(sum(page_error_count_list)))
    print("article errors:" + str(sum(article_error_count_list)))
    print("耗時:" + timeSpent)
