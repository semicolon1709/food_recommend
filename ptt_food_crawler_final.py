
from bs4 import BeautifulSoup as bs
from concurrent.futures import ThreadPoolExecutor, wait
from pymongo import MongoClient
from datetime import datetime
import datetime as dt
import time
import pandas as pd
import numpy as np
import requests as r
import gc
import jieba_cut


domain = "http://www.ptt.cc"
first_url = "https://www.ptt.cc/bbs/{}/index.html"
base_url = "https://www.ptt.cc/bbs/{}/index{}.html"


def forum_crawler(page_num, forum_name):
    '''
    :param page_num:int :要爬取的page
    :param forum_name:str :爬取的板名
    '''

    url = base_url.format(forum_name, page_num)
    articles = bs(r.get(url).text, "lxml").select("div.r-ent")
    print("page:{} start".format(page_num))
    articles_visited_count.append(len(articles))
    article_list = []
    # 逐筆爬取該頁文章內容
    for article in articles:

        article_dict = {

            "_id": "",
            "title": "",
            "author": "",
            "city": "",
            "date": "",
            "forum": forum_name,
            "web": "ptt",
            "url": "",
            "tag": [],
            "reply_count": None,
            "click_count": None,
            "content": "",
            "raw_content"
            "pushCount": None,
            "mark": None,
            "reply_content": []
        }
        try:
            raw_date = article.select_one(".date").text.strip().replace("/", "-")
            title = article.select_one(".title").text.strip()
            category = title.split("[")[1].split("]")[0]

            # 先判別是Food版還是地區版
            if forum_name == "Food":
                area_list = ["桃園", "中壢", "八德", "平鎮", "龜山", "大溪", "楊梅", "蘆竹", "大園", "龍潭", "新屋", "觀音",
                             "竹北", "竹東", "新埔", "關西", "湖口", "新豐", "芎林", "橫山", "北埔", "寶山", "香山", "新竹",
                             "苗栗", "竹南", "後龍", "頭份", "造橋", "三灣", "頭屋", "南庄", "公館", "通霄", "苑裡", "銅鑼",
                             "大湖", "三義", "卓蘭"]
                exclusive_list = ["宜蘭", "台北", "臺北", "板橋", "銅鑼灣"]

                # 文章標題有"食記"，且地區在area_list，不在exclusive_list才爬
                if category == "食記":
                    if any(word in title for word in area_list[0:12]) and all(word not in title for word in exclusive_list):
                        article_dict['city'] = "桃園"
                        article_list.append(content_crawler(title, raw_date, article_dict, article))
                    elif any(word in title for word in area_list[12:24]) and all(word not in title for word in exclusive_list):
                        article_dict['city'] = "新竹"
                        article_list.append(content_crawler(title, raw_date, article_dict, article))
                    elif any(word in title for word in area_list[24:39]) and all(word not in title for word in exclusive_list):
                        article_dict['city'] = "苗栗"
                        article_list.append(content_crawler(title, raw_date, article_dict, article))
            else:
                # 文章標題有"食記"或"美食"才爬
                if category == "食記" or category == "美食":
                    city_dict = {
                        "Taoyuan": "桃園",
                        "ChungLi": "桃園",
                        "Hsinchu": "新竹",
                        "Miaoli": "苗栗"}
                    article_dict['city'] = city_dict[forum_name]
                    # 判別後呼叫content_crawler()爬取文章內容
                    article_list.append(content_crawler(title, raw_date, article_dict, article))
        except:
            pass
    if len(article_list) != 0:
        #獲得文章內容後，將資料存入mongoDB
        db.foodPtt.insert_many(article_list)
        pass
    articles_grabbed_count.append(len(article_list))
    print("page:" + str(page_num) + " done")

def content_crawler(title, raw_date, article_dict, article):
    user_id_list = []
    try:
        article_dict['pushCount'] = article.select_one("span.hl").text.strip()
    except:
        pass
    article_dict['mark'] = article.select_one("div.mark").text.strip()
    content_url = article.select_one("div.title > a")["href"].strip()
    soup_content = bs(r.get(domain + content_url).text, "lxml")
    article_dict["url"] = domain + content_url
    article_dict["_id"] = content_url.split("/")[-1].split(".html")[0]
    content_spliter = soup_content.select("div.article-metaline")[2].select_one("span.article-meta-value").text.strip()
    article_dict['date'] = content_spliter.split(" ")[-1] + "-" + raw_date
    # 如果是回復的文章，將引用本文內容的部分去除
    if title.startswith("Re"):
        article_dict['title'] = "Re: " + title.split("] ")[1]
        article_dict["raw_content"] = soup_content.select_one("div#main-content").text.split(content_spliter)[1].\
            split("※ 發信站: 批踢踢實業坊(ptt.cc)")[0].replace("\n", "")
        try:
            quote_list = [quote.text for quote in
                          soup_content.select_one("div#main-content").select("span.f6")]
            for quote in quote_list:
                article_dict['raw_content'] = article_dict['raw_content'].replace(quote, "")
            quote_list = [quote.text for quote in
                          soup_content.select_one("div#main-content").select("span.f2")]
            for quote in quote_list:
                article_dict["raw_content"] = article_dict["raw_content"].replace(quote, "")
        except:
            pass
    else:
        article_dict['title'] = ("FW: " + title.split("] ")[1] if title.startswith("Fw") else title.split("] ")[1])
        article_dict["raw_content"] = soup_content.select_one("div#main-content").text.split(content_spliter)[1].\
            split("※ 發信站: 批踢踢實業坊(ptt.cc)")[0].replace("\n", "")
    article_dict['author'] = soup_content.select("div.article-metaline")[0].\
        select_one("span.article-meta-value").text.split("(")[0].strip()

    try:
        # 整理推文內容
        push_contents = soup_content.select("div.push")
        reply_dataframe = pd.DataFrame(np.array([[""]]), columns=["content"])
        for push_content in push_contents:
            user_id = push_content.select_one("span.push-userid").text.strip()
            if user_id in user_id_list:
                reply_dataframe.loc[user_id, "content"] += " " + push_content.\
                    select_one("span.push-content").text.split(": ")[1]
            else:
                user_id_list.append(user_id)
                reply_dataframe.loc[user_id, "content"] = \
                    push_content.select_one("span.push-content").text.split(": ")[1]
        for user in reply_dataframe.index:
            if user != 0:
                reply_dic = {
                    "author": "",
                    "floor": "-",
                    "content": ""
                }
                reply_dic["author"] = user
                reply_dic["content"] = reply_dataframe.loc[user, "content"]
                article_dict["reply_content"].append(reply_dic)
    except:
        pass
    # 將文章內容以結巴斷詞
    article_dict["content"] = "-"
    print(article_dict)
    return article_dict


if __name__ == "__main__":
    # 預爬取的版名及爬到第幾頁
    forum_list = [("Food", 1000), ("Taoyuan", 540), ("ChungLi", 600), ("Hsinchu", 260), ("Miaoli", 740)]
    total_time_spent = dt.timedelta()
    report_list = []
    # 多執行緖數量
    num_thread = 60
    # 建立mongdoDB資訊
    client = MongoClient('localhost', 27017)
    db = client['cuisines']

    # 逐版爬取
    for forum in forum_list:
        articles_visited_count = []
        articles_grabbed_count = []
        futures = []

        res = r.get(first_url.format(forum[0]))
        soup = bs(res.text, "lxml")
        # 獲取該版最新頁面頁數
        page_num = int(soup.select_one("div.btn-group-paging").select("a.btn")[1]["href"] \
                       .split("index")[1].split(".")[0]) + 1
        threads = ThreadPoolExecutor(num_thread)

        time_start = datetime.now()
        # 以多執行緖逐頁爬取文章
        for page in range(page_num, forum[1], -1):
            # 建立多執行緖工作清單
            futures.append(threads.submit(forum_crawler, page, forum[0]))
        wait(futures)
        time_end = datetime.now()

        time_spent = str(time_end - time_start).split('.')[0]
        total_time_spent += time_end - time_start
        # 紀錄該版爬取資料
        report = "forum:" + forum[0] + "\n執行緒:" + str(num_thread) + "\n文章總數:" \
                 + str(sum(articles_visited_count)) + "\n文章爬取數:" + str(sum(articles_grabbed_count)) \
                 + "\n耗時:" + time_spent + "\n" + "\n==================="
        report_list.append(report)
        del threads
        gc.collect()
        time.sleep(0.3)

    print("===================")
    list(map(lambda x: print(x), report_list))
    print("total time spent: {}".format(total_time_spent).split('.')[0])

