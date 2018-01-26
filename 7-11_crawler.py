import requests as r
import re

area_dict = {
    "桃園市":['中壢區','平鎮區','龍潭區','楊梅區','新屋區','觀音區','桃園區','龜山區','八德區','大溪區','大園區','蘆竹區'],
    "新竹縣":["新埔鎮","關西鎮","竹北市","湖口鄉","新豐鄉","芎林鄉","竹東鎮","橫山鄉","北埔鄉","寶山鄉"],
    "新竹市":["東區","北區","香山區"],
    "苗栗縣":["竹南鎮","頭份市","南庄鄉","獅潭鄉","後龍鎮","通霄鎮","苑裡鎮","苗栗市","造橋鄉","頭屋鄉","公館鄉","大湖鄉","銅鑼鄉","卓蘭鎮","三灣鄉"]
}
shop_list = []
for city in area_dict.keys():
    for district in area_dict[city]:
        data={
            "commandid":"SearchStore",
            "city":city,
            "town":district
            
                }
        shops = requests.post("http://emap.pcsc.com.tw/EMapSDK.aspx",data=data).text.split("<POIID>")[1:]
        for shop in shops:
            shop_dict = {}
            shop_dict["_id"] = "uni_" + re.findall('(.{1,6})              </POIID>',shop)[0]
            shop_dict["name"] = re.findall("<POIName>(.*)</POIName>", shop)[0]
            shop_dict["address"] = re.findall("<Address>(.*)</Address>", shop)[0]
            shop_dict["city"] = city
            shop_dict["district"] =  distric
            shop_dict["longitude"] = float(re.findall('<X>(.*)</X>', shop)[0])/1000000
            shop_dict["latitude"] = float(re.findall('<Y>(.*)</Y>', shop)[0])/1000000
            shop_dict["tel"] = re.findall('<Telno>(.*)</Telno>', shop)[0].strip()
            shop_dict["wc"] = re.findall('<isLavatory>(.*)</isLavatory>', shop)[0]
            shop_dict["atm"] = re.findall('<isATM>(.*)</isATM>', shop)[0]
            shop_list.append(shop_dict)