import requests as r
import pandas

def iplist():
    res = r.get("https://free-proxy-list.net/")
    data = pandas.DataFrame(pandas.read_html(res.text)[0])
    data = data.loc[:, ["IP Address", "Port", "Https"]][data.loc[:, "Https"] == "yes"]
    data = data.drop(data.index[[len(data)-1]])
    data["Port"] = data["Port"].astype(int).astype(str)  # pandas讀入為float，Host有小數點，轉成int,再轉成str
    data["IP"] = data["IP Address"] + ":" + data["Port"]
    data = data["IP"].values.tolist()
    return data






