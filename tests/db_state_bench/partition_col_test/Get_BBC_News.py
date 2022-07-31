import os
import requests
from bs4 import BeautifulSoup as bs
import xml.etree.ElementTree as ET


TARGET_DIR = "posts"

class BBCNews:
    def __init__(self, url: str):
        contents = requests.get(url).content
        soup = bs(contents, "html.parser")
        self._article = soup.find("article")
        self.body = self.get_body()
        self.title = self.get_title()

    def get_body(self) -> list:
        body = str()
        for i in self._article.find_all("div", recursive=False):
            if ("data-component" in i.attrs) == False or i.attrs["data-component"] != "text-block":
                continue
            body += (i.text + "\n")
        return body

    def get_title(self) -> str:
        return list(self._article.find("header").children)[0].text


class BBCRss:
    def __init__(self, url: str):
        contents = requests.get(url).content
        self.links = [i.text for i in ET.fromstring(
            contents).findall("channel/item/link")]


os.makedirs(TARGET_DIR, exist_ok=True)


def rss_save(name: str, url: str):
    idx = 0

    rrs = BBCRss(url)

    for i in rrs.links:
        print(str(idx), name, url)
        idx += 1

        try:
            pasred = BBCNews(i)
            f = open(TARGET_DIR + "/post_" + name + "_" + str(idx), "w")
            f.write(pasred.title)
            f.write(pasred.body)
            f.close()
        except:
            continue


rss_save("top",         "http://feeds.bbci.co.uk/news/rss.xml")
rss_save("world",       "http://feeds.bbci.co.uk/news/world/rss.xml")
rss_save("uk",          "http://feeds.bbci.co.uk/news/uk/rss.xml")
rss_save("business",    "http://feeds.bbci.co.uk/news/business/rss.xml")
rss_save("politics",    "http://feeds.bbci.co.uk/news/politics/rss.xml")
rss_save("health",      "http://feeds.bbci.co.uk/news/health/rss.xml")
rss_save("education",   "http://feeds.bbci.co.uk/news/education/rss.xml")
rss_save("science_and_environment",
         "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml")
rss_save("technology",  "http://feeds.bbci.co.uk/news/technology/rss.xml")
rss_save("entertainment_and_arts",
         "http://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml")

