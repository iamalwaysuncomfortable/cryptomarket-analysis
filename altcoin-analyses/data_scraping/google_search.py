from data_scraping import scrapingutils as su
from bs4 import BeautifulSoup
from random import randint
from time import sleep

coin_results = su.get_coinlist("https://api.coinmarketcap.com/v1/ticker/")
coin_results = coin_results[["id"]]

for x in xrange(0,6):
    link_name = "link_" + str(x)
    title_name = "title_" + str(x)
    coin_results[link_name] = ""
    coin_results[title_name] = ""

def coin_search(querytext, coin_column, df, start, end):
    data = df
    for i in xrange(start, end):
        url = "https://www.google.com/search?q=" + coin_column[i] + "+" + querytext
        print "Search URL is %s" %(url)
        response = su.get_data(url, max_retries=1)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            links = soup.find_all('cite', class_="_Rm")
            titles = soup.find_all('h3', class_="r")
            for j in xrange(0, 6):
                link_ = "link_" + str(j)
                title_ = "title_" + str(j)
                print "Result number %s from link %s :" % (j, i)
                try:
                    print "Title: %s" %(titles[j].text)
                    print "Link: %s" %(links[j].text)
                    data.set_value(i, link_, links[j].text)
                    data.set_value(i, title_, titles[j].text)
                except:
                    print "Unicode Error, proceeding to next"
            sleep(randint(30, 90))
        else:
            print "process exited at iteration %s with response code of %s" %(i, response.status_code)
            print "response message was %s" %(response.content)
            return data
    print "process was successful"
    return data

results = coin_search("github", coin_results.id, coin_results, 13, 50)
coin_results = coin_results.set_index("symbol")