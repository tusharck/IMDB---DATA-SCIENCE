import urllib
import urllib.request as uReq
import bs4                           
from bs4 import BeautifulSoup 
import pandas as pd
from time import sleep
from time import time
from random import randint
from IPython.core.display import clear_output
from warnings import warn

import base64

import requests as abcd123

import pickle 
#from bs4 import BeautifulSoup as soup
import requests
from requests import get

#rows = containers[0].findAll("tr")
imdb_id= []
names = []
years = []
imdb_ratings = []
metascores = []
votes = []
discription = []
genre = []
runtime = []
director = []
stars = []
image = []
country = []
language = []

start_time = time()
requests = 0


headers = {"Accept-Language": "en-US, en;q=0.5; charset=UTF-8"}
i=0

start = 1
end = 0

mainCounter = 1

parentUrl  = 'https://www.imdb.com'
childUrl = '/search/title?release_date=2004-01-01,2005-12-31&count=250'
#'/search/title?release_date=2004-01-01,2007-12-31&count=250'

def scrape_country_and_language(imdbID):
    detail_ur = 'https://www.imdb.com/title/' + str(imdbID) + '/?ref_=adv_li_tt'
    response = get(detail_ur)
    deatail_page_html = BeautifulSoup(response.text, 'html.parser')
    detail_mv_containers = deatail_page_html.find('div', id = 'titleDetails')
    sub_container = detail_mv_containers.find_all('div',class_='txt-block')
    #print(detail_mv_containers)
    CN = ''
    LN = ''

    for sc in sub_container:
        i=0
        
        if sc is not None:
                
            if 'Country' in str(sc):
                cntry = sc.find_all('a')
                
                for inCN in cntry:
                    
                    if i==0:
                        CN = CN + inCN.text

                    else:
                        CN = CN + ',' + inCN.text

                    i=i+1
            if 'Language' in str(sc):
                lng = sc.find_all('a')
                
                for inLN in lng:
                    
                    if i==0:
                        LN = LN + inLN.text

                    else:
                        LN = LN + ',' + inLN.text

                    i=i+1
    return CN,LN
    
while start != end:
    if mainCounter % 200 == 0:

        movie_ratings = pd.DataFrame({'imdb_id':imdb_id,
                            'movie': names,
                            'year': years,
                            'genre': genre,
                            'runtime': runtime,
                            'country': country,
                            'language': language,
                            'imdb': imdb_ratings,
                            'metascore': metascores,
                            'votes': votes,
                            'Director':director,
                            'stars':stars,
                            'Discription':discription,
                            'Image':image})

        print(movie_ratings.info())

        movie_ratings.to_csv('batch-3-' + str(mainCounter) +'.csv', index=False )
        imdb_id = []
        names = []
        years = []
        imdb_ratings = []
        metascores = []
        votes = []
        discription = []
        genre = []
        runtime = []
        director = []
        stars = []
        language = []
        image = []
        country = []


    mainUrl = parentUrl + childUrl
    print(mainUrl)
    response = get(mainUrl)

    # Pause the loop
    sleep(randint(6,10))

    # Monitor the requests
    requests += 1
    print('Request:{}'.format(requests))
    clear_output(wait = True)
    
    # Throw a warning for non-200 status codes
    if response.status_code != 200:
        warn('Request: {}; Status code: {}'.format(requests, response.status_code))
    
    # Parse the content of the request with BeautifulSoup
    page_html = BeautifulSoup(response.text, 'html.parser')

    
    # Select all the 50 movie containers from a single page
    mv_containers = page_html.find_all('div', class_ = 'lister-item mode-advanced')

    mymy = page_html.find('div',class_= 'desc')
    abc = str(mymy.text).split(' ')
    num = abc[2].split(',')
    end = int(''.join(list(num)))

    num2 = abc[0].split('-')
    num2 = num2[1].split(',')
    start = int(''.join(list(num2)))

    print(start)
    print(end)

    next = str(page_html.find('a',class_= 'lister-page-next next-page'))
    startIndex = next.find('href="') + 6
    endIndex = next.find('">Next')

    childUrl = next[startIndex:endIndex]

    for container in mv_containers:
        
        # If the movie has a Metascore, then:
        
            # Scrape the name

        ID = str(container.find('div',class_='ribbonize')['data-tconst'])
        ID = ID.strip()
        imdb_id.append(ID)

        LC = scrape_country_and_language(ID)
        country.append(LC[0])
        language.append(LC[1])
        name = container.h3.a.text
        names.append(str(name).strip())

        # Scrape the year 
        year = str(container.h3.find('span', class_ = 'lister-item-year').text)
        year = year.replace("‚Äì", "-")
        years.append(str(year).strip())

        # Scrape the IMDB rating
        if container.strong is not None:
            imdb = float(container.strong.text)
        else:
            imdb = 0.0
        imdb_ratings.append(imdb)

        # Scrape the Metascore
        if container.find('div', class_ = 'ratings-metascore') is not None:
            m_score = container.find('span', class_ = 'metascore').text
        else:
            m_score = 0.0
        metascores.append(int(m_score))

        # Scrape the number of votes
        if container.find('p', class_ = 'sort-num_votes-visible') is not None:
            w = container.find('p', class_ = 'sort-num_votes-visible')
            if w.find('span'):
                vote = container.find('span', attrs = {'name':'nv'})['data-value']
            else:
                vote = str('0')
        else:
            vote = str('0')
        votes.append(str(vote))
        
        if container.find('span', class_ = 'genre') is not None:
            genreData = str(container.find('span', class_ = 'genre').text)
        else:
            genreData = 'NA'
        genre.append(str(genreData).strip())

        if container.find('span', class_ = 'runtime') is not None:
            run = str(container.find('span', class_ = 'runtime').text)
        else:
            run = 'NA'
        runtime.append(str(run).strip())

        if container.find_all('p', class_ = 'text-muted') is not None:
            discMain = container.find_all('p', class_ = 'text-muted')

            disc = discMain[1].text
        else:
            disc = 'NA'
        discription.append(str(disc).strip())

        if container.find_all('p', class_ = '') is not None:
            dirMain = container.find_all('p', class_ = '')
            for dirIter in dirMain:
                dir =''
                star = ''
                if 'Director' in str(dirIter.text) and 'Star' in str(dirIter.text):
                    xyz3 = dirIter.find_all('a')
                    i=0
                    j=0
                    for p3 in xyz3:
                        if '_dr_' in str(p3):
                            if i==0:
                                dir = dir + p3.text

                            else:
                                dir = dir + ',' + p3.text

                            i=i+1

                        elif '_st_' in str(p3):
                            if j==0:
                                star = star + p3.text
                            else:
                                star = star + ',' + p3.text
                            j=j+1

                elif 'Director:' in str(dirIter.text):
                    star = 'NA'
                    i=0
                    xyz2 = dirIter.find_all('a')
                    for p2 in xyz2:
                        if i==0:
                            dir = dir  + p2.text
                        else:
                            dir = dir + ',' + p2.text
                        i=i+1
                elif 'Stars:' in str(dirIter.text):
                    i=0
                    dir='NA'
                    xyz = dirIter.find_all('a')
                    for p1 in xyz:
                        if i==0:
                            star = star + p1.text
                        else:
                            star = star + ',' + p1.text
                        i=i+1
                else:
                    dir = 'NA'
                    star = 'NA'

        else:
            dir = 'NA'
            star = 'NA'
        director.append(str(dir).strip())
        stars.append(str(star).strip())

        if container.find('img', class_ = 'loadlate') is not None:
            img = container.find('img', class_ = 'loadlate')['loadlate']

            try:
                img = str(base64.b64encode(abcd123.get(img).content))
                img = img[2:(len(img)-1)]
            except:
                print('Error in ')
                img = 'NA'
                print('image not taken')
        else:
            img = 'NA'
        image.append(str(img))

    mainCounter = mainCounter + 1

movie_ratings = pd.DataFrame({'imdb_id':imdb_id,
                            'movie': names,
                            'year': years,
                            'genre': genre,
                            'runtime': runtime,
                            'country': country,
                            'language': language,
                            'imdb': imdb_ratings,
                            'metascore': metascores,
                            'votes': votes,
                            'Director':director,
                            'stars':stars,
                            'Discription':discription,
                            'Image':image})

print(movie_ratings.info())

movie_ratings.to_csv('batch-3-final.csv', index=False )
