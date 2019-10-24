import re


def filterMovies(line):
    matchObj = re.match( r'\(\d{4}\)', str(line[2]))
    matchObj2 = re.match( r'\(\S*\) \(\d{4}\)', str(line[2]))
    if line[2] is not None:
        indexofbrace = line[2].find('(')
        if indexofbrace == 0:
            if matchObj or matchObj2:
                return True
#2015–2017
def filter_tv(line):
    matchObj = re.match( r'\(\d{4}.*?\d{4}\)', str(line[2]))
    
    matchObj2 = re.match( r'\(\d{4}[^a-z^A-Z]* \)', str(line[2]))
    if matchObj or matchObj2:\
        return True

def filter_games(line):
    #matchObj = re.match( r'\(\d{4} [\S ]*\)', str(line[2]))
    if line[2] is not None:
        if 'Video Game' in  line[2]:
            return True

def filter_tvmovie(line):
    if line[2] is not None:
        if 'TV Movie' in  line[2]:
            return True

def filter_video(line):
    if line[2] is not None:
        if 'Video)' in  line[2]:
            return True

def filter_tvspecial(line):
    if line[2] is not None:
        if 'TV Special' in  line[2]:
            return True

def filter_tvshort(line):
    if line[2] is not None:
        if 'TV Short' in  line[2]:
            return True