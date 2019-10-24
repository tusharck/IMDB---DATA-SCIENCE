import pandas as pd
import os
import sys

inputs = sys.argv[1]
output = sys.argv[2]
os.chdir('../..')
imdbPD = pd.read_csv(str(os.getcwd()) +'/' + inputs)
imdbPD['movie'] = imdbPD['movie'].str.strip()
imdbPD['year'] = imdbPD['year'].str.strip()
imdbPD['genre'] = imdbPD['genre'].str.strip()
imdbPD['runtime'] = imdbPD['runtime'].str.strip()
imdbPD['stars'] = imdbPD['stars'].str.strip()
imdbPD['Discription'] = imdbPD['Discription'].str.strip()
imdbPD.to_csv(str(os.getcwd()) +'/technoaces/dataset/' + str(output) + '.csv' )