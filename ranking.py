import mincemeat
import glob
import pprint
import operator
import datetime

#data_files = glob.glob('training_set/*')
data_files = glob.glob('test/*')

def read_data(file_name):
        f = open(file_name)
        try:
                return f.read()
        finally:
                f.close()

data = dict((file_name, read_data(file_name)) for file_name in data_files)

print "Data loaded"
print datetime.datetime.now()

def mapfn(key, value):
    lines = value.splitlines()
    film_id = lines[0][:-1]
    for line in lines[1:]:
        items = line.split(",")
        user_id = items[0]
        rating = items[1]
        date = items[2]
    yield user_id, film_id

def reducefn(key, values):
        number_of_films = 0
        for value in values:
                number_of_films += 1
        return number_of_films

s = mincemeat.Server()
s.datasource = data
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server (password = "ruben")
sorted_results = sorted(results.iteritems(), key=operator.itemgetter(1))
pp = pprint.PrettyPrinter(indent=4)
pp.pprint(sorted_results)
print datetime.datetime.now()
