from __future__ import division
import mincemeat
import glob
import pprint
import operator


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

def mapfn(key, value):

        lines = value.splitlines()
        for line in lines[1:]:
                items = line.split(",")
                film_id = items[0]
                rating = items[1]
                date = items[2]
                yield film_id, rating

def reducefn(key, values):

        #data = {}
        values_sum = 0
        elements = 0
        rating = 0
        print "====="
        print key
        for value in values:
                values_sum += int(value)
                elements += 1
        rating = (values_sum / elements)
        print values_sum
        print elements
        print rating
        return rating

s = mincemeat.Server()
s.datasource = data
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server (password = "ruben")

#print results['1428688']
sorted_results = sorted(results.iteritems(), key=operator.itemgetter(1))
pp = pprint.PrettyPrinter(indent=4)
#pp.pprint(results)
pp.pprint(sorted_results)
