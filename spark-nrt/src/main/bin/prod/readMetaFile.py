import json
import sys

file=sys.argv[1]
output=sys.argv[2]

f=open(output, "w+")
with open(file) as json_file:
    data = json.load(json_file)
    for metaFile in data['metaFiles']:
        date = metaFile['date']
        for file in metaFile['files']:
            f.write(date + ' ' + file + '\n')

f.close()