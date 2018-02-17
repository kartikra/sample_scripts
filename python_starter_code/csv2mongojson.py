#!/usr/bin/python
import csv, codecs, decimal, json
from optparse import OptionParser

'''
This program converts a csv file to mogodb import ready json file.
Input File, Output File and delimiter will be passed as arguments to python program from UNIX.

Sample Input: 
name\tage\tcity\tsalary
joe\t23\tsfo\t5000,45
david\t34\tnyc\t6000,37

Sample Output:
{"name":"joe","age":23,"city":"sfo","salary":5000.45}
{"name":"david","age":34,"city":"nyc","salary":6000.37}


Process to load file in mongo.Run the following commands in UNIX:

# Convert File:
python csv2mongodbjson.py -c example.csv -j example.json -d '\t'


# Load file :
 mongo -u username -p password hostname:port/dbname
 mongoimport --db dbname --collection collectionName --file example.json

'''

# parsing the commandline options
parser = OptionParser(
    description="parses a csv-file and converts it to mongodb json format. The csv file has to have the column names in the first line.")
parser.add_option("-c", "--csvfile", dest="csvfile", action="store", help="input csvfile")
parser.add_option("-j", "--jsonfile", dest="jsonfile", action="store", help="json output file")
parser.add_option("-d", "--delimiter", dest="delimiter", action="store", help="csvdelimiter")

(options, args) = parser.parse_args()


'''For debugging
options.csvfile="/Users/krama/Downloads/en.openfoodfacts.org.products.csv"
options.delimiter="\t"
options.jsonfile="/Users/krama/Downloads/test.json"
'''


data=open(options.csvfile,"rU")
reader_csvfile = csv.DictReader(data, delimiter=options.delimiter)
header = reader_csvfile.fieldnames # read headers

out_jsonfile = codecs.open(options.jsonfile, 'w', encoding="utf-8")

reader_csvfile = csv.DictReader(data, fieldnames=header, delimiter=options.delimiter, quotechar='"')
reader_csvfile.next()
for row in reader_csvfile:
    for k, v in row.items():
        # make sure nulls are generated
        if not v:
            row[k] = None
        # generate a number
        elif k == "age":
            row[k] = int(v)
    data = json.dumps(row)
    out_jsonfile.write(data+"\n")

out_jsonfile.close()
