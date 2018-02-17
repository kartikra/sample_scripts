import psycopg2
import csv
import difflib

path_infile = r'C:\Users\kartikr\Desktop\duplicate data.csv'
path_outfile = r'C:\Users\kartikr\Desktop\duplicate results.csv'

with open(path_infile) as csv_infile:
    with open(path_outfile, "w+", newline='') as csv_outfile:
        reader_infile = csv.DictReader(csv_infile, delimiter="\t")
        outfile_fieldnames = reader_infile.fieldnames
        outfile_fieldnames.append("difflib_score")
        
        writer_outfile = csv.DictWriter(csv_outfile,fieldnames=outfile_fieldnames,dialect='excel')
        writer_outfile.writeheader()

        for row_infile in reader_infile:
            if row_infile:
                a=row_infile['business_name']
                b=row_infile['dup_business_name']
                seq=difflib.SequenceMatcher(a=a.lower(), b=b.lower())
                difflib_score = "{0:0.2f}".format(seq.ratio())
                row_infile.update({'difflib_score': difflib_score})
                writer_outfile.writerow(row_infile)
