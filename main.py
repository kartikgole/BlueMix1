#https://github.com/altmanWang/IBM-DB2/blob/master/Insert.py

import csv
import io
from flask import Flask, render_template, request
import time

app = Flask(__name__)
import ibm_db_dbi

cnxn = ibm_db_dbi.connect("#####;", "", "")
if cnxn:
    print('database connected')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST', 'GET'])
def insert_table():

   cursor = cnxn.cursor()

   start_time = time.time()
   cursor.execute("CREATE TABLE equake1(time varchar(50), latitude float(20), longitude float(50), depth float(50), mag float(50), magType varchar(50), nst int, gap int, dmin float(50), rms float(50),net varchar (50), id varchar(50), updated varchar(50), place varchar(50),type varchar(50),horizontal float(50), depthError float(50), magError float(50), magNst int,status varchar (50), locationSource varchar(50), magSource varchar(50))")
   cnxn.commit()

   if request.method == 'POST':
       f = request.files['data_file']
       if not f:
           return "No file"


       stream = io.StringIO(f.stream.read().decode("UTF8"), newline=None)
       csv_input = csv.reader(stream)
       next(csv_input)
       for row in csv_input:
           print(row)
           try:

               cursor.execute(
                   "INSERT INTO equake1(time, latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net, id, updated, place, type, horizontal, depthError, magError, magNst, status, locationSource, magSource) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",row)
               cnxn.commit()

           except Exception as e:
               print(e)
               cnxn.rollback()

   end_time = time.time()
   time_diff = end_time - start_time
   return render_template('index.html',timesdiff = time_diff)


if __name__ == '__main__':
    app.run(debug = True)
