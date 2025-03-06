#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Data publication as a RESTful service API

The `requirements.txt` file list all the Python packages your code relies
on, and their versions.  
"""

# You can import more modules from the standard library here if you need them
# (which you will, e.g. sqlite3).
import os
from pathlib import Path

# You can import more third-party packages here if you need them, provided
# that they've been used in the weekly labs, or specified in this assignment,
# and their versions match.
from dotenv import load_dotenv          # Needed to load the environment variables from the .env file
import google.generativeai as genai     # Needed to access the Generative AI API

from flask import Flask, request, send_file
from flask_restx import Resource, Api, fields
import requests as rq
import sqlite3
from datetime import datetime

studentid = Path(__file__).stem         # Will capture your zID from the filename.
db_file   = f"{studentid}.db"           # Use this variable when referencing the SQLite database file.
txt_file  = f"{studentid}.txt"          # Use this variable when referencing the txt file for Q7.

app = Flask(__name__)
api = Api(app,
          default = 'Stops',
          title = 'Touring API with stops stroing DB',
          description  = 'Retrieve stop info through Deutsche Bahn API and Touring info through Gemini API')

#schema for the sqlite db
stops_model = api.model('Stops', {
    'stop_id': fields.Integer,
    'name': fields.String,
    'latitude': fields.Float,
    'longitude': fields.Float,
    'last_updated': fields.String(example='yyyy-mm-dd-hh:mm:ss'),
    'self': fields.String,
    'prev': fields.String,
    'next':fields.String,
    'next_departure':fields.String
})

#external DB Api
api_url = 'https://v6.db.transport.rest/'

@api.route('/stops/<string:query>', endpoint = 'stops')
@api.param('query','query string in the form of : query={name of the stop}')  
class GetStops(Resource):
   @api.doc(description='put stops queried from Deutsche Bahn API into Database')
   @api.response(200, 'OK')
   @api.response(201, 'CREATED')
   @api.response(404, 'Stop Not Found')
   @api.response(400, 'Query Malformed')
   @api.response(503, 'Service Not Avalaible')
   def put(self, query):
      resp = rq.get('{api_url}locations?{query}'.format(api_url=api_url, query = query + '&results=5'))
      data = resp.json()

      if resp.status_code != 200:
         if resp.status_code == 404:
            api.abort(404,'Stop queried does not exist.')
         if resp.status_code == 400:
            api.abort(400, 'query is malformed.')
         if resp.status_code == 503:
            api.abort(503, 'Service is not avalaible at the time.')

      for n in data:
         code = 200
         if n['type'] == 'stop':
            t = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
            self = 'http://127.0.0.1:5000/stops/{id}'.format(id=n['id'])
            id = int(n['id'])
            name = n['name']
            latitude = n['location']['latitude']
            longitude = n['location']['longitude']
            cur.execute('SELECT * FROM stops_table WHERE stop_id = ?', (id,))
            selected = cur.fetchone()
            if selected is None:
               insert_query = """INSERT INTO stops_table 
                                 (stop_id,name,latitude,longitude,last_updated,self)
                                 VALUES
                                 (?,?,?,?,?,?);"""
               data_tuple = (id,name,latitude,longitude,t,self)
               cur.execute(insert_query,data_tuple)
               con.commit()
               code = 201
            else:
               update_query = '''UPDATE stops_table SET last_updated = ? where stop_id = ?'''
               data_tuple = (t,id)
               cur.execute(update_query, data_tuple)
               con.commit()

         rows = cur.execute('SELECT stop_id, last_updated,self FROM stops_table ORDER BY stop_id').fetchall()
         result = []
         for row in rows:
            d = {}
            for i, col in enumerate(['stop_id','last_updated','_links']):
               if col != '_links':
                  d[col] = row[i]
               else:
                  d[col] = {'self':{'href':row[i]}}
            result.append(d)

      return result, code
   
@api.route('/stops/<string:include>')
@api.param('include','include query string in the from of : {stop_id}?inlcude={parameters to be included}')  
class StopsInclude(Resource):
   @api.doc(description='retrieve information about a stop in Database from Deutsche Bahn API')
   @api.response(200, 'OK')
   @api.response(404, 'Stop Not Found')
   @api.response(400, 'Query Malformed')
   @api.response(503, 'Service Not Avalaible')
   def get(self, include):
      query = include
      display = []
      if query.isdigit():
         id = int(query)
      else:
         index_n = query.index('?') if '?' in query else -1
         if query[:index_n].isdigit():
            id = int(query[:index_n])
            rest = query[index_n:]
            if rest[:9] == '?include=':
               rest = rest[9:]

               rest = rest.split(',')
              
               for para in rest:
                  if para == '_links' or para =='stop_id':
                     api.abort(400, 'query is malformed, default parameters should not be included.')
                  if para != 'last_updated' and para != 'name' and para != 'latitude' and para != 'longitude' and para != 'next_departure':
                     api.abort(400, 'query parameter is malformed')
               
               display.append('stop_id')

               if 'last_updated' in rest:
                  display.append('last_updated')
               if 'name' in rest:
                  display.append('name')
               if 'latitude' in rest:
                  display.append('latitude')
               if 'longitude' in rest:
                  display.append('longitude')
               if 'next_departure' in rest:
                  display.append('next_departure')

               display.append('_links')
            else:
               api.abort(400, 'query string word is malformed.')
         else:
            api.abort(400, 'query is malformed.')
      
      if display == ['stop_id','_links'] or display == []:
         display = ['stop_id','last_updated','name','latitude','longitude','next_departure','_links']

      cur.execute('SELECT * FROM stops_table WHERE stop_id = ?', (id,))
      selected = cur.fetchone()
      if selected is None:
         api.abort(404, 'Stop not found in DB')
      
      resp = rq.get('{api_url}stops/{id}/departures?duration=120'.format(api_url=api_url, id = id))
      data = resp.json()

      if resp.status_code == 503:
         api.abort(503, 'Service is not avalaible at the time.')

      if data['departures'] == []:
         api.abort(404, 'No departues in next 120 miniutes')
      else:
         for d in data['departures']:
            if d['platform'] and d['direction']:
               des = d
               break
      

      cur.execute('SELECT DISTINCT self from stops_table WHERE stop_id < ? ORDER BY stop_id DESC',(id,))
      prev = cur.fetchone()[0]
      cur.execute('SELECT DISTINCT self from stops_table WHERE stop_id > ? ORDER BY stop_id',(id,))
      next = cur.fetchone()[0]

      update_query = '''UPDATE stops_table SET last_updated = ?, next_departure = ?, next = ?, prev = ? where stop_id = ?'''
      t = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
      next_dep = 'Platform {n} {name} towards {d}'.format(n=des['platform'],name=des['line']['id'],d=des['direction'])
      data_tuple = (t,next_dep,next,prev,id)
      cur.execute(update_query, data_tuple)
      con.commit()


      result = {}
      self = cur.execute('SELECT self FROM stops_table WHERE stop_id = ?',(id,)).fetchone()[0]
      for p in display:
         if p != '_links':
            result[p] = cur.execute('SELECT '+p+ ' FROM stops_table WHERE stop_id = ' +str(id)).fetchone()[0]
         else:
            result[p] = {'self':{'href':self},'next':{'href':next},'prev':{'href':prev}}
      return result, 200
   
@api.route('/stops/<int:stop_id>')
@api.param('stop_id','id of stop (stop_id)')  
class Stops(Resource):
   @api.doc(description='retrieve information about a stop in Database from Deutsche Bahn API (use only stop id with default included value)')
   @api.response(200, 'OK')
   @api.response(404, 'Stop Not Found')
   @api.response(400, 'Query Malformed')
   @api.response(503, 'Service Not Avalaible')
   def get(self, stop_id):
      query = str(stop_id)
      display = ['stop_id','last_updated','name','latitude','longitude','next_departure','_links']
      if query.isdigit():
         id = int(query)
      else:
         api.abort(400, 'query parameter is malformed')

      cur.execute('SELECT * FROM stops_table WHERE stop_id = ?', (id,))
      selected = cur.fetchone()

      if selected is None:
         api.abort(404, 'Stop not found in DB')
      
      resp = rq.get('{api_url}stops/{id}/departures?duration=120'.format(api_url=api_url, id = id))
      data = resp.json()

      des=[]

      if resp.status_code == 503:
         api.abort(503, 'Service is not avalaible at the time.')

      if data['departures'] == []:
         api.abort(404, 'No departues in next 120 miniutes')
      else:
         for d in data['departures']:
            if d['platform'] and d['direction']:
               des = d
               break
      
      
      cur.execute('SELECT DISTINCT self from stops_table WHERE stop_id < ? ORDER BY stop_id DESC',(id,))
      d = cur.fetchone()
      if d:
         prev = d[0]
      else:
         prev = None
      cur.execute('SELECT DISTINCT self from stops_table WHERE stop_id > ? ORDER BY stop_id',(id,))
      d = cur.fetchone()
      if d:
         next = d[0]
      else:
         next = None
      update_query = '''UPDATE stops_table SET last_updated = ?, next_departure = ?, next = ?, prev = ? where stop_id = ?'''
      t = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
      if des != []:
         next_dep = 'Platform {n} {name} towards {d}'.format(n=des['platform'],name=des['line']['id'],d=des['direction'])
      else:
         api.abort(404, 'No departues in next 120 miniutes')

      data_tuple = (t,next_dep,next,prev,id)
      cur.execute(update_query, data_tuple)
      con.commit()

      result = {}
      self = cur.execute('SELECT self FROM stops_table WHERE stop_id = ?',(id,)).fetchone()[0]
      for p in display:
         if p != '_links':
            result[p] = cur.execute('SELECT '+p+ ' FROM stops_table WHERE stop_id = ' +str(id)).fetchone()[0]
         else:
            result[p] = {'self':{'href':self},'next':{'href':next},'prev':{'href':prev}}
      return result, 200
   
   @api.doc(description='delete a stop from Database')
   @api.response(200, 'OK')
   @api.response(404, 'Stop Not Found')
   @api.response(400, 'Query Malformed')
   def delete(self,stop_id):
      if stop_id > 0:
         id = stop_id
      else:
         api.abort(400, 'query parameter is malformed')
   
      cur.execute('SELECT * FROM stops_table WHERE stop_id = ?', (id,))
      selected = cur.fetchone()
      if selected is None:
         result = {'message' : ' The stop_id {s_id} was not found in the database.'.format(s_id = str(id)),
                   'stop_id' : id}
         return result,404
      else:
         cur.execute('DELETE FROM stops_table WHERE stop_id = ?', (id,))
         con.commit()
         result = {'message' : ' The stop_id {s_id} was removed from the database.'.format(s_id = str(id)),
                   'stop_id' : id}
         return result,200
   
   @api.response(404, 'Stop was not found')
   @api.response(400, 'Validation Error')
   @api.response(200, 'OK')
   @api.expect(stops_model, validate=True)
   @api.doc(description="Update a stop by its stop_id in Database")
   def put(self,stop_id):
      id = stop_id
      cur.execute('SELECT * FROM stops_table WHERE stop_id = ?', (id,))
      selected = cur.fetchone()
      if selected is None:
         api.abort(404, "Stop {} doesn't exist".format(id))

      t = ''

      stop = request.json
      if 'stop_id' in stop or 'self' in stop or 'next' in stop or 'prev' in stop:
         return {'message':'Stop Id, links(self,next,prev) are not permitted parameters and cannot be changed'}, 400
      
      if stop == {}:
         return {'message':'Empty request is invalid'}, 400

      if 'name' in stop:
         if stop['name'] == '':
            return {'message':'Empty string in name field is invalid'}, 400
      if 'next_departure' in stop:
         if stop['next_departure'] == '':
            return {'message':'Empty string in next_departure field is invalid'}, 400
      if 'latitude' in stop:
         if stop['latitude'] < -90 or stop['latitude'] > 90 :
            return {'message':'Invalid latitude value'}, 400
      if 'longitude' in stop:
         if stop['longitude'] < -180 or stop['longitude'] > 180 :
            return {'message':'Invalid longitude value'}, 400   
      if 'last_updated' in stop:
         t = stop['last_updated']
         if len(t) != 19:
            return {'message':'Invalid last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         if t[4] != '-' or t[7] != '-' or t[10] != '-' or t[13] != ':' or t[16] != ':':
            return {'message':'Invalid last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         if not t[0:4].isdigit():
            return {'message':'Invalid last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         if not t[5:7].isdigit():
            return {'message':'Invalid last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         if not t[8:10].isdigit():
            return {'message':'Invalid last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         if not t[11:13].isdigit():
            return {'message':'Invalid last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         if not t[14:16].isdigit():
            return {'message':'Invalid last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         if not t[17:].isdigit():
            return {'message':'Invalid last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         
         #month check
         if int(t[5:7]) > 12:
            return {'message':'Invalid month in last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         if int(t[5:7]) > 12:
            return {'message':'Invalid month in last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         
         #day check
         if int(t[8:10]) > 31 and (int(t[5:7]) == 1 or int(t[5:7]) == 3 or int(t[5:7]) == 5 or int(t[5:7]) == 7 or int(t[5:7]) == 8 or int(t[5:7]) == 10 or int(t[5:7]) == 12):
            return {'message':'Invalid day in last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         if int(t[8:10]) > 30 and (int(t[5:7]) == 4 or int(t[5:7]) == 6 or int(t[5:7]) == 9 or int(t[5:7]) == 11):
            return {'message':'Invalid day in last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         if int(t[8:10]) > 28 and int(t[5:7]) == 2 and int(t[0:4])%4 != 0:
            return {'message':'Invalid day in last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         if int(t[8:10]) > 29 and int(t[5:7]) == 2 and int(t[0:4])%4 == 0:
            return {'message':'Invalid day in last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         
         #hour check
         if int(t[11:13]) > 23:
            return {'message':'Invalid hour in last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         
         #minute check
         if int(t[14:16]) > 59:
            return {'message':'Invalid minute in last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
         
         #second check
         if int(t[17:]) > 59:
            return {'message':'Invalid second in last_updated time format, please refer to yyyy-mm-dd-hh:mm:ss'}, 400
      else:
         t = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
      
      allowed = ['name','latitude','longitude','last_updated','next_departure']
      requested = []
      for req in stop:
         if req not in allowed:
            return {'message':'Invalid request format, please refer to stop model'}, 400
         requested.append(req)

      for r in requested:
         update_query = 'UPDATE stops_table SET '+r+' = ? where stop_id = ?'
         data_tuple = (stop[r],id)
         cur.execute(update_query, data_tuple)
         con.commit()
      
      if 'last_updated' not in stop:
         update_query = '''UPDATE stops_table SET last_updated = ? where stop_id = ?'''
         data_t = (t,id)
         cur.execute(update_query, data_t)
         con.commit()
      
      result = {}
      self = cur.execute('SELECT self FROM stops_table WHERE stop_id = ?',(id,)).fetchone()[0]
      result['stop_id'] = id
      result['last_updated'] = t
      result['_links'] = {'self':{'href':self}}
      return result, 200

@api.route('/operator-profiles/<int:stop_id>')
@api.param('stop_id','id of stop (stop_id)')  
class Profiles(Resource):
   @api.doc(description='retrieve profile information of operators who operate services departing from a stop in DB in the next 90 minutes, (info credit to Gemini API)')
   @api.response(200, 'OK')
   @api.response(404, 'Stop Not Found')
   @api.response(400, 'Query Malformed')
   @api.response(503, 'Service Not Avalaible')
   def get(self, stop_id):
      if stop_id < 0:
         return {'message':'Invalid request format, please enter positive integer'}, 400

      id = stop_id
      cur.execute('SELECT * FROM stops_table WHERE stop_id = ?', (id,))
      selected = cur.fetchone()
      if selected is None:
         api.abort(404, "Stop {} doesn't exist in DB".format(id))

      resp = rq.get('{api_url}stops/{id}/departures?duration=90&results=5'.format(api_url=api_url, id = id))
      if resp.status_code != 200:
         if resp.status_code == 404:
            api.abort(404,'Stop queried does not exist anymore in external DB API.')
         if resp.status_code == 400:
            api.abort(400, 'query is malformed.')
         if resp.status_code == 503:
            api.abort(503, 'Service is not avalaible at the time.')

      data = resp.json()
      operators = []

      if data['departures'] == []:
         api.abort(404, 'No departues in next 90 miniutes')
      else:
         for d in data['departures']:
            if d['line']['operator']['name'] not in operators:
               operators.append(d['line']['operator']['name'])

      result = {}
      result['stop_id'] = id
      all_info = []
      for op in operators:
         question = 'Give me some information about {name} in one paragraph'.format(name = op)
         response = gemini.generate_content(question).text
         all_info.append({'operator_name':op,'information':response})
      
      result['profiles'] = all_info
         
      return result, 200

@api.route('/guide')
class Guide(Resource):
   @api.doc(description='Provide touring information of stored stops in Database, (info credit to Gemini API)')
   @api.response(200, 'OK')
   @api.response(400, 'Query Malformed')
   @api.response(503, 'Service Not Avalaible')
   def get(self):
      query_get_stop_name = 'SELECT name FROM stops_table'
      query_get_departures = 'SELECT next_departure FROM stops_table'
      stop_names = cur.execute(query_get_stop_name).fetchall()
      if len(stop_names) < 2:
         api.abort(404,'Only one stop in Database')
      departures = cur.execute(query_get_departures).fetchall()
      source = ''
      destination = ''
      for nm in stop_names:
         for depart in departures:
            if depart[0]:
               if nm[0] in depart[0]:
                  destination = nm[0]
                  source = stop_names[departures.index(depart)][0]
                  break
      
      if source and destination:
         question_for_source = 'Give me some tour information about {place}'.format(place = source)
         question_for_destination = 'Give me some tour information about {place}'.format(place = destination)
         question_for_extra_experience_inbetween = 'Give me more information about touring from {source} to {destination}'.format(source= source ,destination = destination)

         source_info = gemini.generate_content(question_for_source).text
         destination_info = gemini.generate_content(question_for_destination).text
         extra_info = gemini.generate_content(question_for_extra_experience_inbetween).text

         file = open(txt_file, 'w')

         file.write(source_info)
         file.write('\n')
         file.write(destination_info)
         file.write('\n')
         file.write(extra_info)
         file.close()

      else:
         api.abort(404,'No connection between stops in Database')

      return send_file(txt_file, mimetype='application/txt', as_attachment=True)
      


# Load the environment variables from the .env file
load_dotenv()

# Configure the API key
genai.configure(api_key=os.environ["GOOGLE_API_KEY"])

# Create a Gemini Pro model
gemini = genai.GenerativeModel('gemini-pro')   

if __name__ == "__main__":
   #Here's a quick example of using the Generative AI API:
   #question = "Give me some facts about UNSW!"
   #response = gemini.generate_content(question)
   #print(question)
   #print(response.text)
   #app.run(debug=True)
   con = sqlite3.connect(db_file, check_same_thread=False)
   cur = con.cursor()
   cur.execute('CREATE TABLE IF NOT EXISTS stops_table (stop_id,name,latitude,longitude,last_updated,self,prev,next,next_departure)')
   app.run()