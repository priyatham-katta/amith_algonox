#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 11:37:38 2019

@author: Amith
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
from stats_db import Stats_db


app = Flask(__name__)
CORS(app)

def close_db_object(stats_db_obj):
    stats_db_obj.connection.close()
    stats_db_obj.engine.dispose()
    

@app.route("/get_stats", methods = ['POST', 'GET'])
def get_stats():
    data = request.json
    from_date = data['fromDate']
    to_date = data['toDate']
    stats_db_obj = Stats_db()
    active_stats_dict = stats_db_obj.active_stats() #List of dictionaries - one for each card
    close_db_object(stats_db_obj)
    return jsonify(active_stats_dict)

#Dummy Routes
@app.route("/flip", methods = ['POST', 'GET'])
def dummy1():
    return jsonify({"before_flip": 
                        {"message": "Flip to see other details", 
                         "data" : "14" }, 
                    "after_flip": 
                        {"message" : "Flip to see initial details", 
                         "data": "28"}})

@app.route("/normal_card", methods = ['POST', 'GET'])
def dummy2():
    return jsonify({"data" : "100"})

@app.route("/dummy3", methods = ['POST', 'GET'])
def dummy3():
    return jsonify({"data" : [['Template Training', 777, '#b87333'],
                                 ['TL Verify', 333, 'silver'],
                                 ['Verify', 500, 'gold'],
                                 ['Rejected', 89, 'red']],
                    "columns" : ['Queue Name', 'No. of Invoices', { "role": 'style' }]})

app.run()