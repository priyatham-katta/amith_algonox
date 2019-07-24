#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 11:37:38 2019

@author: Amith
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
from app.stats_db import Stats_db

from app import app

@app.route("/get_stats_cards", methods = ['POST', 'GET'])
def get_stats():
    try:
        data = request.json
        from_date = data['fromDate']
        to_date = data['toDate']
    except Exception as e:
        print("Unexpected request data", e)
        return "Unexpected request data" 
    stats_db_obj = Stats_db()
    try:
        active_stats_dict = stats_db_obj.active_stats() #List of dictionaries - one for each card
    except Exception as e:
        print("Stats DB error", e)
        return "Failed - Unable to connect to the database stats, Please check configuration in stats_db.py"
    return jsonify({"data" : active_stats_dict})


#Dummy Routes
@app.route("/flip", methods = ['POST', 'GET'])
def dummy1():
    return jsonify({"before_flip": {"message": "Flip to see other details", "data" : "14" }, 
                    "after_flip": {"message" : "Flip to see initial details", "data": "28"}})

@app.route("/normal_card", methods = ['POST', 'GET'])
def dummy2():
    return jsonify({"data" : {"name" : "Invoices", "value" : "200"}})

@app.route("/dummy3", methods = ['POST', 'GET'])
def dummy3():
    return jsonify({"data" : {"chart_data" : [['Template Training', 777, '#b87333'],
                                 ['TL Verify', 333, 'silver'],
                                 ['Verify', 500, 'gold'],
                                 ['Rejected', 89, 'red']],
                    "columns" : ['Queue Name', 'No. of Invoices', { "role": 'style' }]}})