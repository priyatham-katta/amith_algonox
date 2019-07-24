#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 11:40:37 2019

@author: user
"""

from sqlalchemy import create_engine, exc
import pandas as pd
from sqlalchemy.orm import sessionmaker

class Stats_db:
    def __init__(self, database = 'stats', host='127.0.0.1', user='root', password='', port='3306'):
        config = f'mysql://{user}:{password}@{host}:{port}/{database}?charset=utf8'
        try:
            self.engine = create_engine(config, 
                                 pool_size=10, 
                                 max_overflow=20, echo = True)
            try:
                self.connection = self.engine.connect()
            except Exception as e:
                print("Unable to connect to Database", e)                
        except Exception as e:
            print("Unable to create engine", e)
                
    def get_stats_master(self):
        result_proxy = self.connection.execute(f'SELECT * FROM `stats_master`')
        d, a = {}, []
        for row in result_proxy:
            for column, value in row.items():
                d = {**d, **{column : value}}
            a.append(d)
        stats_master_df = pd.DataFrame(a)
        return stats_master_df
    
    def get_active_stats(self):
        result_proxy = self.connection.execute(f'SELECT * FROM `active_stats`')
        d, a = {}, []
        for row in result_proxy:
            for column, value in row.items():
                d = {**d, **{column : value}}
            a.append(d)
        active_stats_df = pd.DataFrame(a)
        return active_stats_df
    
    def test(self):
        result_proxy = self.connection.execute(f'SELECT * FROM `asdas`')
        d, a = {}, []
        for row in result_proxy:
            for column, value in row.items():
                d = {**d, **{column : value}}
            a.append(d)
        active_stats_df = pd.DataFrame(a)
        return active_stats_df
    
    def active_stats(self):
        stats_master_df = self.get_stats_master()
        active_stats_df = self.get_active_stats()
        return pd.merge(stats_master_df, active_stats_df, on = 'id', how = 'inner').to_dict(orient = 'records')


#
#try:
#    a = Stats_db()
#    a.test()
#except exc.ProgrammingError as e:
#    print(123, e)
#finally:
#    del a
#    print(456)
        
#a = Stats_db()
#
#from sqlalchemy import MetaData, Table
##metadata = MetaData()
##print(metadata.tables)
#
## reflect db schema to MetaData
#metadata.reflect(bind=a.engine)
#print(metadata.tables)
#tables = []
#for i in list(metadata.tables.keys()):
#    tables.append(metadata.tables[i])
#active_stats_table = metadata.tables['stats_master']
#ins = active_stats_table.insert().values( header = "META3")
#rp = a.engine.execute(ins)
#
#select_st = active_stats_table.select().where(active_stats_table.c.id == 1)
#res = a.connection.execute(select_st)
#for _row in res:
#    print(_row)