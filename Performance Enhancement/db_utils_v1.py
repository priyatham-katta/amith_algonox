"""
Author: Ashyam Zubair
Created Date: 14-02-2019
"""
import pandas as pd
import traceback
import sqlalchemy
import json

from sqlalchemy import create_engine, exc

class DB(object):
    def __init__(self, database, host='127.0.0.1', user='root', password='', port='3306'):
        """
        Initialization of databse object.

        Args:
            databse (str): The database to connect to.
            host (str): Host IP address. For dockerized app, it is the name of
                the service set in the compose file.
            user (str): Username of MySQL server. (default = 'root')
            password (str): Password of MySQL server. For dockerized app, the
                password is set in the compose file. (default = '')
            port (str): Port number for MySQL. For dockerized app, the port that
                is mapped in the compose file. (default = '3306')
        """
        if host in ["extraction_db","queue_db"]:
            self.HOST = '192.168.0.141'
            self.USER = 'root'
            self.PASSWORD = ''
            self.PORT = '3306'
            self.DATABASE = database
        else:
            self.HOST = host
            self.USER = user
            self.PASSWORD = password
            self.PORT = port
            self.DATABASE = database

        # Create the engine
        try:
            config = f'mysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8'
            self.engine = create_engine(config)
        except:
            traceback.print_exc()
            return

    def execute(self, query, database=None, **kwargs):
        """
        Executes an SQL query.

        Args:
            query (str): The query that needs to be executed.
            database (str): Name of the database to execute the query in. Leave
                it none if you want use database during object creation.
            params (list/tuple/dict): List of parameters to pass to in the query.

        Returns:
            (DataFrame) A pandas dataframe containing the data from the executed
            query. (None if an error occurs)
        """
        data = None

        # Use new database if a new databse is given
        if database is not None:
            try:
                config = f'mysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{database}?charset=utf8'
                engine = create_engine(config)
            except:
                traceback.print_exc()
                return False
        else:
            engine = self.engine

        try:
            data = pd.read_sql(query, engine, index_col='id', **kwargs)
        except exc.ResourceClosedError:
            return True
        except:
            traceback.print_exc()
            params = kwargs['params'] if 'params' in kwargs else None
            return False

        return data.where((pd.notnull(data)), None)

    def insert(self, data, table, database=None, **kwargs):
        """
        Write records stored in a DataFrame to a SQL database.

        Args:
            data (DataFrame): The DataFrame that needs to be write to SQL database.
            table (str): The table in which the rcords should be written to.
            database (str): The database the table lies in. Leave it none if you
                want use database during object creation.
            kwargs: Keyword arguments for pandas to_sql function.
                See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
                to know the arguments that can be passed.

        Returns:
            (bool) True is succesfully inserted, else false.
        """
        # Use new database if a new databse is given
        if database is not None:
            try:
                config = f'mysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{database}?charset=utf8'
                engine = create_engine(config)
            except:
                traceback.print_exc()
                return False
        else:
            engine = self.engine

        try:
            data.to_sql(table, engine, **kwargs)
            try:
                self.execute(f'ALTER TABLE `{table}` ADD PRIMARY KEY (`id`);')
            except:
                pass
            return True
        except:
            traceback.print_exc()
            return False

    def update(self, table, update=None, where=None, database=None, force_update=False):
        # Use new database if a new databse is given
        if database is not None:
            try:
                config = f'mysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{database}?charset=utf8'
                self.engine = create_engine(config)
            except:
                traceback.print_exc()
                return False

        try:
            set_clause = []
            set_value_list = []
            where_clause = []
            where_value_list = []

            if where is not None and where:
                for set_column, set_value in update.items():
                    set_clause.append(f'`{set_column}`=%s')
                    set_value_list.append(set_value)
                set_clause_string = ', '.join(set_clause)
            else:
                print(f'ERROR: Update dictionary is None/empty. Must have some update clause.')
                return False

            if where is not None and where:
                for where_column, where_value in where.items():
                    where_clause.append(f'{where_column}=%s')
                    where_value_list.append(where_value)
                where_clause_string = ' AND '.join(where_clause)
                query = f'UPDATE `{table}` SET {set_clause_string} WHERE {where_clause_string}'
            else:
                if force_update:
                    query = f'UPDATE `{table}` SET {set_clause_string}'
                else:
                    message = 'Where dictionary is None/empty. If you want to force update every row, pass force_update as True.'
                    print(f'ERROR: {message}')
                    return False

            params = set_value_list + where_value_list
            self.execute(query, params=params)
            return True
        except:
            traceback.print_exc()
            return False

    def get_column_names(self, table, database=None):
        """
        Get all column names from an SQL table.

        Args:
            table (str): Name of the table from which column names should be extracted.
            database (str): Name of the database in which the table lies. Leave
                it none if you want use database during object creation.

        Returns:
            (list) List of headers. (None if an error occurs)
        """
        try:
            return list(self.execute(f'SELECT * FROM `{table}`', database))
        except:
            return

    def execute_default_index(self, query, database=None, **kwargs):
        """
        Executes an SQL query.

        Args:
            query (str): The query that needs to be executed.
            database (str): Name of the database to execute the query in. Leave
                it none if you want use database during object creation.
            params (list/tuple/dict): List of parameters to pass to in the query.

        Returns:
            (DataFrame) A pandas dataframe containing the data from the executed
            query. (None if an error occurs)
        """
        data = None

        # Use new database if a new databse is given
        if database is not None:
            try:
                config = f'mysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{database}?charset=utf8'
                engine = create_engine(config)
            except:
                traceback.print_exc()
                return False
        else:
            engine = self.engine

        try:
            data = pd.read_sql(query, engine, **kwargs)
        except exc.ResourceClosedError:
            return True
        except:
            traceback.print_exc()
            params = kwargs['params'] if 'params' in kwargs else None
            return False

        return data.where((pd.notnull(data)), None)


    def get_all(self, table, database=None, discard=None):
        """
        Get all data from an SQL table.

        Args:
            table (str): Name of the table from which data should be extracted.
            database (str): Name of the database in which the table lies. Leave
                it none if you want use database during object creation.
            discard (list): columns to be excluded while selecting all
        Returns:
            (DataFrame) A pandas dataframe containing the data. (None if an error
            occurs)
        """
        if discard:
            columns = list(self.execute_default_index(f'SHOW COLUMNS FROM `{table}`',database).Field)
            columns = [col for col in columns if col not in discard]
            columns_str = json.dumps(columns).replace("'",'`').replace('"','`')[1:-1]
            return self.execute(f'SELECT {columns_str} FROM `{table}`', database)

        return self.execute(f'SELECT * FROM `{table}`', database)
    
    def get_latest(self, data, group_by_col, sort_col):
        """
        Group data by a column containing repeated values and get latest from it by
        taking the latest value based on another column.

        Example:
        Get the latest products
            id     product   date
            220    6647     2014-09-01
            220    6647     2014-10-16
            826    3380     2014-11-11
            826    3380     2015-05-19
            901    4555     2014-09-01
            901    4555     2014-11-01

        The function will return
            id     product   date
            220    6647     2014-10-16
            826    3380     2015-05-19
            901    4555     2014-11-01

        Args:
            data (DataFrame): Pandas DataFrame to query on.
            group_by_col (str): Column containing repeated values.
            sort_col (str): Column to identify the latest record.

        Returns:
            (DataFrame) Contains the latest records. (None if an error occurs)
        """
        try:
            return data.sort_values(sort_col).groupby(group_by_col).tail(1)
        except KeyError as e:
            print(f'ERROR: Column `{e.args[0]}` does not exist.')
            return None
        except:
            traceback.print_exc()
            return None
