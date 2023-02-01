from typing import Any
from sqlalchemy.engine import Engine
import psycopg2
from sqlalchemy import create_engine
import os
import warnings

import sys
import inspect
import time
import traceback

from sqlalchemy import event
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# logging
import logging
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
    )
logger = logging.getLogger()
class QueryStats(object):
    def __init__(self):
        self.count = 0
        self.total_time = 0.0

        self._query_log = []
        self.query_info = None
        print('QueryStats INIT')

    def start_query(self, statement, parameters):
        if self.query_info is not None:
            self._query_log.append( self.query_info + (float('nan'),) )

        stack = extract_user_stack(1)

        self.query_info = (time.time(), statement, parameters, stack)
        self.count += 1

    def end_query(self):
        assert self.query_info is not None

        query_end = time.time()
        self.total_time += query_end - self.query_info[0]
        self._query_log.append( self.query_info + (query_end - self.query_info[0],) )

        self.query_info = None

    @property
    def query_log(self):
        if self.query_info is not None:
            return self._query_log + [ self.query_info + (float('nan'),) ]
        return self._query_log

    def __repr__(self):
        return '<QueryStats count=%d time=%.2fs>' % (self.count, self.total_time)

class DatabaseBackend:
    def __init__(self,*args,**kwargs):
        self.interface_name = 'iface'
        self.expected_connection_args = [
                'host',
                'database',
                'user',
                'password',
                'port'
            ]
        self.database_module = psycopg2
        self.alchemy_engine_flag = 'psycopg2'
        self.driver_signature = ''
        self.user:str = ''
        self.database:str = ''
        self.password:str = ''
        self.host:str = ''
        self.port:str = None
        self._parse_connection_args(kwargs)
        self.count = 0
        self.total_time = 0.0

        self.session = None
        self.engine = None
        self.execution_metrics = {
            'inserted_rows':0,
            'updated_rows':0,
            'deleted_rows':0,
            'processed_rows':0,
            }
        #self.set_engine()

    def __str__(self) -> str:
        return str(f'Interface Connection [{self.interface_name}]')

    def _parse_connection_args(self,arguments:dict):
        for key in self.expected_connection_args:
            if not key in arguments:
                raise ValueError(f'Expected argument not found in connection arguments: {key}')
            else:
                self.__setattr__(key,arguments[key])
    
    @staticmethod
    def _sql_type_name(col_type):

        from pandas.io.sql import _SQL_TYPES

        if col_type == 'timedelta64':
            warnings.warn("the 'timedelta' type is not supported, and will be "
                          "written as integer values (ns frequency) to the "
                          "database.", UserWarning, stacklevel=8)
            col_type = "integer"

        elif col_type == "datetime64":
            col_type = "datetime"

        elif col_type == "empty":
            col_type = "string"

        elif col_type == "complex":
            raise ValueError('Complex datatypes not supported')

        if col_type not in _SQL_TYPES:
            col_type = "string"

        return _SQL_TYPES[col_type]

    @property
    def uri(self):
        s = str(f'{self.alchemy_engine_flag}://{self.user}:{self.password}@{self.host}/{self.database}')
        if self.port:
            s = f'{self.alchemy_engine_flag}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        #print('returning ',s)
        return s
    
    def get_connection(self,*args,**kwargs)->Any:
        if isinstance(self.engine,Engine):
            return self.engine.connect()
        return self.database_module.connect(
            dbname=self.database, 
            host=self.host, 
            port=self.port, 
            user=self.user, 
            password=self.password)
    
    def set_engine(self,*args,**kwargs)->Engine:
        engine = create_engine(self.uri,*args,**kwargs)
        session = sessionmaker(bind=engine)()
        setup_session_tracking(session)
        self.engine = engine
        self.session = session


    def get_engine(self,*args,**kwargs)->Engine:
        if isinstance(self.engine,Engine):
            return self.engine
        self.engine = create_engine(self.uri,connect_args={'sslmode': 'prefer'},*args,**kwargs)
        self.session = sessionmaker(bind=self.engine)()
        setup_session_tracking(self.session)
        return self.engine

    def record_exists(self,*args,**kwargs)->bool:
        pass

    def collect_metrics(self):
        raise Exception('Your connector does not implement metrics collection system.')
    

    def insert_on_conflict(
        self, 
        active_connection,
        df, 
        schema,
        table_name,
        if_exists='append',
        conflict_key=None,
        conflict_action=None):
        """
        Interface method.

        Every lib should implement this method in order to execute insert
        statement with conflict option.
        """
        logger.critical(f'your database lib does not implement insert_on_conflict method')
    
    def val_record_exists(self,p_cur, p_tab, p_pk_cols, p_pks):
        """checks if the record already exists in the target table / member."""
        p_cur.execute("select  count(1)  FROM "+p_tab+"  where (" + p_pk_cols + ") =  (" + p_pks + ")")
        
        query_result = p_cur.fetchone()
        query_result = query_result[0]

        if query_result != 0:
            result =  True
        else:
            result = False

        return result

    
    def update_single_row(
            self,
            p_cursor, 
            p_table,
            p_values, 
            p_pk_cols, 
            p_pks,p_row):
        """
        Update single row

        params:
            p_cursor: sqlalchemy connection cursor
            p_table: string table name
            p_values: list
            p_pk_cols: str
            p_pks: str
            p_row: Pandas dataframe row
        """
        columns_names_values = p_row.index.values.tolist()
        columns_names_values = ','.join(str(e)+'= %s' for e in columns_names_values)
        
        sql_upd = ("UPDATE " + p_table +
                    " SET " + columns_names_values +
                " WHERE (" + p_pk_cols + ") = (" + p_pks +")")
        
        p_cursor.execute(sql_upd, p_values)
        self.execution_metrics['updated_rows'] += p_cursor.row_count
        

    def insert_single_row(self,p_cursor, p_table, p_values):
        """
        Insert a single row on the database.
        """
        n_values = '%s' + ''.join([','+'%s']*(len(p_values)-1))

        sql_ins = "INSERT INTO " + p_table + " VALUES (" + n_values + ")"
        
        p_cursor.execute(sql_ins, p_values)
        self.execution_metrics['inserted_rows'] += p_cursor.row_count


@event.listens_for(Engine, 'before_cursor_execute')
def on_query_start(conn, cursor, statement, parameters, context, executemany):
    if hasattr(conn, '_query_stats'):
        conn._query_stats.start_query(statement, parameters)

@event.listens_for(Engine, 'after_cursor_execute')
def on_query_end(conn, cursor, statement, parameters, context, executemany):
    if hasattr(conn, '_query_stats'):
        conn._query_stats.end_query()

def setup_session_tracking(session):
    """ Enables the stats tracking for a Session or a Session factory """
    @event.listens_for(session, 'after_begin')
    def on_begin(session, transaction, connection):
        if not hasattr(session, '_query_stats'):
            session._query_stats = QueryStats()

        connection._query_stats = session._query_stats

    @event.listens_for(session, 'after_commit')
    @event.listens_for(session, 'after_rollback')
    def on_commit(session):
        print("Done %s" % session._query_stats)

def extract_user_stack(skip=1):
    """Like traceback.extract_stack, but skips any sqlalchemy 
    modules and only returns filenames """
    try:
        raise ZeroDivisionError
    except ZeroDivisionError:
        f = sys.exc_info()[2].tb_frame.f_back 
    list = []

    while f is not None and skip > 0:
        f = f.f_back
        skip -= 1

    skipping = True
    while f is not None:
        lineno = f.f_lineno
        co = f.f_code
        module = inspect.getmodule(co)
        if skipping and not module.__name__.startswith('sqlalchemy'):
            skipping = False
        if not skipping:
            name = co.co_name  
            list.append((module.__name__, name, lineno))
        f = f.f_back
    list.reverse()
    return list