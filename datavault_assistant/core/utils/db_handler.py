# core/utils/db_handler.py
import psycopg2
from psycopg2.extras import execute_values
from typing import Dict, Any, List, Optional
import pandas as pd
from contextlib import contextmanager
from datavault_assistant.core.utils.log_handler import create_logger
import logging 
logger = create_logger(__name__,'db_handler.log',level=logging.DEBUG)


class DatabaseHandler:
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self._conn = None
        
    @property
    def conn(self):
        if self._conn is None:
            self._conn = psycopg2.connect(**self.db_config)
        return self._conn

    @contextmanager
    def cursor(self):
        cursor = self.conn.cursor()
        try:
            yield cursor
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cursor.close()


    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[List[tuple]]:
        """Execute a query and return results if any
        
        Args:
            query (str): SQL query to execute
            params (tuple, optional): Query parameters
            
        Returns:
            Optional[List[tuple]]: Query results for SELECT or RETURNING queries
        """
        with self.cursor() as cur:
            try:
                cur.execute(query, params)
                result = cur.fetchall()
                return result if result else None
            except Exception as e:
                logger.error(f"Query execution error: {str(e)}")
                raise

    def execute_many(self, query: str, data: List[tuple]) -> None:
        """Execute a batch INSERT or UPDATE query with multiple rows of data
        
        Args:
            query (str): SQL query template with %s placeholders
            data (List[tuple]): List of parameter tuples to insert
        """
        with self.cursor() as cur:
            try:
                # Tạo template cho execute_values
                template = query.split('VALUES')[0] + ' VALUES %s'
                if 'ON CONFLICT' in query:
                    template += query.split('VALUES')[1].split('%s')[1]
                
                # Execute với proper template và data
                execute_values(
                    cur,
                    template,
                    data,
                    template=None, # Use template from query
                    page_size=100   # Batch size 
                )
            except Exception as e:
                logger.error(f"Batch execution error: {str(e)}")
                raise

    def query_to_df(self, query: str, params: tuple = None) -> pd.DataFrame:
        return pd.read_sql_query(query, self.conn, params=params)

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None