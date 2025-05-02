# db_manager.py
import sqlite3
import os
import logging
import json

logger = logging.getLogger(__name__)
DESENSITIZATION_DB_PATH = "./data_desens.db"

class dataDesensManager:
    def __init__(self, db_path=DESENSITIZATION_DB_PATH):
        self.db_path = db_path
        self._create_table()

    def _get_connection(self):
        """获取数据库连接"""
        # check_same_thread=False 是为了支持多线程访问，但在多线程环境下最好为每个线程创建独立的连接
        return sqlite3.connect(self.db_path)

    def _create_table(self):
        """创建存储已索引文件信息的表"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS data_desens (
                    hospital_id TEXT PRIMARY KEY,
                    contexts TEXT CHECK(json_valid(contexts)) NOT NULL
                );
            ''')
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error creating database table: {e}")
        finally:
            if conn:
                conn.close()

    def addDesensData(self, hospital_id,context):
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT hospital_id FROM data_desens WHERE hospital_id = ?", (hospital_id,))
            row = cursor.fetchone()

            if row is None:
                cursor.execute("INSERT INTO data_desens (hospital_id, contexts) VALUES (?, ?)", (hospital_id, context))
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Database error {e}")

        finally:
            if conn:
                conn.close()
    
    def updateDesensData(self, hospital_id,context):
        conn = None
        try: 
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("UPDATE data_desens SET contexts = ? WHERE hospital_id = ?", (context, hospital_id))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Database error {e}")
        finally:
            if conn:
                conn.close()

    def IsChecked(self, hospital_id):
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT hospital_id FROM data_desens WHERE hospital_id = ?", (hospital_id,))
            row = cursor.fetchone()
            if row is None:
                return False
            return True
        except sqlite3.Error as e:
            logger.error(f"Database error {e}")

        finally:
            if conn:
                conn.close()
    def getDesensData(self, hospital_id):
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            sql_statement = f"SELECT contexts FROM data_desens WHERE hospital_id = '{hospital_id}'" # 使用SELECT语句
            logger.info(sql_statement)
            cursor.execute(sql_statement)
            row = cursor.fetchone()
            if row is None:
                return None
            return row[0]
        except sqlite3.Error as e:
            logger.error(f"Database error {e}")
        finally:
            if conn:
                conn.close()
    def getDesensDataDict(self,hospital_id):
        desensdata_str = self.getDesensData(hospital_id)
        if desensdata_str is None:
            return None
        try:
            desens_data = json.loads(desensdata_str)
            desens_data = {key: value for key, value in desens_data.items() if value != '' and  key != 'age'}
            return desens_data
        except (json.JSONDecodeError, TypeError):
            print(f"错误: 无法解析 desens_data 字符串为 JSON - {desensdata_str[:100]}...")
            return None
    def deleteDesensData(self, hospital_id):
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM data_desens WHERE hospital_id = ?", (hospital_id,))
            conn.commit()
            return True 
        except sqlite3.Error as e:
            logger.error(f"Database error {e}")
        finally:
            if conn:
                conn.close()
    
    def getAllHospitalId(self):
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT hospital_id FROM data_desens")
            rows = cursor.fetchall()
            return [row[0] for row in rows]
        except sqlite3.Error as e:
            logger.error(f"Database error {e}")
        finally:
            if conn:
                conn.close()