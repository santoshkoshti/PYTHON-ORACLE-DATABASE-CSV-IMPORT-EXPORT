import numpy as np
import pandas as pd
import re
import cx_Oracle
from datetime import datetime
from dateutil import parser
from oracle_database_conf import oracle_connect

csv_file_path = r"D:\employees.csv"     # MENTION CSV FILE PATH
database_name = "SANTU"                 # mention table name creates

def extract_file_data(path,database_name,cur,con):
    data = pd.read_csv(path)
    data = data.replace(to_replace=np.nan, value='-')
    df = pd.DataFrame(data)
    csv_data_insert_oracle(database_name, df,cur,con)

def get_columns_csv(df):
    query = []
    for name, dtype in df.dtypes.iteritems():
        try:
            varch_len = df[name].apply(len).max()
        except:
            varch_len = 100
        col = re.sub('[-,.\'~`() "?!@#$%+|^&*<>/{}[\]]', '', name)
        if object == dtype:
            if 'date' in str(col).lower():
                db_col = col + ' DATE'
                query.append(db_col)
            else:
                db_col = col + ' VARCHAR2({})'.format(varch_len)
                query.append(db_col)
        else:
            db_col = col + ' NUMBER(20)'
            query.append(db_col)
    return query

def create_table_oracle(tb_name,query,cur):
    make_query = 'CREATE TABLE {}('.format(tb_name) + ",".join(query) + ')'
    try:
        cur.execute(make_query)
        return "table {} created".format(tb_name)
    except cx_Oracle.DatabaseError as er:
        if 'ORA-00955' in str(er):
            return 'DataBase Already exits..'
        else:
            return "something wrong in query or connection"


def is_date_matching(dt_date):
    if dt_date:
        try:
            int(dt_date)
            return False
        except:
            try:
                parser.parse((dt_date))
                return True
            except:
                return False
    return False

def csv_data_insert_oracle(db_name,df,cur,con):
    op_create_table_oracle = create_table_oracle(db_name, get_columns_csv(df), cur)  # replace oracle table name in place of santu
    print(op_create_table_oracle)
    count = 1
    for row in df.itertuples():
        query1 =''
        for i in row:
            if i=='-':
                query1 +=',null'
            elif type(i)==int:
                query1 += ',{}'.format(i)
            elif type(i)==float:
                query1 += ',{}'.format(i)
            elif is_date_matching(i)==True:
                if ":" in i:
                    query1 += ",'{}'".format(i)
                elif any(num.isdigit() for num in i)==False:
                    query1 += ",'{}'".format(i)
                else:
                    h = parser.parse(i)
                    i = datetime.strptime(str(h),'%Y-%m-%d  %H:%M:%S').strftime('%m/%d/%Y')
                    query1 += ",to_date('{}','mm-dd-yy')".format(i)
            else:
                i= re.sub('[^A-Za-z0-9@./&]+', '', i)
                i = i.replace("'","")
                query1 += ",'{}'".format(i)
        query1= 'INSERT INTO {} VALUES('.format(db_name) + query1.split(',', 2)[-1]+')'
        cur.execute(query1)
        print("{} rows inserted".format(count))
        count += 1
    con.commit()
    con.close()
    print("all data loaded..")


if __name__ == '__main__':
    cur, con = oracle_connect()
    extract_file_data(csv_file_path,database_name,cur,con)