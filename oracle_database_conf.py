import cx_Oracle

USER='scott'
PASS='tiger'
HOST='localhost'
PORT='1521'
SERVICE_NAME='ORCL'
orcl_inst_path = r"instantclient_21_6"  # download instantclient_21_6 from google and paste this folder here

def oracle_connect():
    cx_Oracle.init_oracle_client(lib_dir=orcl_inst_path)
    con = cx_Oracle.connect(USER,PASS,HOST+':'+PORT+'/'+SERVICE_NAME)
    cur = con.cursor()
    print("database connected..")
    return cur,con                              #get like cur,con=oracle_connect()