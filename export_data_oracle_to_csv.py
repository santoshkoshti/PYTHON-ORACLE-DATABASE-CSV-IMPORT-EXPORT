import csv
from oracle_database_conf import oracle_connect

def get_all_tables(cur):
    query = 'SELECT table_name FROM user_tables'
    get_tables = cur.execute(query)
    if get_tables.rowcount == 0:
        print("no tables found in {} database".format(db_name))
    else:
        for i in get_tables.fetchall():
            oracle_to_csv(cur,i[0])

def oracle_to_csv(cur,table_name):
    with open("{}.csv".format(table_name), "w", encoding='utf-8') as outputfile:
        writer = csv.writer(outputfile, lineterminator="\n")
        results = cur.execute('SELECT * FROM {}'.format(table_name))
        writer.writerow(i[0] for i in results.description)
        writer.writerows(results)
        print("{}.csv file created..".format(table_name))

if __name__ == '__main__':
    cur, con = oracle_connect()
    get_all_tables(cur)