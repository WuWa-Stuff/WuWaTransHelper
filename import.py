import csv
import os.path
import sqlite3

dbs_dir = os.environ['DIR_TRANSLATED']
csv_out = os.environ['FILE_CSV']

last_file = None
sql_con = None
sql_cur = None
with open(csv_out, 'r', encoding='utf-8') as csv_file_content:
    csv_file = csv.reader(csv_file_content)
    for file, table_name, r_id, en_value, ru_value in csv_file:
        if last_file != file:
            print(f'Processing {file}')
            last_file = file
            if sql_con is not None:
                sql_con.commit()
                sql_cur.close()
                sql_con.close()

            full_file_path = os.path.join(dbs_dir, file)
            sql_con = sqlite3.connect(full_file_path)
            sql_cur = sql_con.cursor()

        sql_cur.execute(f"UPDATE '{table_name}' SET Content = ? WHERE Id = ?;", (ru_value, r_id))

if sql_con is not None:
    sql_con.commit()
    sql_cur.close()
    sql_con.close()