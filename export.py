import csv
import os
import sqlite3


dbs_dir = os.environ['DIR_TRANSLATED']
dbs_dir_orig = os.environ['DIR_ORIGINAL']
csv_out = os.environ['FILE_CSV']

if os.path.exists(csv_out):
    os.remove(csv_out)

with open(csv_out, 'w', encoding='utf-8', newline='') as csv_file_content:
    csv_file = csv.writer(csv_file_content)
    for file in os.listdir(dbs_dir):
        full_file_path = os.path.join(dbs_dir, file)
        full_file_path_orig = os.path.join(dbs_dir_orig, file)
        print(f'Processing {file}...')
        with sqlite3.connect(full_file_path) as sql_con:
            with sqlite3.connect(full_file_path_orig) as sql_con_orig:
                sql_cur = sql_con.cursor()
                sql_cur_orig = sql_con_orig.cursor()

                sql_cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = sql_cur.fetchall()
                for table_name_ex in tables:
                    table_name = table_name_ex[0]
                    print(f'Processing table "{table_name}"...')
                    sql_cur.execute(f"SELECT Id,Content FROM '{table_name}';")
                    data = sql_cur.fetchall()
                    for r_id, content in data:
                        sql_cur_orig.execute(f"SELECT Content FROM '{table_name}' where Id = '{r_id}';")
                        orig_values = sql_cur_orig.fetchall()
                        orig_value = ""
                        if len(orig_values) > 0 and len(orig_values[0]) > 0:
                            orig_value = orig_values[0][0]

                        csv_file.writerow((file, table_name, r_id, orig_value, content))
