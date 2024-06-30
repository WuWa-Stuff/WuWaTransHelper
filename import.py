import csv
import os.path
import shutil
import sqlite3
import sys

dbs_orig_dir = os.environ['DIR_ORIGINAL']
dbs_dir = os.environ['DIR_TRANSLATED']
csv_out = os.environ['FILE_CSV']

last_file = None
sql_con = None
sql_cur = None


if os.environ['UPDATE_MODE'] == '1':
    for file in os.listdir(dbs_dir):
        os.remove(os.path.join(dbs_dir, file))

    for file in os.listdir(dbs_orig_dir):
        shutil.copy(os.path.join(dbs_orig_dir, file), os.path.join(dbs_dir, file))


try:
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

            value_to_use = ru_value
            if value_to_use is None or len(value_to_use) == 0:
                value_to_use = en_value

            sql_cur.execute(f"UPDATE '{table_name}' SET Content = ? WHERE Id = ?;", (value_to_use, r_id))
except Exception as e:
    print(f'Something went wrong:\n{e}')
    sys.exit(os.EX_SOFTWARE)

if sql_con is not None:
    sql_con.commit()
    sql_cur.close()
    sql_con.close()
