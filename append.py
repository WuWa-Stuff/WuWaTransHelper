import csv
import os
import sqlite3

dbs_dir_orig = os.environ['DIR_ORIGINAL']
csv_path = os.environ['FILE_CSV']
file_new_temp = csv_path + ".db"

if os.path.exists(file_new_temp):
    os.remove(file_new_temp)

print("Init...")
with sqlite3.connect(file_new_temp) as sql_con_tmp:
    sql_cur_tmp = sql_con_tmp.cursor()
    sql_cur_tmp.execute("CREATE TABLE tmp(db_file, db_table, db_id, db_text_orig, db_text_ru)")
    sql_con_tmp.commit()

    print("Reading csv...")
    with open(csv_path, 'r', encoding='utf-8') as old_csv_file_content:
        old_csv_file = csv.reader(old_csv_file_content)
        for csv_db, csv_db_table, csv_db_id, csv_text_orig, csv_text_ru in old_csv_file:
            sql_cur_tmp.execute("INSERT INTO 'tmp' VALUES (?, ?, ?, ?, ?)",
                                (csv_db, csv_db_table, csv_db_id, csv_text_orig, csv_text_ru))
    sql_con_tmp.commit()

    with open(csv_path + ".tmp", 'w', encoding='utf-8', newline='') as csv_file_content:
        csv_file = csv.writer(csv_file_content)
        for file in os.listdir(dbs_dir_orig):
            full_file_path_orig = os.path.join(dbs_dir_orig, file)
            print(f'Processing {file}...')
            with sqlite3.connect(full_file_path_orig) as sql_con_orig:
                sql_cur_orig = sql_con_orig.cursor()

            sql_cur_orig.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = sql_cur_orig.fetchall()
            for table_name_ex in tables:
                table_name = table_name_ex[0]
                print(f'Processing table "{table_name}"...')
                sql_cur_orig.execute(f"SELECT Id,Content FROM '{table_name}';")
                data = sql_cur_orig.fetchall()
                for r_id, content in data:
                    sql_cur_tmp.execute(
                        "SELECT db_file,db_table,db_id,db_text_orig,db_text_ru"
                        " FROM 'tmp'"
                        " WHERE db_file = ? AND db_table = ? AND db_id = ?",
                        (str(file), str(table_name), str(r_id)))
                    result = sql_cur_tmp.fetchone()

                    if result is None:
                        sql_cur_tmp.execute("INSERT INTO 'tmp' VALUES (?, ?, ?, ?, ?)",
                                            (file, table_name, r_id, content, ""))
                        continue

                    db_file = result[0]
                    db_table = result[1]
                    db_id = result[2]
                    db_text_orig = result[3]

                    if content is None:
                        content = ''

                    if content == db_text_orig:
                        continue

                    if (content.replace('\r', '')) == (content.replace('\r', '')):
                        continue

                    sql_cur_tmp.execute("UPDATE 'tmp'"
                                        " SET db_text_orig = ? db_text_ru = ''"
                                        " WHERE db_file = ? AND db_table = ? AND db_id = ?",
                                        (content, str(db_file), str(db_table), str(db_id)))

        print("Saving to csv init...")
        sql_con_tmp.commit()
        sql_cur_tmp.execute("SELECT db_file,db_table,db_id,db_text_orig,db_text_ru FROM 'tmp'")
        to_dump_back = sql_cur_tmp.fetchall()
        print("Saving to csv...")
        for db_file, db_table, db_id, db_text_orig, db_text_ru in to_dump_back:
            csv_file.writerow((db_file, db_table, db_id, db_text_orig, db_text_ru))

print("Done")
