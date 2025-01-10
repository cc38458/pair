import pandas as pd
import sqlite3
import random
import math


###########################################建立資料庫###########################################
excel_file = 'data.xlsx'  # Excel 檔案
df = pd.read_excel(excel_file, engine='openpyxl')

conn = sqlite3.connect('data.db')  
cursor = conn.cursor()


table_name = 'my_table'  # 資料表名稱
df.to_sql(table_name, conn, if_exists='replace', index=True)
cursor.execute("ALTER TABLE my_table ADD COLUMN weight INTEGER DEFAULT 0")
cursor.execute("ALTER TABLE my_table ADD COLUMN mate INTEGER DEFAULT 0")
cursor.execute("ALTER TABLE my_table ADD COLUMN mate_results STRING DEFAULT 0")
conn.commit()

###########################################輸入參數###########################################

cursor.execute("SELECT * FROM my_table WHERE  condition1 == ?", (1,))
manNum = len(cursor.fetchall())
cursor.execute("SELECT * FROM my_table WHERE  condition1 == ?", (2,))
womanNum = len(cursor.fetchall())

print("總人數",manNum+womanNum)
print("男女比例:",manNum,":",womanNum)
print("一個女生配對男生數:",manNum/womanNum)

while True:
    womanMate = int(input("女生最多配對數量 : "))
    if womanMate>(manNum/womanNum) and womanMate<20: 
        break
    else:
        print("輸入無效 請輸入有效值")
while True:
    manMate = int(input("男生配對數量 : "))
    if manMate>=1 and manMate*manNum<womanNum*womanMate: 
        break
    else:
        print("輸入無效 請輸入有效值")

###########################################資料前處裡###########################################

cursor.execute("SELECT id, condition1 FROM my_table")
rows = cursor.fetchall()

for row in rows:
    record_id, condition1 = row
    new_mate = manMate if condition1 == 1 else womanMate 
    cursor.execute("UPDATE my_table SET mate = ? WHERE id = ?", (new_mate, record_id))
conn.commit()

###########################################設定權重###########################################
quantity1 = [0,0,0]
quantity2 = [0,0,0,0]
quantity3 = [0,0,0,0,0,0]

for i in range(0,3):
    cursor.execute("SELECT * FROM my_table WHERE  hope1 == ?", (i,))
    quantity1[i] = len(cursor.fetchall())
sorted_indices = sorted(range(len(quantity1)), key=lambda i: quantity1[i], reverse=True)
weight1 = [0] * len(quantity1)
for rank, original_index in enumerate(sorted_indices, start=1):
    weight1[original_index] = rank

for i in range(0,4):
    cursor.execute("SELECT * FROM my_table WHERE  hope2 == ?", (i,))
    quantity2[i] = len(cursor.fetchall())
sorted_indices = sorted(range(len(quantity2)), key=lambda i: quantity2[i], reverse=True)
weight2 = [0] * len(quantity2)
for rank, original_index in enumerate(sorted_indices, start=1):
    weight2[original_index] = rank

for i in range(0,6):
    cursor.execute("SELECT * FROM my_table WHERE  hope3 == ?", (i,))
    quantity3[i] = len(cursor.fetchall())
sorted_indices = sorted(range(len(quantity3)), key=lambda i: quantity3[i], reverse=True)
weight3 = [0] * len(quantity3)
for rank, original_index in enumerate(sorted_indices, start=1):
    weight3[original_index] = rank

###########################################分配權重###########################################
cursor.execute("SELECT id, condition1, condition2, condition3 FROM my_table")
rows = cursor.fetchall()

for row in rows:
    record_id, condition1, condition2, condition3 = row
    condition_list = list(map(int, condition2.split(','))) #將字串轉換為串列
    new_weight = weight1[condition1]*100 + (6-sum(weight2[i - 1] for i in condition_list))*10 + weight3[condition3]
    cursor.execute("UPDATE my_table SET weight = ? WHERE id = ?", (new_weight, record_id))
conn.commit()

###########################################權重排序###########################################

cursor.execute("SELECT ROWID FROM my_table ORDER BY weight DESC")
rows = cursor.fetchall()

new_id = 1
for row in rows:
    rowid = row[0]
    cursor.execute("UPDATE my_table SET id = ? WHERE ROWID = ?", (new_id, rowid))
    new_id += 1

conn.commit()

###########################################配對###########################################

cursor.execute("SELECT ID, condition1, condition2, condition3, mate, mate_results, contact FROM my_table ORDER BY ID ")
rows = cursor.fetchall()


for row in rows:
    id, condition1, condition2, ondition3, mate, mate_results, contact = row

    while mate > 0:

        condition2_values = condition2.split(",")
        like_clauses2 = " OR ".join(["hope2 LIKE ?" for _ in condition2_values])
        
        sql_query = f"""
        SELECT id, mate, mate_results, contact
        FROM my_table
        WHERE hope1 = ?
        AND ({like_clauses2})
        AND hope3 LIKE ?
        AND mate != 0;
        """
        like_params = [f"%{value}%" for value in condition2_values]  # 模糊匹配 condition2
        params = (condition1, *like_params, f"%{condition3}%")  # 展開參數列表

        cursor.execute(sql_query, params)
        results = cursor.fetchall()

        if len(results) == 0:
            sql_query = f"""
            SELECT id, mate, mate_results, contact
            FROM my_table
            WHERE hope1 = ?
            AND ({like_clauses2})
            AND mate != 0;
            """
            params = (condition1, *like_params)  # 展開參數列表

            cursor.execute(sql_query, params)
            results = cursor.fetchall()
            if len(results) == 0:
                break
        scope = len(results)
        ran = math.floor(random.random()*scope)
        m_id, m_mate, m_mate_results, m_contact = results[ran]

        mate = mate - 1

        if mate_results == 0 :
            cursor.execute("UPDATE my_table SET mate = ?, mate_results = ? WHERE id = ?", (mate, m_contact, id))
        else:
            cursor.execute("UPDATE my_table SET mate = ?, mate_results = ? WHERE id = ?", (mate, mate_results + "\n" + m_contact, id))
        if m_mate_results == 0 :
            cursor.execute("UPDATE my_table SET mate = ?, mate_results = ? WHERE id = ?", (m_mate-1, contact, m_id))
        else:
            cursor.execute("UPDATE my_table SET mate = ?, mate_results = ? WHERE id = ?", (m_mate-1, m_mate_results + "\n" + contact, m_id))
conn.commit()






###########################################輸出###########################################

df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

output_file = "output.xlsx"
df.to_excel(output_file, index=False, engine="openpyxl")


###########################################結束###########################################

conn.close()