import cx_Oracle
import json
import datetime

# Thực hiện kết nối đến cơ sở dữ liệu Oracle
dsn = cx_Oracle.makedsn('10.1.6.19', 1521, service_name='dbsf')
connection = cx_Oracle.connect('b2c_1a', 'b2c_1a_dev', dsn)

# Tạo một con trỏ để thực hiện truy vấn
cursor = connection.cursor()

# Thực hiện truy vấn
query = """
    SELECT 
        RECORD_TYPE_ID,
        DETAIL_ERROR_ID,
        PHONE,
        COMPLAIN_ID,
        NOTE,
        RESPOND_TYPE,
        COMP_FEEDBACK_TYPE_ID,
        ACCEPT_TYPE_ID,
        ACCOUNT_ID,
        URGENT,
        RESULT_CONTENT,
        COMPLAIN_TYPE,
        COMP_TYPE_ID,
        REFERENCE_ID,
        CASE_NUMBER,
        ACCEPT_SOURCE_ID,
        TO_CHAR(ACCEPT_DATE, 'YYYY-MM-DD') AS ACCEPT_DATE,
        TO_CHAR(FEEDBACK_DATE, 'YYYY-MM-DD') AS FEEDBACK_DATE,
        ACCEPT_USER,
        END_USER,
        COMPLAINER,
        COMP_CONTENT,
        PRE_RESULT,
        PHONE_1,
        TITLE,
        STATUS,
        CALL_BACK_REQUIRE,
        TEMPORARY_CLOSE,
        CEN_CODE
    FROM 
        OMNI_CASE
"""
cursor.execute(query)


# Lấy kết quả của truy vấn
result = cursor.fetchall()
print(result)
# Chuyển đổi kết quả thành danh sách các từ điển
json_result = []
# print(json_result)
for row in result:
    json_result.append({
        # "RECORD_TYPE_ID": row[0],
        # "DETAIL_ERROR_ID": row[1],
        # "PHONE": row[2],
        "COMPLAIN_ID": row[3],
        "NOTE": row[4],
        # "RESPOND_TYPE": row[5],
        # "COMP_FEEDBACK_TYPE_ID": row[6],
        # "ACCEPT_TYPE_ID": row[7],
        # "ACCOUNT_ID": row[8],
        # "URGENT": row[9],
        # "RESULT_CONTENT": row[10],
        # "COMPLAIN_TYPE": row[11],
        # "COMP_TYPE_ID": row[12],
        # "REFERENCE_ID": row[13],
        # "CASE_NUMBER": row[14],
        # "ACCEPT_SOURCE_ID": row[15],
        # "ACCEPT_DATE": str(row[16]) if row[16] else None,
        # "FEEDBACK_DATE": str(row[17]) if row[17] else None,
        # "ACCEPT_USER": row[18],
        # "END_USER": row[19],
        # "COMPLAINER": row[20],
        # "COMP_CONTENT": row[21],
        # "PRE_RESULT": row[22],
        # "PHONE_1": row[23],
        # "TITLE": row[24],
        # "STATUS": row[25],
        # "CALL_BACK_REQUIRE": row[26],
        # "TEMPORARY_CLOSE": row[27],
        # "CEN_CODE": row[28]
    })

print(json_result)

# Chuyển đổi danh sách từ điển thành chuỗi JSON
json_string = json.dumps(json_result, ensure_ascii=False, default=str)
print(json_string)