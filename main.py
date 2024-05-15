from fastapi import FastAPI, HTTPException
import cx_Oracle
from simple_salesforce import Salesforce, SalesforceAuthenticationFailed, SalesforceGeneralError
import logging

app = FastAPI()

# Thông tin kết nối Oracle
oracle_username = ''
oracle_password = ''
oracle_host = ''
oracle_port = ''
oracle_service = ''

# Thông tin kết nối Salesforce
salesforce_username = ''
salesforce_password = ''
salesforce_security_token = ''
salesforce_domain = 'test'  # hoặc 'login' nếu bạn đang sử dụng Salesforce Production

# Kết nối đến Oracle
def connect_to_oracle():
    try:
        connection = cx_Oracle.connect(
            user=oracle_username,
            password=oracle_password,
            dsn=f"{oracle_host}:{oracle_port}/{oracle_service}"
        )
        logging.info("Kết nối Oracle thành công")
        return connection
    except cx_Oracle.DatabaseError as e:
        logging.error(f"Lỗi kết nối đến Oracle Database: {e}")
        raise HTTPException(status_code=500, detail="Lỗi kết nối đến Oracle Database")

# Kết nối đến Salesforce
def connect_to_salesforce():
    try:
        sf = Salesforce(username=salesforce_username, password=salesforce_password,
                        security_token=salesforce_security_token, domain=salesforce_domain)
        logging.info("Kết nối Salesforce thành công")
        return sf
    except SalesforceAuthenticationFailed as e:
        logging.error(f"Lỗi xác thực Salesforce: {e}")
        raise HTTPException(status_code=500, detail="Lỗi xác thực Salesforce")
    except SalesforceGeneralError as e:
        logging.error(f"Lỗi chung Salesforce: {e}")
        raise HTTPException(status_code=500, detail="Lỗi kết nối đến Salesforce")

@app.get("/push_to_salesforce")
async def push_to_salesforce():
    # Kết nối đến Oracle
    try:
        oracle_connection = connect_to_oracle()
        oracle_cursor = oracle_connection.cursor()
    except HTTPException as e:
        return {"error": str(e.detail)}

    # Lấy dữ liệu từ Oracle
    try:
        oracle_cursor.execute("SELECT * FROM g_contact WHERE ROWNUM <= 5")
        data_from_oracle = oracle_cursor.fetchall()
    except cx_Oracle.DatabaseError as e:
        logging.error(f"Lỗi lấy dữ liệu từ Oracle: {e}")
        return {"error": "Lỗi lấy dữ liệu từ Oracle"}

    # Đóng kết nối Oracle
    oracle_cursor.close()
    oracle_connection.close()

    # Kết nối đến Salesforce
    try:
        sf = connect_to_salesforce()
    except HTTPException as e:
        return {"error": str(e.detail)}

    # Lấy tên các trường trên Salesforce Object bạn muốn đẩy dữ liệu vào
    fields_to_push = ['Email']

    # Đẩy dữ liệu lên Salesforce
    for row in data_from_oracle:
        try:
            record = {fields_to_push[0]: row[0]}
            sf_object = sf.Contact.create(record)
            logging.info(f"Đã chèn bản ghi với ID: {sf_object['id']}")
        except Exception as e:
            logging.error(f"Lỗi chèn bản ghi vào Salesforce: {e}")
            return {"error": f"Lỗi chèn bản ghi vào Salesforce: {e}"}

    return {"message": "Đã đẩy dữ liệu lên Salesforce thành công"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app)
