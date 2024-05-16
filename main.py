from fastapi import FastAPI, HTTPException
import cx_Oracle
from simple_salesforce import Salesforce, SalesforceAuthenticationFailed, SalesforceGeneralError
import logging
import time

app = FastAPI()

# Thông tin kết nối Oracle
oracle_username = 'cargo_dev'
oracle_password = 'cargo_dev2023'
oracle_host = '116.103.228.228'
oracle_port = '1521'
oracle_service = 'dwhcargo'

# Thông tin kết nối Salesforce
salesforce_username = 'nvtuan@vna.capa'
salesforce_password = 'Tuan@161099'
salesforce_security_token = 'SbryDAX5vIBobMtpqFmkP7px'
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
start_time = time.time()
@app.get("/push_to_salesforce")
async def push_to_salesforce():
    # Kết nối đến Oracle
    try:
        oracle_connection = connect_to_oracle()
        oracle_cursor = oracle_connection.cursor()
    except HTTPException as e:
        return {"error": str(e.detail)}

    start_time = time.time()
    # Lấy dữ liệu từ Oracle
    try:
        oracle_cursor.execute("SELECT Email_1, Last_Name FROM g_contact WHERE ROWNUM <= 5")
        data_from_oracle = oracle_cursor.fetchall()
    except cx_Oracle.DatabaseError as e:
        logging.error(f"Lỗi lấy dữ liệu từ Oracle: {e}")
        return {"error": "Lỗi lấy dữ liệu từ Oracle"}

    # Đóng kết nối Oracle
    # oracle_cursor.close()
    # oracle_connection.close()
    end_time = time.time()

    execution_time = end_time - start_time

    minutes = int(execution_time // 60)
    seconds = int(execution_time % 60)
    logging.info(f"Thời gian kéo Data: {minutes} phút {seconds} giây")
    # Kết nối đến Salesforce
    try:
        sf = connect_to_salesforce()
    except HTTPException as e:
        return {"error": str(e.detail)}
    salesforce_record_id = '0031m00000O5c7WAAR'
    # Lấy tên các trường trên Salesforce Object bạn muốn đẩy dữ liệu vào
    # fields_to_push = ['Email']
    # Đẩy dữ liệu lên Salesforce
    for row in data_from_oracle:
        try:
            record = {'Email': row[0], 'LastName': row[1]}
            # sf_object = sf.Contact.create(record)
            sf.Contact.update(salesforce_record_id, record)
            # logging.info(f"Đã chèn bản ghi với ID: {sf_object['id']}")
            logging.info(f"Đã chèn bản ghi với ID: {salesforce_record_id}")
        except Exception as e:
            logging.error(f"Lỗi chèn bản ghi vào Salesforce: {e}")
            return {"error": f"Lỗi chèn bản ghi vào Salesforce: {e}"}
    overall_end_time = time.time()
    overall_execution_time = overall_end_time - start_time

    overall_minutes = int(overall_execution_time // 60)
    overall_seconds = int(overall_execution_time % 60)
    # print("Thoi gian Chay:", minutes, "phut", seconds, "giay")
    logging.info(f"Thời gian chạy: {overall_minutes} phút {overall_seconds} giây")
    return {"message": "Đã đẩy dữ liệu lên Salesforce thành công",
            "Thời gian kéo Data": f"{minutes} phút {seconds} giây",
            "Thời gian chạy": f"{overall_minutes} phút {overall_seconds} giây"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app)
