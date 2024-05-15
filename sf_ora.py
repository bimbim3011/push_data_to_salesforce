from fastapi import FastAPI, HTTPException
import cx_Oracle
from simple_salesforce import Salesforce, SalesforceAuthenticationFailed, SalesforceGeneralError
import logging
import uuid

#Đẩy từ DB ra cho lên SF

# Cấu hình ghi nhật ký
logging.basicConfig(level=logging.INFO)

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

@app.get("/push_to_oracle")
def push_to_oracle():
    # Kết nối đến Salesforce
    try:
        sf = connect_to_salesforce()
    except HTTPException as e:
        return {"error": str(e.detail)}

    # Truy vấn dữ liệu từ Salesforce
    try:
        contacts = sf.query_all("SELECT Email FROM Contact LIMIT 5")
        logging.info("Lấy dữ liệu từ Salesforce thành công")
    except Exception as e:
        logging.error(f"Lỗi truy vấn dữ liệu từ Salesforce: {e}")
        return {"error": "Lỗi truy vấn dữ liệu từ Salesforce"}
    email_list = []
    # Kết nối đến Oracle
    try:
        oracle_connection = connect_to_oracle()
        oracle_cursor = oracle_connection.cursor()
    except HTTPException as e:
        return {"error": str(e.detail)}

    # Đẩy dữ liệu vào Oracle
    try:
        for contact in contacts['records']:
            email = contact['Email']
            if email:  # Kiểm tra nếu email không rỗng
                unique_id = str(uuid.uuid4())  # Tạo một ID duy nhất
                logging.info(f"Chèn email: {email} với ID: {unique_id}")
                email_list.append(email)
                try:
                    oracle_cursor.execute(
                        "INSERT INTO g_contact (ID, Email) VALUES (:id, :email)",
                        {'id': unique_id, 'email': email}
                    )
                except cx_Oracle.DatabaseError as e:
                    logging.error(f"Lỗi chèn email {email} vào Oracle: {e}")
        # oracle_connection.commit()
        logging.info("Đã chèn dữ liệu vào Oracle thành công")
    except cx_Oracle.DatabaseError as e:
        logging.error(f"Lỗi chèn dữ liệu vào Oracle: {e}")
        return {"error": "Lỗi chèn dữ liệu vào Oracle"}

    # Đóng kết nối Oracle
    oracle_cursor.close()
    oracle_connection.close()

    return {"message": "Đã đẩy dữ liệu lên Oracle thành công"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
