# Sen13_project

## Giới thiệu
Dự án này là một hệ thống xử lý dữ liệu tài chính, hỗ trợ thu thập, lưu trữ và phân tích dữ liệu từ nhiều nguồn khác nhau. Dự án sử dụng Python và tổ chức thành nhiều module như xử lý dữ liệu, kết nối SQL Server, Redis, và các công cụ hỗ trợ.

## Cấu trúc thư mục
- `config/`: Cấu hình môi trường và thiết lập chung
- `data_process/`: Xử lý dữ liệu
- `redis_helper/`: Hỗ trợ kết nối và thao tác với Redis
- `sql_helper/`: Hỗ trợ kết nối và thao tác với SQL Server
- `tvc_lib/`, : Thư viện và module thu thập dữ liệu
- `ws_client/`: Kết nối WebSocket
- Các file CSV, TXT: Danh sách mã chứng khoán

## Yêu cầu hệ thống
- Python 3.11 trở lên
- SQL Server
- Redis

## Hướng dẫn tạo môi trường ảo (venv)
> **Lưu ý:** Thư mục venv sẽ nằm trong `.gitignore` và không được đẩy lên Git.

Để tạo môi trường ảo cho dự án, thực hiện các bước sau:

1. Mở terminal tại thư mục dự án.
2. Chạy lệnh sau để tạo venv:

```powershell
python -m venv venv
```

3. Kích hoạt môi trường ảo:
- Trên Windows (PowerShell):

```powershell
.\venv\Scripts\Activate.ps1
```

- Trên Windows (cmd):

```cmd
.\venv\Scripts\activate.bat
```

- Trên Linux/MacOS:

```bash
source venv/bin/activate
```

4. Cài đặt các package cần thiết:

```powershell
pip install -r requirements.txt
```

Nếu chưa có file `requirements.txt`, hãy tạo bằng lệnh:

```powershell
pip freeze > requirements.txt
```

## Hướng dẫn chạy dự án
- Cấu hình các thông số kết nối trong `config/settings.py`


