
# README sql_helper

## Tổng quan
Thư mục `sql_helper` cung cấp các tiện ích và script để quản lý cơ sở dữ liệu SQL Server trong dự án Sen13_project_V1. Bao gồm các công cụ tạo schema, khởi tạo, reset và tương tác với SQL Server.

## Cấu trúc thư mục
- `__init__.py`: Đánh dấu thư mục là một package Python.
- `create_schema.sql`: Script SQL để tạo cấu trúc cơ sở dữ liệu.
- `db_utils.py`: Các hàm tiện ích Python cho thao tác với cơ sở dữ liệu (kết nối, truy vấn, ...).
- `init_db.py`: Script khởi tạo cơ sở dữ liệu theo schema đã định nghĩa.
- `reset_db.py`: Script để reset cơ sở dữ liệu về trạng thái ban đầu.
- `sqlserver_utils.py`: Các hàm hỗ trợ thao tác đặc thù với SQL Server.
- `sqlserver_writer.py`: Hàm/lớp hỗ trợ ghi dữ liệu vào SQL Server.

## Hướng dẫn sử dụng
1. **Tạo schema**: Chạy hoặc chỉnh sửa `create_schema.sql` để định nghĩa bảng và quan hệ trong cơ sở dữ liệu.
2. **Khởi tạo database**: Sử dụng `init_db.py` để thiết lập cơ sở dữ liệu theo schema.
3. **Reset database**: Sử dụng `reset_db.py` để xóa và khởi tạo lại cơ sở dữ liệu.
4. **Tiện ích thao tác DB**: Import và sử dụng các hàm trong `db_utils.py` và `sqlserver_utils.py` để kết nối, truy vấn, ...
5. **Ghi dữ liệu**: Sử dụng `sqlserver_writer.py` để chèn hoặc cập nhật dữ liệu hiệu quả.

## Ví dụ
```python
from sql_helper import db_utils
conn = db_utils.get_connection()
result = db_utils.execute_query(conn, "SELECT * FROM my_table")
```

## Yêu cầu
- Python 3.x
- Thư viện pyodbc hoặc driver Python khác cho SQL Server
- Có sẵn một instance SQL Server

## Lưu ý
- Cập nhật thông tin kết nối trong các script cho phù hợp với môi trường của bạn.
- Đảm bảo có quyền thực thi các script tạo schema và reset database.

## Giấy phép
Module này thuộc dự án Sen13_project_V1. Xem chi tiết giấy phép ở dự án chính.
