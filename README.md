# Dataflow_Dashboard_schedule
Đây là 1 dự án tạo ra luồng, dòng chảy dữ liệu để dữ liệu được cập nhật tự động lên dashboard, tạo ra dashboard theo dõi các chỉ số ngành game, cụ thể là dữ liệu chảy từ google bigquery vào Google Looker Studi, có 1 số lưu ý trước khi tạo dash:
- Chi phí query là rất lớn, đặc biệt là càng ngày dữ liệu càng nhiều thêm, vì vậy bài toán tối ưu chi phí là quan trọng nhất
- Ngoài chi phí lấy dữ liệu hàng ngày, còn có dữ liệu khác cũng rất lớn là ở mỗi lần ta lọc data (filter) trên dashboard cũng tốn kém vì vậy nếu ko xử lý bài toán về chi phí, thì dashboard này sẽ rất tốn kém
- Dashboard này phục vụ mục đích chính ngoài theo dõi chỉ số còn để só sánh xem version nào là tốt nhất

Từ những yêu cầu trên, ta có 1 số hướng đi cho dashboard như sau:
- Về vấn đề chi phí, query tốn chi phí là do query trực tiếp vào bảng event - param (Bảng lưu event, có quá nhiều trường thông tin mình không cần quan tâm), bảng này chưa được unnsest, vì vậy ta sẽ tạo ra các bảng flatten table ứng với từng event
- Sau khi tạo xong các bảng Flatten (ví dụ: level_start, level_win, first_open,....) -> ta sẽ tạo ra các bảng dashboard (ví dụ: winrate, retention,....)
- Cuối cùng là vẽ dashboard

Chốt lại. Luồng dữ liệu của ta sẽ đi như sau: Bảng gốc (full) -> Flatten table (dataset chứa các bảng event đã được flatten) -> Dashboard table (dataset chứa các bảng để vẽ dash được tạo ra từ flatten table) -> Google Data Studio

Với hướng đi này, 1 lần lấy dữ liệu sẽ tiết kiệm chi phí gấp 100 - 200 lần (từ vài trăm mb xuống còn gần 1mb)
