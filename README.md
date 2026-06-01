# Bài Tập: Xử lý thời gian thực với Webhook và Kafka

Dự án này bao gồm một hệ thống hướng sự kiện (Event-driven) có khả năng xử lý thời gian thực các sự kiện từ Facebook Page (như bình luận, tin nhắn), lọc spam và phân tích ngữ nghĩa (Intent & Sentiment) bằng AI.

## 🏗 Kiến trúc dự án
Dự án được chia thành 2 microservices chính:
1. **Webhook Service (Node.js)**
   - Đóng vai trò là cổng giao tiếp (API Gateway/Ingress) nhận dữ liệu từ Facebook Webhook.
   - Xác thực request, chuẩn hóa (normalize) payload của Facebook về một schema chung.
   - Publish sự kiện vào Kafka topic: `raw_events`.
2. **Core Service (.NET 9)**
   - Chạy nền (Background Service) Consume tin nhắn từ Kafka.
   - Pipeline xử lý:
     - Lọc Spam (chứa URL, lặp từ).
     - Phân tích Intent (Ý định) và Sentiment (Cảm xúc) thông qua AI.
     - Tự động ra quyết định (ẩn comment, gửi thông báo,...).

## 🚀 Yêu cầu hệ thống
- **Docker** & **Docker Compose** (để chạy Kafka và Zookeeper)
- **Node.js** (v18 trở lên)
- **.NET 9.0 SDK**

## ⚙️ Hướng dẫn cài đặt và khởi chạy

### Bước 1: Khởi động Kafka Cluster
Mở terminal tại thư mục gốc của dự án và chạy lệnh sau để khởi động Kafka và Kafka UI:
docker-compose up -d
*Lưu ý: Mở Docker Desktop trước khi chạy.*

Sau khi Container đã chạy, tạo topic `raw_events` cho Kafka:
```bash
docker exec kafka kafka-topics --create --topic raw_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Bước 2: Chạy Webhook Service (Node.js)
Mở một terminal mới, chuyển vào thư mục `webhook-service`:
```bash
cd webhook-service
npm install
npm start
```
*Service sẽ chạy ở port mặc định (thường là 3001) chờ nhận Webhook.*

### Bước 3: Chạy Core Service (.NET 9)
Mở một terminal mới, chuyển vào thư mục ứng dụng .NET:
```bash
cd "FacebookAPI - PageAPI"
dotnet build
dotnet run
```
*Service sẽ bắt đầu kết nối với Kafka và chờ các event đến.*

---

## 🧪 Hướng dẫn kiểm thử (Bằng Fake Dữ liệu - Mock Data)
Do việc cấu hình Facebook App đòi hỏi publish lên server có HTTPS (ngrok, domain thật), trong môi trường dev và phục vụ chấm bài, các bạn có thể dùng **Fake Feed (Mock Data)**. 

Gửi trực tiếp các POST Request giả lập Webhook của Facebook vào `webhook-service` thông qua Postman, hoặc dùng lệnh `curl` dưới đây (Mở terminal mới để test):

### 1. Test Comment Bình thường (Hỏi giá)
```bash
curl -X POST http://localhost:3001/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "object": "page",
    "entry": [{
      "id": "123456789",
      "time": 1690000000,
      "changes": [{
        "value": {
          "from": { "id": "987654321", "name": "Nguyễn Văn Test" },
          "item": "comment",
          "message": "Shop ơi cái này giá bao nhiêu vậy?"
        },
        "field": "feed"
      }]
    }]
  }'
```
*Kỳ vọng: Core Service bên .NET sẽ in ra log "Hỏi giá", Sentiment "Trung tính".*

### 2. Test Comment Khiếu nại
```bash
curl -X POST http://localhost:3001/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "object": "page",
    "entry": [{
      "id": "123456789",
      "time": 1690000000,
      "changes": [{
        "value": {
          "from": { "id": "987654321", "name": "Người mua bức xúc" },
          "item": "comment",
          "message": "Mình chưa nhận được hàng, làm ăn chán quá!"
        },
        "field": "feed"
      }]
    }]
  }'
```
*Kỳ vọng: Core Service bên .NET in ra log "Khiếu nại", Sentiment "Tiêu cực" kèm quyết định Cần tạo ticket hỗ trợ khẩn cấp.*

### 3. Test Comment Spam (Chứa Link)
```bash
curl -X POST http://localhost:3001/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "object": "page",
    "entry": [{
      "id": "123456789",
      "time": 1690000000,
      "changes": [{
        "value": {
          "from": { "id": "Spammer123", "name": "Bot Spam" },
          "item": "comment",
          "message": "Click vào link để nhận quà: http://scam-link.vn"
        },
        "field": "feed"
      }]
    }]
  }'
```
*Kỳ vọng: Core Service bên .NET sẽ chặn ngay ở bước 1 và in ra Warning "Spam detected...".*
