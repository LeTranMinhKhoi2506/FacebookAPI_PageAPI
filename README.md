# Facebook Page Event Pipeline

Hệ thống này là một pipeline xử lý sự kiện theo hướng event-driven cho Facebook Page.
Nó gồm 4 service chính: `webhook-service`, `core-service`, `backend-api` và `retry-service`, chạy cùng Kafka và SQLite để xử lý comment, reply, ẩn bình luận, retry và dead-letter.

## Kiến trúc hiện tại

### 1. `webhook-service`
- Nhận webhook từ Facebook Page qua endpoint `/webhook`.
- Xác thực request bằng `VERIFY_TOKEN` và `X-Hub-Signature-256`.
- Normalize payload Facebook về schema chung.
- Publish event vào Kafka topic `raw_events`.
- Có endpoint `/health` để kiểm tra trạng thái kết nối Kafka.

### 2. `core-service`
- Consume raw events từ topic `raw_events`.
- Phân loại comment bằng rule + AI.
- Phát lệnh sang topic `reply_commands`.
- Khi phát hiện spam, có thể tạo lệnh `hide_comment`.
- Ghi audit vào SQLite.
- Có rate limit nội bộ cho người dùng/commenter.

### 3. `backend-api`
- Nhận lệnh từ Kafka và gọi Facebook Graph API.
- Xử lý reply comment, hide comment, và các action khác.
- Có circuit breaker để giảm lỗi khi Facebook API không ổn định.
- Ghi audit và trạng thái xử lý vào SQLite.

### 4. `retry-service`
- Consume sự kiện lỗi từ topic `send_failed`.
- Lưu trạng thái retry vào SQLite.
- Retry theo backoff cấu hình sẵn.
- Khi vượt số lần thử, đẩy sang topic `dead_letter`.
- Có alerting và metrics tùy chọn.

## Kafka topics
- `raw_events`: event đầu vào từ Facebook hoặc dữ liệu mô phỏng.
- `reply_commands`: lệnh phản hồi hoặc hide comment.
- `send_failed`: message lỗi cần retry.
- `dead_letter`: message thất bại cuối cùng.

## Yêu cầu hệ thống
- .NET SDK tương ứng với các project .NET trong workspace
- Node.js cho `webhook-service`
- Kafka và Kafka UI
- SQLite
- Visual Studio hoặc terminal `pwsh`

## Cấu hình

### `fb_api/services/webhook-service/.env`
- `PORT`
- `VERIFY_TOKEN`
- `FACEBOOK_APP_SECRET`
- `KAFKA_BROKERS`
- `KAFKA_TOPIC`
- `SKIP_SIGNATURE_VERIFY`

### `fb_api/services/core-service/appsettings.json`
- `AI:Gemini:ApiKey`
- `Kafka:BootstrapServers`
- `Kafka:Topic`
- `Kafka:FailedTopic`
- `Kafka:ReplyCommandsTopic`

### `fb_api/services/backend-api/appsettings.json`
- `Facebook:PageAccessToken`
- `Facebook:GraphVersion`
- `Kafka:BootstrapServers`
- `Kafka:Topic`
- `Kafka:FailedTopic`

### `fb_api/services/retry-service/appsettings.json`
- `Kafka:FailedTopic` = `send_failed`
- `Kafka:DeadLetterTopic` = `dead_letter`
- `Retry:MaxAttempts`
- `Retry:BackoffSeconds`
- `Retry:PollIntervalMs`
- `Alerts:EnablePrometheusMetrics`

Lưu ý: secret như Facebook token hoặc API key không nên commit vào repo. Nên dùng biến môi trường hoặc user-secrets.

## Chạy hệ thống

### 1. Khởi động Kafka và Kafka UI
Chạy Docker Compose ở thư mục gốc của dự án.

### 2. Chạy `webhook-service`
Mở project `fb_api/services/webhook-service`, cài dependencies và chạy service Node.js.

### 3. Chạy `core-service`
Mở project `fb_api/services/core-service` trong Visual Studio hoặc chạy bằng .NET.

### 4. Chạy `backend-api`
Mở project `fb_api/services/backend-api` và chạy web API.

### 5. Chạy `retry-service`
Mở project `fb_api/services/retry-service` và chạy background worker.

## Test nhanh bằng Kafka UI

### Test `webhook-service`
Gửi request vào `POST /webhook` để xác nhận payload Facebook được normalize và đẩy vào `raw_events`.

### Test `retry-service`
Produce message vào topic `send_failed` với payload có `command_id` và `rawEvent`.

Ví dụ:
```json
{
  "failedId": "fail-12345",
  "command_id": "cmd-abcd-5678",
  "sourceTopic": "reply_commands",
  "errorType": "TimeoutException",
  "errorMessage": "Failed to connect to backend Facebook API",
  "failedAt": "2026-06-01T07:23:04.488Z",
  "retryCount": 0,
  "status": "pending",
  "rawEvent": "{\"comment_id\":\"122102358038599871_1313333986965114\",\"message\":\"Shop hỗ trợ rất nhanh\"}"
}
```

Sau đó kiểm tra topic `dead_letter`.

## Test page thật
1. Chạy đủ 4 service.
2. Đảm bảo `Facebook:PageAccessToken` là token thật nhưng không lưu cứng trong repo.
3. Cấu hình Facebook webhook trỏ về `webhook-service`.
4. Comment thật trên page.
5. Quan sát log ở `webhook-service`, `core-service`, `backend-api`, và `retry-service`.
6. Kiểm tra Kafka UI ở các topic `raw_events`, `reply_commands`, `send_failed`, `dead_letter`.

## Lưu ý
- Nếu comment spam, core-service có thể phát lệnh ẩn bình luận.
- Nếu comment như “Mình chờ quá lâu”, hệ thống có thể trả lời xin lỗi thay vì bỏ qua.
- Nếu Facebook API lỗi, backend-api có circuit breaker.
- Nếu retry-service hết số lần thử, message sẽ sang `dead_letter`.
- `webhook-service` có thể tắt verify signature khi test local bằng `SKIP_SIGNATURE_VERIFY=true`.
