# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Văn Ước |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602445 |
| GitHub | nvuoc |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-06570 |

## Kết quả cuối

- `dataset_version` của bạn: `c414c71912e6` (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 716 train / 162 val / 141 test (tổng cộng 1019 khung hình từ 26 trip: 18 train, 4 val, 4 test)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (`task_crash_log.png`, `airflow_task_crash.png` thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Bốn khác biệt lớn giữa telemetry nguồn A và nguồn B ở Bronze là:
1. **Tên cột**: Nguồn A dùng `ts_utc`, `speed_mps`, `heading_deg`, `acc_x`, `gas`; nguồn B dùng `timestamp_ms`, `spd_kmh`, `hdg`, `ax`, `throttle_pct`.
2. **Đơn vị vận tốc**: Nguồn A dùng m/s (`speed_mps`), nguồn B dùng km/h (`spd_kmh`).
3. **Định dạng thời gian**: Nguồn A dùng chuỗi ISO 8601 UTC (`2025-10-07T08:00:00.014Z`), nguồn B dùng Unix epoch milliseconds dạng số nguyên (`1759933801509`).
4. **Đồng hồ thiết bị**: Nguồn B bị lệch đồng hồ so với video thực tế (clock offset +1500 ms) và đơn vị bàn đạp ga/phanh ở dạng phần trăm (%) thay vì tỉ lệ 0..1 như nguồn A.

Pipeline giữ mỗi nguồn một bảng Bronze riêng và lưu mọi cột ở dạng chuỗi (text/string) vì nguyên tắc cốt lõi của Medallion Bronze là lưu trữ nguyên bản (raw data fidelity). Việc không gộp và không ép kiểu ngay giúp tiếp nhận dữ liệu không bị lỗi runtime kể cả khi có schema drift, giá trị dị biệt hay dòng dữ liệu lỗi, bảo toàn chứng cứ thô để audit và cho phép replay logic xử lý về sau.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Pipeline sử dụng các model dbt staging (`stg_telemetry_a.sql` và `stg_telemetry_b.sql`) để chuẩn hóa: quy đổi vận tốc `spd_kmh / 3.6` về `speed_mps`, chuyển epoch milliseconds thành timestamp UTC, và quan trọng nhất là trừ đi 1500 ms độ lệch đồng hồ (`telemetry_clock_offset_ms`) đã khai báo trong manifest `sources.json`. Bằng chứng là sau khi chuẩn hóa, thời điểm bắt đầu của telemetry B (`2025-10-08 14:30:00.009000+00:00`) đã khớp hoàn hảo với thời điểm bắt đầu của video gốc `drive_b`.

Bảng cách ly dữ liệu (quarantine) ghi nhận:
| drive_id | reject_reason | so_dong |
|---|---|---|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

Tổng cộng 106 dòng bị cách ly vào bảng `silver.silver_telemetry_rejects`. Dòng lỗi được đưa vào rejects thay vì bị xóa âm thầm nhằm đảm bảo tính toàn vẹn (auditing): clean (2968 + 3181) + rejected (53 + 53) = 6255 dòng nhận vào, giúp kỹ sư dữ liệu giám sát tỷ lệ lỗi cảm biến và truy nguyên nguyên nhân mà không làm gián đoạn pipeline.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Test `max_gap_ms` được cấu hình là `warn` vì việc mất tín hiệu telemetry trong tích tắc khi xe di chuyển ngoài đời thực (đi qua hầm, bóng râm, nhiễu GPS) là hiện tượng vật lý bình thường, pipeline có thể nội suy hoặc bỏ qua khung hình mất đồng bộ mà không cần dừng toàn bộ hệ thống. Ngược lại, test `in_range` (vận tốc 0..70 m/s) là `error` vì vận tốc âm hoặc vượt quá 250 km/h là dữ liệu hỏng hoàn toàn, nếu lọt vào sẽ làm hỏng hàm loss khi huấn luyện mô hình lái xe tự động.

Trong thí nghiệm `break-gate` (`report/evidence/break_gate.json`), khi nới lỏng luật lọc vận tốc từ 70 lên 1000 m/s, cổng chất lượng dbt test đã kích hoạt chặn ngay lập tức: test `in_range_silver_telemetry_speed_mps__70__0` báo FAIL bắt được chính xác 6 dòng dữ liệu dị biệt (tóm tắt test: `PASS=5 WARN=1 ERROR=1 SKIP=0`). Điều này chứng minh "cổng chất lượng" (quality gate) đóng vai trò là chốt chặn bảo vệ đáng tin cậy ngăn chặn dữ liệu bẩn rò rỉ vào các tầng sau.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Theo `report/evidence/idempotency.json`, trước khi chạy lại bảng có 1246 dòng (dấu vân tay `6c7480dfde7e`). Sau 2 lần chạy lại partition `trip-06570`, số dòng vẫn giữ nguyên là 1246 dòng và dấu vân tay vẫn là `6c7480dfde7e` (không hề suy chuyển). Ngược lại, nếu ghi theo kiểu append truyền thống, một trip 45 khung hình sau 2 lần chạy đã bị nhân đôi lên thành 90 dòng.

Delta Lake `overwrite` với `predicate` (theo từng trip) an toàn hơn cơ chế xoá-ghi thủ công vì thao tác này mang tính ACID transaction. Toàn bộ dữ liệu mới được ghi vào các file parquet tạm, commit log Delta chỉ cập nhật trỏ tới các file mới và đánh dấu file cũ khi hoàn tất trọn vẹn. Nếu giữa chừng tiến trình bị crash, bảng Delta vẫn nguyên vẹn ở trạng thái phiên bản trước đó, không bao giờ bị rơi vào tình trạng bảng trống hay mất dữ liệu như thao tác DELETE thông thường.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

Khung hình đen (`is_black`, mean luma < 12.0) là lỗi phần cứng hoặc camera bị che kín hoàn toàn không chứa thông tin hữu ích nên bị loại bỏ hoàn toàn khỏi dataset. Khung hình tối (`is_dark`, mean luma thấp nhưng > 12.0) là cảnh quay trong điều kiện thiếu sáng thực tế (như chập tối, bóng râm, đường hầm); pipeline gắn cờ nhận diện nhưng vẫn giữ lại (`dark_kept`: drive B có tới 61 khung). Cách xử lý này rất hợp lý và cần thiết để mô hình xe tự lái học được khả năng nhận diện trong điều kiện đêm tối hoặc ánh sáng yếu.

Dựa vào `report/evidence/threshold.json`, số khung hình bị coi là trùng ở từng ngưỡng dHash:
| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Tôi chọn ngưỡng **2** (ngưỡng pipeline sử dụng). Ở ngưỡng 0, ta bỏ sót nhiều khung trùng chỉ vì nhiễu hạt nhẹ của cảm biến. Ở ngưỡng 6, quá nhiều khung hình có chuyển động thực bị loại bỏ nhầm. Ngưỡng dHash phụ thuộc chặt chẽ vào tốc độ xe: khi xe chạy nhanh, khung cảnh thay đổi mạnh giữa các frame nên khoảng cách Hamming lớn; khi xe đứng yên chờ đèn đỏ hoặc chạy chậm (như drive A có đoạn đứng hình camera 6 giây), các frame kế tiếp gần như trùng khít, đòi hỏi ngưỡng nhỏ để lọc trùng hiệu quả.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Theo `report/evidence/leakage.json`, khi chia ngẫu nhiên theo khung hình (per-frame split), có toàn bộ **26/26 trip bị chia cắt** (straddling) và cả 26 trip đều có khung hình xuất hiện đồng thời ở cả tập train lẫn tập test. Trong khi đó, với phương pháp phân chia theo cảnh/trip của pipeline, có **0 trip** bị chia cắt.

Chia theo khung hình gây rò rỉ dữ liệu (data leakage) nghiêm trọng vì video được lấy mẫu với tần số cao, các khung hình kề nhau cách nhau chỉ nửa giây có bối cảnh, góc quay và telemetry gần như hệt nhau. Khi một khung nằm ở train và khung kế tiếp nằm ở test, mô hình chỉ việc "học vẹt" bối cảnh đã ghi nhớ để dự đoán test, dẫn tới điểm kiểm thử cao một cách giả tạo nhưng sẽ trượt thảm hại khi gặp một hành trình hoặc cung đường hoàn toàn mới ngoài thực tế.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` là mã băm nội dung (content-based hash sha256 cắt ngắn 12 ký tự) của toàn bộ danh sách frame_id và thông tin metadata trong dataset. Băm nội dung đảm bảo tính tất định và bất biến: nếu dữ liệu đầu vào và luật sinh không đổi thì hai lần chạy liên tiếp sẽ cho ra cùng một `dataset_version` duy nhất (`c414c71912e6`), tránh việc tạo ra phiên bản giả mạo chỉ vì thời gian chạy máy khác nhau.

`dataset_version` của tôi khác bạn bè vì pipeline sử dụng biến môi trường `LAB_STUDENT_ID` (MSSV: `2A202602445`) làm "muối" (salt) trong câu truy vấn window function `row_number() over (partition by drive_id order by md5(trip_id || '2A202602445'))`. Do đó, thứ tự gán các trip vào train, val, test của mỗi sinh viên là hoàn toàn khác nhau, tạo ra các tập split độc lập và mã băm phiên bản riêng biệt.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Từ phần `lineage` trong `manifest.json` của dataset `c414c71912e6`:
```json
{
  "bronze.raw_telemetry_a": 0,
  "bronze.raw_telemetry_b": 0,
  "bronze.trips_raw": 0,
  "bronze.video_files": 1,
  "silver.frames": 50,
  "silver.telemetry": 0,
  "silver.frames_flagged": 0,
  "silver.frame_telemetry": 0,
  "input_video_sha256": {
    "drive_b": "bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc",
    "drive_a": "636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa"
  }
}
```
Dataset này được tạo trực tiếp từ video gốc có sha256 tương ứng (`636ba1...` và `bfa627...`), bảng bronze telemetry commit 0, bảng bronze trips commit 0, và bảng silver frames commit phiên bản tương ứng.

Nếu 3 tháng sau cần tái tạo chính xác dataset này, ta cần:
1. Hai video gốc có mã sha256 ghi trong lineage.
2. Mã commit version của các bảng Delta nguồn (nhờ tính năng Time Travel của Delta Lake).
3. Giá trị `LAB_STUDENT_ID=2A202602445` và tham số phân chia split (`SPLIT=(70, 15, 15)`) trong mã dbt.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Khi thiết lập `LAB_FAIL_ONCE=trip-06570`:
- Đúng **1 task** `silver_frames` (map index 17 tương ứng với `trip-06570`) chuyển sang màu **đỏ (failed)** với thông báo lỗi `RuntimeError: simulated crash while extracting trip-06570; run it again`.
- **7 task phía sau** chuyển trạng thái màu cam **`upstream_failed`** bao gồm: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`.
- Khi tôi bấm **Clear** task đỏ `silver_frames` (map index 17) và các task downstream, Airflow chỉ kích hoạt chạy lại đúng task bị lỗi và các task phụ thuộc tiếp sau, toàn bộ DAG đã chuyển sang màu **xanh (success)**.
- Dữ liệu hoàn toàn **không bị nhân đôi** (nhờ cơ chế overwrite partition của Delta), tổng số dòng silver frames vẫn đúng 1246, `verify` đạt 27/27, và `dataset_version` vẫn giữ nguyên là `c414c71912e6`. Bằng chứng là ảnh chụp `task_crash_log.png` và `airflow_task_crash.png` trong `report/screenshots/`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Khi mở rộng quy mô lên 500 xe/ngày (hàng trăm ngàn phút video và hàng triệu bản ghi telemetry):
1. **Bộ giải mã video cục bộ (FFmpeg trên single container Airflow)**: Decode video là nút cổ chai tính toán nặng nhất (slide 24). Khi tăng lên 500 xe, một node Airflow sẽ quá tải CPU và hết dung lượng đĩa tạm. Cần chuyển sang mô hình xử lý phân tán: dùng Serverless / Ray / Kubernetes Jobs hoặc worker pool chuyên dụng trên GPU/CPU đa luồng để phân tải song song theo từng đoạn video (slide 23).
2. **DuckDB đơn tiến trình (`lakehouse.duckdb`)**: DuckDB chạy in-process và khóa file khi ghi, không thể đáp ứng hàng trăm worker ghi đồng thời (slide 22). Cần thay thế DuckDB bằng engine tính toán phân tán như **Apache Spark** (PySpark) hoặc **Trino/StarRocks** kết hợp thẳng với Delta Lake trên S3.
3. **Mô hình lập lịch tĩnh trên Airflow**: Tạo 500 * 13 = 6500 task động trên một DAG instance sẽ làm nghẽn Airflow Scheduler và database metastore. Cần chuyển sang kiến trúc Event-Driven Ingestion sử dụng message queue (Apache Kafka / AWS SQS) kích hoạt xử lý streaming hoặc micro-batching thay vì polling theo lô nguyên khối (slide 12, 26).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
SELECT 
    trip_id,
    drive_id,
    lighting,
    conditions,
    total_frames,
    frames_kept,
    ROUND(100.0 * (total_frames - frames_kept) / NULLIF(total_frames, 0), 1) AS drop_rate_pct
FROM marts.mart_trip_summary
ORDER BY drop_rate_pct DESC;
```

**Ý nghĩa**: Câu truy vấn này tính toán tỷ lệ phần trăm khung hình bị loại bỏ (`drop_rate_pct`) trên từng trip trong 26 trip của cả hai hành trình. Kết quả cho thấy các trip ban đêm hoặc đi qua đường hầm có tỷ lệ loại bỏ thấp hơn dự kiến (do cờ tối được giữ lại), trong khi các trip có đoạn dừng đỗ hoặc camera bị rung lắc/đứng hình ở `drive_a` có tỷ lệ khung hình bị loại lên tới hơn 40%, giúp đội ngũ Data Science đánh giá chất lượng thu thập của từng cảm biến trên xe.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điều làm tôi bất ngờ nhất là sức mạnh của kiến trúc Medallion kết hợp Delta Lake: tính năng overwrite partition theo predicate giúp pipeline đạt tính idempotent tuyệt đối mà không cần can thiệp phức tạp khi một task bị crash giữa chừng. Điểm tôi thấy cần đào sâu thêm là cách tối ưu hóa hiệu năng tính hash dHash trên mảng đa chiều khi số lượng frame lên hàng triệu. Một lỗi dữ liệu mà pipeline hiện tại chưa bắt được là **camera bị bẩn cục bộ (ví dụ giọt nước đọng hoặc vết bùn che một phần góc ống kính)**: khung hình không bị đen, độ mờ tổng thể (Laplacian variance) vẫn đạt ngưỡng cao do các chi tiết sắc nét ở vùng còn lại, khiến frame lỗi này vẫn lọt vào tập Gold.
