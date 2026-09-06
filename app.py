import os
import json
import math
import orjson
import traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, g
import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extras import RealDictCursor, execute_values
from werkzeug.security import generate_password_hash, check_password_hash
from flask_compress import Compress
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from dotenv import load_dotenv

# Nạp các biến bảo mật từ file .env
load_dotenv()


def _sanitize_for_json(obj):
    """orjson tuân thủ ĐÚNG chuẩn JSON (RFC 8259): không cho phép NaN/
    Infinity/-Infinity như module json chuẩn của Python vẫn hay "dễ dãi"
    chấp nhận. Dữ liệu ở đây (từ pandas) rất hay có NaN cho các ô trống, nên
    nếu không xử lý trước, orjson.dumps() sẽ ném lỗi ngay khi gặp NaN đầu
    tiên (không đi qua được 'default' - đó là callback cho KIỂU dữ liệu lạ,
    không phải cho GIÁ TRỊ đặc biệt như NaN của kiểu float đã biết).
    Hàm này duyệt đệ quy dict/list, đổi NaN/Infinity -> None trước khi đưa
    cho orjson, để dữ liệu MỚI ghi ra từ giờ luôn là JSON hợp lệ."""
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    if isinstance(obj, (float, np.floating)):
        try:
            if math.isnan(obj) or math.isinf(obj):
                return None
        except TypeError:
            pass
        return float(obj)
    return obj


def _orjson_default(obj):
    """orjson tự serialize sẵn hầu hết kiểu dữ liệu Python gốc kể cả
    datetime.datetime/date, nhưng KHÔNG biết cách xử lý một số kiểu đặc thù
    của pandas/numpy hay xuất hiện trong dữ liệu ở đây (pd.Timestamp,
    numpy.int64...). Hàm này chỉ được gọi cho đúng những trường hợp orjson
    "bó tay" về KIỂU dữ liệu, đóng vai trò tương đương default=str của
    json.dumps() trước đây. Giá trị NaN/Infinity đã được _sanitize_for_json()
    xử lý từ trước nên không cần lo ở đây nữa."""
    if isinstance(obj, (pd.Timestamp,)):
        return str(obj)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    try:
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass
    return str(obj)


def dumps_json(obj):
    """Thay cho json.dumps(obj, ensure_ascii=False, default=str). orjson
    nhanh hơn json chuẩn của Python khoảng 3-10 lần cho cả dump lẫn load -
    đáng chú ý vì các cột JSON ở đây (ds_po_json, receipt_json, row_json,
    data_json trong bảng cache) có thể chứa tới hàng chục nghìn dòng.
    orjson trả về bytes nên cần decode('utf-8') trước khi lưu vào cột TEXT."""
    return orjson.dumps(_sanitize_for_json(obj), default=_orjson_default).decode('utf-8')


def loads_json(s):
    """Thay cho json.loads(s). Có fallback về json chuẩn: dữ liệu đã lưu
    trong DB TỪ TRƯỚC khi đổi sang orjson có thể chứa literal NaN (json
    chuẩn cho ghi ra, orjson thì không) - nếu orjson đọc thất bại, thử lại
    bằng json chuẩn (vẫn đọc được NaN) thay vì crash, để không cần phải
    migrate lại toàn bộ dữ liệu cũ đang có trong Postgres. Từ nay các lượt
    ghi mới đều đã sạch NaN (nhờ dumps_json ở trên) nên fallback này sẽ
    ngày càng ít được dùng tới theo thời gian, chỉ còn hữu ích cho dữ liệu
    cũ chưa được ghi đè lại."""
    try:
        return orjson.loads(s)
    except orjson.JSONDecodeError:
        return json.loads(s)


app = Flask(__name__)

# SECRET_KEY bắt buộc phải có trong biến môi trường (không dùng giá trị mặc
# định hardcode trong code nữa — nếu thiếu, ứng dụng sẽ báo lỗi ngay khi
# khởi động thay vì chạy với 1 secret key ai cũng đọc được từ source code).
_secret_key = os.environ.get('FLASK_SECRET_KEY')
if not _secret_key:
    raise RuntimeError(
        "Thiếu biến môi trường FLASK_SECRET_KEY. Hãy đặt biến này trên Render "
        "(Settings > Environment) với 1 chuỗi ngẫu nhiên dài, ví dụ tạo bằng: "
        "python -c \"import secrets; print(secrets.token_hex(32))\""
    )
app.secret_key = _secret_key

# Server host (Render...) thường chạy theo giờ UTC, không phải giờ Việt Nam.
# Nếu dùng datetime.now() thẳng thì các mốc "Cập nhật lần cuối" sẽ bị lệch
# -7 giờ so với giờ admin thực tế bấm nút. Hàm này luôn trả về giờ VN (naive,
# không kèm tzinfo) để lưu thẳng vào cột TIMESTAMP (không có time zone) của
# Postgres mà không bị Postgres tự quy đổi lại theo timezone của session.
VN_TZ = ZoneInfo('Asia/Ho_Chi_Minh')


def vn_now():
    return datetime.now(VN_TZ).replace(tzinfo=None)


# Tên thứ trong tuần bằng tiếng Việt (dùng cho phần Lịch Sử Tải Lên) -
# datetime.weekday(): Thứ Hai = 0 ... Chủ Nhật = 6.
_WEEKDAY_VI = ['Thứ Hai', 'Thứ Ba', 'Thứ Tư', 'Thứ Năm', 'Thứ Sáu', 'Thứ Bảy', 'Chủ Nhật']


def format_vi_datetime(dt):
    """Định dạng datetime thành chuỗi tiếng Việt, ví dụ:
    'Thứ Bảy, 05/09/2026 14:10'. Trả về None nếu dt rỗng."""
    if dt is None:
        return None
    return f"{_WEEKDAY_VI[dt.weekday()]}, {dt.strftime('%d/%m/%Y %H:%M')}"

# Cấu hình cookie phiên đăng nhập an toàn hơn:
# - SECURE: chỉ gửi cookie qua HTTPS (Render luôn phục vụ qua HTTPS)
# - HTTPONLY: JavaScript phía trình duyệt không đọc được cookie (chống XSS đánh cắp session)
# - SAMESITE=Lax: hạn chế cookie bị gửi kèm trong các request từ trang khác (chống CSRF cơ bản)
app.config.update(
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    PERMANENT_SESSION_LIFETIME=timedelta(hours=8),
    MAX_CONTENT_LENGTH=50 * 1024 * 1024,  # Giới hạn upload tối đa 50MB / request
)

# Nén response (JSON, HTML) bằng gzip để giảm dung lượng truyền tải -> tải nhanh hơn
Compress(app)

# Giới hạn số lần thử đăng nhập để chống brute-force mật khẩu
limiter = Limiter(get_remote_address, app=app, default_limits=[], storage_uri="memory://")


@app.errorhandler(413)
def handle_file_too_large(e):
    """Mặc định Flask trả về trang lỗi HTML khi vượt MAX_CONTENT_LENGTH,
    khiến frontend (đang gọi res.json()) hiểu nhầm thành mất kết nối mạng.
    Trả JSON để hiển thị đúng thông báo cho người dùng."""
    limit_mb = app.config['MAX_CONTENT_LENGTH'] // (1024 * 1024)
    return jsonify({'error': f'File tải lên vượt quá giới hạn cho phép ({limit_mb}MB). Vui lòng chia nhỏ file trước khi tải lên.'}), 413


@app.errorhandler(500)
def handle_internal_error(e):
    """Bắt các lỗi không lường trước (ví dụ mất kết nối DB giữa chừng) để
    luôn trả về JSON thay vì trang lỗi HTML mặc định của Flask/Werkzeug."""
    return jsonify({'error': 'Lỗi máy chủ nội bộ. Vui lòng thử lại sau ít phút.'}), 500

# Lấy chuỗi kết nối bảo mật từ biến môi trường
DATABASE_URL = os.environ.get('DATABASE_URL')

# Connection pool tới Postgres/Neon: tái sử dụng kết nối thay vì mở kết nối
# TCP/TLS mới cho MỖI request (việc mở mới rất tốn thời gian, đặc biệt với
# Neon/serverless Postgres) -> tăng tốc đáng kể cho mọi API.
_db_pool = None


def _get_pool():
    """Tạo connection pool LƯỜI BIẾNG (lazy) - chỉ tạo khi có request đầu
    tiên thật sự cần dùng DB, thay vì tạo ngay lúc import module.
    Lý do: Neon (Postgres serverless) có thể đang "ngủ" (autosuspend) khi
    Render khởi động lại app; nếu tạo pool ngay lúc import mà Neon chưa kịp
    "thức dậy", tiến trình Flask có thể bị treo/timeout ngay từ bước khởi
    động, khiến Render coi app là "không phản hồi" và khởi động lại liên
    tục - biểu hiện ra ngoài là các lỗi kết nối chập chờn."""
    global _db_pool
    if _db_pool is None:
        # Số 10 này là max PER WORKER. Trên gói Render Free (512MB RAM, CPU
        # chia sẻ rất yếu), nên giữ --workers 1 (xem giải thích ở Start
        # Command) - vì vậy tổng kết nối tối đa lý thuyết vẫn là 10, không
        # cần nhân thêm. Nếu sau này nâng cấp gói và tăng số --workers, nhớ
        # NHÂN số này với số --workers để không vượt giới hạn kết nối của
        # Neon (vượt sẽ gây lỗi "too many connections").
        _db_pool = pg_pool.SimpleConnectionPool(1, 10, DATABASE_URL, cursor_factory=RealDictCursor)
    return _db_pool

# Số ngày lưu trữ dữ liệu "Chi tiết PO" trước khi tự động dọn dẹp
PO_DETAIL_RETENTION_DAYS = 120


def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        pool = _get_pool()
        db = pool.getconn()

        # "Pre-ping": kiểm tra kết nối lấy từ pool còn sống hay đã bị Neon
        # đóng do rảnh quá lâu (rất hay gặp với Postgres serverless). Nếu
        # kết nối đã chết, loại bỏ nó khỏi pool và lấy 1 kết nối mới thay vì
        # để lỗi "server closed the connection unexpectedly" làm hỏng cả
        # request của người dùng.
        try:
            probe = db.cursor()
            probe.execute('SELECT 1')
            probe.close()
        except Exception:
            try:
                pool.putconn(db, close=True)
            except Exception:
                pass
            db = pool.getconn()

        g._database = db
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        pool = _get_pool()
        # Nếu có lỗi xảy ra giữa request mà chưa rollback, trả kết nối bẩn về
        # pool sẽ làm hỏng transaction của request tiếp theo dùng lại nó.
        if exception is not None:
            try:
                db.rollback()
            except Exception:
                pass
        try:
            pool.putconn(db)
        except Exception:
            pass


def init_db():
    """Khởi tạo bảng và dữ liệu mẫu trên Neon PostgreSQL"""
    with app.app_context():
        db = get_db()
        cursor = db.cursor()

        # 1. Tạo bảng users
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                username VARCHAR(50) PRIMARY KEY,
                password VARCHAR(255) NOT NULL,
                role VARCHAR(20) NOT NULL,
                store_code VARCHAR(20) NOT NULL
            )
        ''')

        # 2. Bảng nhật ký các lượt tải file (chỉ để hiển thị lịch sử, không dùng để tính toán)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS upload_log (
                id SERIAL PRIMARY KEY,
                store_code VARCHAR(20) NOT NULL,
                upload_time TIMESTAMP NOT NULL,
                ds_po_filename TEXT,
                po_detail_filename TEXT,
                receipt_filename TEXT
            )
        ''')

        # 3. Bảng lưu dữ liệu MỚI NHẤT của "Danh sách PO" và "Chi tiết nhận hàng"
        #    -> mỗi lần có file mới sẽ GHI ĐÈ (xoá sạch + thay thế) cho từng cửa hàng.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS latest_uploads (
                store_code VARCHAR(20) PRIMARY KEY,
                ds_po_filename TEXT,
                ds_po_json TEXT,
                ds_po_upload_time TIMESTAMP,
                receipt_filename TEXT,
                receipt_json TEXT,
                receipt_upload_time TIMESTAMP
            )
        ''')

        # 4. Bảng lưu dữ liệu "Chi tiết PO" theo kiểu CỘNG DỒN (append).
        #    Mỗi dòng dữ liệu được lưu kèm thời điểm nhập (upload_time) để
        #    có thể tự động xoá các dòng đã nhập quá 120 ngày mà không ảnh
        #    hưởng tới các dữ liệu khác. Cột "quantity" dùng để phát hiện
        #    trùng lặp (Mã PO + Mã phụ tùng + Số lượng) khi ghi thêm dữ liệu.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS po_detail_items (
                id SERIAL PRIMARY KEY,
                store_code VARCHAR(20) NOT NULL,
                po_code VARCHAR(100) NOT NULL,
                part_code VARCHAR(100) NOT NULL,
                quantity NUMERIC,
                row_json TEXT NOT NULL,
                filename TEXT,
                upload_time TIMESTAMP NOT NULL
            )
        ''')
        # Đảm bảo cột "quantity" tồn tại kể cả với CSDL đã tạo từ trước
        # (khi bảng po_detail_items đã có sẵn nhưng chưa có cột này).
        cursor.execute('ALTER TABLE po_detail_items ADD COLUMN IF NOT EXISTS quantity NUMERIC')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_po_detail_store ON po_detail_items(store_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_po_detail_upload_time ON po_detail_items(upload_time)')
        # Index gộp (store_code, upload_time DESC): load_store_dataframes()
        # luôn lọc theo store_code RỒI sort theo upload_time giảm dần cùng
        # lúc - 2 index riêng ở trên chỉ giúp được 1 trong 2 việc, Postgres
        # vẫn phải tự sort sau khi lọc. Index gộp này khớp thẳng với mẫu
        # WHERE + ORDER BY của câu query, tránh bước sort tốn thêm khi bảng
        # càng tích luỹ nhiều dữ liệu theo thời gian (cache miss).
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_po_detail_store_time ON po_detail_items(store_code, upload_time DESC)')

        # Dọn các dòng trùng lặp (Mã PO + Mã phụ tùng + Số lượng) có thể đã
        # tồn tại từ TRƯỚC KHI có cơ chế dedup ở CSDL dưới đây (chỉ giữ lại
        # dòng có id nhỏ nhất/cũ nhất của mỗi nhóm trùng) - bước này BẮT
        # BUỘC phải chạy trước khi tạo UNIQUE INDEX, nếu không CREATE UNIQUE
        # INDEX sẽ báo lỗi vì dữ liệu đang vi phạm ràng buộc duy nhất.
        cursor.execute('''
            DELETE FROM po_detail_items a
            USING po_detail_items b
            WHERE a.id > b.id
              AND a.store_code = b.store_code
              AND a.po_code = b.po_code
              AND a.part_code = b.part_code
              AND a.quantity IS NOT DISTINCT FROM b.quantity
        ''')

        # Đổi từ INDEX thường sang UNIQUE INDEX: nhờ vậy việc chống trùng
        # lặp khi ghi thêm dữ liệu (append_po_detail) có thể giao hẳn cho
        # Postgres xử lý bằng "INSERT ... ON CONFLICT DO NOTHING" thay vì
        # phải SELECT toàn bộ khoá đã có của cửa hàng vào RAM Python rồi so
        # sánh từng dòng - cách cũ càng chậm và tốn RAM hơn khi bảng càng
        # tích luỹ nhiều dữ liệu theo thời gian.
        cursor.execute('DROP INDEX IF EXISTS idx_po_detail_dedup')
        cursor.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS idx_po_detail_dedup_uniq
            ON po_detail_items(store_code, po_code, part_code, quantity)
        ''')

        # 5. Bảng lưu TỒN KHO HỆ THỐNG (dạng "dài": mỗi dòng là 1 mã hàng
        #    tại 1 cửa hàng). Đây là dữ liệu kiểu "ghi đè toàn bộ" mỗi lần
        #    admin tải file tồn kho mới lên (khác với po_detail_items là
        #    cộng dồn) - vì tồn kho là số liệu cuối kỳ tại 1 thời điểm, tải
        #    file mới nghĩa là thay hoàn toàn số liệu cũ.
        #    store_code ở đây dùng đúng giá trị NS1..NS5, NSM1 như bảng users.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS inventory_items (
                id SERIAL PRIMARY KEY,
                part_code VARCHAR(100) NOT NULL,
                part_name TEXT,
                unit VARCHAR(50),
                store_code VARCHAR(20) NOT NULL,
                quantity NUMERIC NOT NULL DEFAULT 0
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_inventory_part ON inventory_items(part_code)')

        # 6. Bảng lưu thông tin lần tải file tồn kho gần nhất (chỉ 1 dòng
        #    duy nhất, luôn bị ghi đè - phục vụ hiển thị "Cập nhật lần cuối").
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS inventory_meta (
                id INTEGER PRIMARY KEY DEFAULT 1,
                filename TEXT,
                uploaded_by VARCHAR(50),
                upload_time TIMESTAMP,
                total_parts INTEGER,
                skipped_rows INTEGER
            )
        ''')

        # 8. Cache bảng đối soát đã tính sẵn cho mỗi cửa hàng, LƯU TRONG CSDL
        #    (không chỉ RAM của 1 tiến trình) để dùng CHUNG được giữa NHIỀU
        #    worker Flask (nếu Render chạy `gunicorn -w N` với N > 1) - mỗi
        #    worker là 1 tiến trình riêng, không chia sẻ RAM với nhau, nên
        #    biến toàn cục _result_cache trong Python chỉ có tác dụng cho
        #    đúng worker đã tính ra nó. Cột "version" dùng để biết cache còn
        #    hợp lệ hay đã cũ (giống hệt cơ chế data_version đang dùng).
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS computed_cache (
                store_code VARCHAR(20) PRIMARY KEY,
                version VARCHAR(50) NOT NULL,
                data_json TEXT NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
        ''')

        # 8b. Bảng lưu PHIẾU XIN LUÂN CHUYỂN NỘI BỘ giữa các cửa hàng. Mỗi
        #     phiếu có 1 cửa hàng gửi (from_store), 1 cửa hàng được xin
        #     (to_store) và có thể chứa NHIỀU mã hàng (xem bảng
        #     transfer_items bên dưới). to_store chỉ cần xử lý 1 lần duy
        #     nhất cho cả phiếu: Đồng ý hoặc Từ chối - không còn bước
        #     "Đã soạn/Chưa soạn" trung gian nữa. Admin xem được toàn bộ
        #     phiếu của mọi cửa hàng.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transfer_requests (
                id SERIAL PRIMARY KEY,
                from_store VARCHAR(20) NOT NULL,
                to_store VARCHAR(20) NOT NULL,
                note TEXT,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                reject_reason TEXT,
                created_by VARCHAR(50) NOT NULL,
                created_at TIMESTAMP NOT NULL,
                responded_by VARCHAR(50),
                responded_at TIMESTAMP,
                updated_at TIMESTAMP NOT NULL
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transfer_from_store ON transfer_requests(from_store)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transfer_to_store ON transfer_requests(to_store)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transfer_updated_at ON transfer_requests(updated_at)')

        # 8c. Bảng lưu TỪNG MÃ HÀNG bên trong 1 phiếu luân chuyển (1 phiếu -
        #     nhiều dòng). Cột "received" đánh dấu cửa hàng xin (from_store)
        #     đã thực nhận được đúng mã hàng đó hay chưa - khi tick nhận
        #     hàng, ghi chú tô màu tồn kho của mã này sẽ tự biến mất.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transfer_items (
                id SERIAL PRIMARY KEY,
                request_id INTEGER NOT NULL REFERENCES transfer_requests(id) ON DELETE CASCADE,
                part_code VARCHAR(100) NOT NULL,
                part_name TEXT,
                quantity NUMERIC,
                received BOOLEAN NOT NULL DEFAULT FALSE,
                received_at TIMESTAMP
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transfer_items_request ON transfer_items(request_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transfer_items_part ON transfer_items(part_code)')

        # 8d. Migration từ schema CŨ (1 phiếu = đúng 1 mã hàng, có bước "Đã
        #     soạn/Chưa soạn") sang schema MỚI (1 phiếu - nhiều mã hàng,
        #     không còn bước soạn hàng). Chỉ chạy 1 lần duy nhất, tự động
        #     phát hiện qua sự tồn tại của cột "part_code" cũ trên bảng
        #     transfer_requests - nếu đã migrate rồi thì cột này không còn.
        cursor.execute('''
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'transfer_requests' AND column_name = 'part_code'
        ''')
        if cursor.fetchone():
            cursor.execute('''
                INSERT INTO transfer_items (request_id, part_code, part_name, quantity, received, received_at)
                SELECT id, part_code, part_name, quantity,
                       (status = 'approved' AND prepare_status = 'da_soan'), NULL
                FROM transfer_requests WHERE part_code IS NOT NULL
            ''')
            cursor.execute('ALTER TABLE transfer_requests DROP COLUMN IF EXISTS part_code')
            cursor.execute('ALTER TABLE transfer_requests DROP COLUMN IF EXISTS part_name')
            cursor.execute('ALTER TABLE transfer_requests DROP COLUMN IF EXISTS quantity')
            cursor.execute('ALTER TABLE transfer_requests DROP COLUMN IF EXISTS prepare_status')

        # 9. Seed default users nếu chưa có
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()['count']
        if count == 0:
            default_users = [
                ('admin', 'admin123', 'admin', 'ALL'),
                ('NS1', 'ns1123', 'store', 'NS1'),
                ('NS2', 'ns2123', 'store', 'NS2'),
                ('NS3', 'ns3123', 'store', 'NS3'),
                ('NS4', 'ns4123', 'store', 'NS4'),
                ('NS5', 'ns5123', 'store', 'NS5'),
                ('NSM1', 'nsm1123', 'store', 'NSM1'),
            ]
            # Mật khẩu mặc định chỉ dùng cho lần khởi tạo đầu tiên - LƯU DƯỚI
            # DẠNG HASH ngay từ đầu (không lưu plain text). Sau khi deploy,
            # nên đổi ngay các mật khẩu mặc định này qua trang Quản lý user.
            default_users_hashed = [
                (u, generate_password_hash(p), r, s) for (u, p, r, s) in default_users
            ]
            cursor.executemany("INSERT INTO users (username, password, role, store_code) VALUES (%s, %s, %s, %s) ON CONFLICT (username) DO NOTHING", default_users_hashed)

        db.commit()
        cursor.close()


# Tự động gọi khởi tạo bảng khi chạy app
init_db()


def clean_str(val):
    if pd.isna(val):
        return ""
    return str(val).strip().upper()


def find_col(columns, patterns):
    lower_map = {c: str(c).strip().lower() for c in columns}
    for pattern in patterns:
        for c in columns:
            if pattern in lower_map[c]:
                return c
    return None


def _looks_usable(df):
    """Một DataFrame được coi là hợp lệ khi có nhiều hơn 1 cột (tức là đã
    tách đúng dấu phân cách, không bị dồn hết dữ liệu vào 1 cột)."""
    return df is not None and df.shape[1] > 1


def read_csv_robust(file_storage):
    """
    Đọc file CSV một cách an toàn, chống được các lỗi thường gặp:
    - Sai bảng mã (encoding) tiếng Việt.
    - File được lưu ở dạng UTF-16 (Unicode) - dấu hiệu là 2 byte BOM đầu
      file 0xFF 0xFE hoặc 0xFE 0xFF. Nếu bỏ qua trường hợp này, các bảng mã
      1-byte (latin1, cp1252...) vẫn "đọc thành công" nhưng ra toàn ký tự
      rác xen kẽ \\x00, khiến không tìm được cột nào cả.
    - Dấu phân cách không phải dấu phẩy (chấm phẩy, tab...).
    - File xuất ra có 1-2 dòng tiêu đề/giới thiệu phía trên dòng header thật,
      khiến pandas suy luận nhầm số cột ở những dòng đầu rồi báo lỗi kiểu
      "Error tokenizing data. Expected 1 fields in line 3, saw 3".
    - Một vài dòng lỗi định dạng rải rác trong file.
    """
    encodings = ['utf-8-sig', 'utf-8', 'latin1', 'cp1258', 'cp1252']

    # Ưu tiên thử UTF-16 trước nếu phát hiện BOM tương ứng (thường gặp khi
    # file CSV được xuất ra từ các hệ thống/Excel trên Windows ở định dạng
    # "Unicode Text").
    file_storage.seek(0)
    head = file_storage.read(4)
    file_storage.seek(0)
    if head[:2] in (b'\xff\xfe', b'\xfe\xff'):
        encodings = ['utf-16'] + encodings
    elif head[:4] in (b'\xff\xfe\x00\x00', b'\x00\x00\xfe\xff'):
        encodings = ['utf-32'] + encodings

    last_err = None

    for enc in encodings:
        # 1) Thử đọc bình thường, để pandas tự dò dấu phân cách (sep=None)
        try:
            file_storage.seek(0)
            df = pd.read_csv(file_storage, encoding=enc, sep=None, engine='python')
            if _looks_usable(df):
                return df
        except Exception as e:
            last_err = e

        # 2) Nếu file có vài dòng tiêu đề rác phía trên header thật, thử bỏ
        #    qua lần lượt 1-5 dòng đầu để tìm đúng dòng header
        for skip in range(1, 6):
            try:
                file_storage.seek(0)
                df = pd.read_csv(file_storage, encoding=enc, sep=None, engine='python', skiprows=skip)
                if _looks_usable(df):
                    return df
            except Exception as e:
                last_err = e

        # 3) Cuối cùng, chấp nhận bỏ qua các dòng lỗi định dạng rải rác
        try:
            file_storage.seek(0)
            df = pd.read_csv(file_storage, encoding=enc, on_bad_lines='skip', engine='python')
            if df is not None and df.shape[1] >= 1:
                return df
        except Exception as e:
            last_err = e

    raise ValueError(f"Không thể đọc được file CSV (định dạng/bảng mã không hợp lệ). Chi tiết: {last_err}")


# Các từ khoá tiêu đề cột thường gặp trong 3 loại file (Danh sách PO,
# Chi tiết PO, Chi tiết nhận hàng). Dùng để dò đúng dòng header thật khi
# file Excel/CSV có vài dòng tiêu đề/giới thiệu rác phía trên.
_HEADER_KEYWORDS = [
    'order number', 'mã đơn hàng mua', 'mã đơn hàng', 'mã po', 'po number',
    'siebel po number', 'part#', 'part #', 'part number', 'mã phụ tùng',
    'quantity requested', 'số lượng yêu cầu', 'số lượng', 'quantity',
    'ngày tạo đơn hàng mua', 'ngày tạo', 'ngày đặt',
    'ngày gửi đơn đặt hàng', 'trạng thái đơn hàng mua', 'trạng thái đơn hàng',
    'trạng thái po', 'mrn status', 'trạng thái', 'status', 'part',
    'phân loại đơn hàng phụ tùng', 'phân loại đơn hàng', 'loại đơn hàng', 'order type',
]


def _clean_col_name(c):
    # Loại bỏ khoảng trắng thường + khoảng trắng không ngắt dòng (\xa0) + BOM
    return str(c).replace('\ufeff', '').replace('\xa0', ' ').strip()


def _find_best_header_row(raw_df, max_scan=10):
    """Quét (tối đa) max_scan dòng đầu của 1 DataFrame đọc thô (header=None)
    để tìm dòng nào giống dòng tiêu đề cột nhất, dựa trên số từ khoá cột
    quen thuộc xuất hiện trong dòng đó."""
    best_idx, best_score = None, 0
    for i in range(min(max_scan, len(raw_df))):
        row_vals = [_clean_col_name(v).lower() for v in raw_df.iloc[i].tolist()]
        score = sum(1 for kw in _HEADER_KEYWORDS if any(kw in v for v in row_vals))
        if score > best_score:
            best_score, best_idx = score, i
    return best_idx if best_score > 0 else None


def _looks_like_bad_header(df):
    """True nếu phần lớn tên cột bị đọc sai (dạng 'Unnamed: n' hoặc rỗng),
    dấu hiệu cho thấy dòng header thật không phải dòng đầu tiên."""
    if df is None or df.shape[1] == 0:
        return True
    bad = sum(1 for c in df.columns if str(c).strip() == '' or str(c).startswith('Unnamed'))
    return bad / df.shape[1] > 0.3


def read_any(file_storage):
    """
    Hàm đọc file thông minh: Tự động nhận diện file Excel hoặc tự động thử
    các bảng mã/định dạng của file CSV để chống lỗi mã hóa và lỗi cấu trúc.
    Đồng thời tự động dò đúng dòng tiêu đề thật nếu phía trên có vài dòng
    tiêu đề/giới thiệu rác (áp dụng cho cả Excel lẫn CSV).
    """
    filename = (file_storage.filename or "").lower()

    def _read_excel_smart():
        file_storage.seek(0)
        df = pd.read_excel(file_storage)
        if _looks_like_bad_header(df):
            file_storage.seek(0)
            raw = pd.read_excel(file_storage, header=None)
            header_idx = _find_best_header_row(raw)
            if header_idx is not None:
                file_storage.seek(0)
                df = pd.read_excel(file_storage, header=header_idx)
        return df

    if filename.endswith(('.xlsx', '.xls')):
        df = _read_excel_smart()
    elif filename.endswith('.csv'):
        df = read_csv_robust(file_storage)
        if _looks_like_bad_header(df):
            file_storage.seek(0)
            raw = pd.read_csv(file_storage, sep=None, engine='python', header=None)
            header_idx = _find_best_header_row(raw)
            if header_idx is not None:
                file_storage.seek(0)
                df = pd.read_csv(file_storage, sep=None, engine='python', header=header_idx)
    else:
        # Trường hợp định dạng khác, thử đọc như Excel trước, lỗi thì đọc như CSV
        try:
            df = _read_excel_smart()
        except Exception:
            file_storage.seek(0)
            df = read_csv_robust(file_storage)

    df.columns = [_clean_col_name(c) for c in df.columns]
    return df


# ----------------------------------------------------------------------------
# TỒN KHO HỆ THỐNG - import file "Tổng hợp tồn kho"
# ----------------------------------------------------------------------------

# Chỉ lấy các mã kho thuộc 3 nhóm này (tiền tố), các mã kho khác (KBD, KX,
# KKM, KHO151, KHONDAKM, KHANGCHAMBAN, ...PI2...) sẽ bị bỏ qua.
_INVENTORY_ALLOWED_PREFIXES = {'KPT', 'KPK', 'KPTN'}
# Hậu tố tương ứng với 6 cửa hàng - trùng với store_code trong bảng users.
_INVENTORY_ALLOWED_STORES = {'NS1', 'NS2', 'NS3', 'NS4', 'NS5', 'NSM1'}


def _split_warehouse_code(raw_kho):
    """Tách 'KPT NS1' -> ('KPT', 'NS1'). Trả về (None, None) nếu không đúng
    định dạng "TIỀN TỐ HẬU TỐ" (ví dụ dòng 'Tổng cộng')."""
    parts = str(raw_kho).strip().upper().split()
    if len(parts) != 2:
        return None, None
    return parts[0], parts[1]


def parse_inventory_excel(file_storage):
    """
    Đọc file "Tổng hợp tồn kho" (định dạng đặc thù: 2-3 dòng tiêu đề rác phía
    trên + 2 dòng header bị merge ô + dữ liệu). Không dùng read_any() thông
    thường vì cấu trúc header ở đây khác hẳn (2 dòng header lồng nhau kiểu
    "Cuối kỳ" -> "Số lượng"/"Giá trị").

    Tự động dò:
      - Dòng header chính (dòng có ô đầu tiên là "Mã kho").
      - Cột "Cuối kỳ" - "Số lượng" bằng cách quét 2 dòng header, không hardcode
        cứng theo số thứ tự cột (K), để không bị vỡ nếu form Excel đổi bố cục.

    Trả về danh sách dict: {part_code, part_name, unit, store_code, quantity}
    (đã lọc chỉ giữ mã kho thuộc 18 mã cho phép), kèm số dòng bị bỏ qua và
    danh sách cảnh báo (nếu 1 mã hàng bị trùng ở nhiều nhóm tiền tố khác
    nhau - trường hợp hiếm, sẽ cộng dồn số lượng lại thay vì ghi đè).
    """
    file_storage.seek(0)
    # engine_kwargs={'read_only': True}: yêu cầu openpyxl chỉ đọc GIÁ TRỊ ô,
    # bỏ qua toàn bộ style/định dạng/màu sắc/border (thường rất nhiều trong
    # file "Tổng hợp tồn kho" xuất từ hệ thống) - đây thường là phần tốn thời
    # gian + bộ nhớ nhất khi đọc file Excel lớn bằng pandas, dù app không hề
    # dùng tới thông tin định dạng đó. Bọc try/except vì tham số engine_kwargs
    # chỉ có từ pandas >= 1.3 - nếu server đang chạy bản pandas cũ hơn, tự
    # động rơi về cách đọc mặc định (chậm hơn nhưng vẫn đúng).
    try:
        raw = pd.read_excel(
            file_storage, header=None, dtype=object,
            engine='openpyxl', engine_kwargs={'read_only': True},
        )
    except TypeError:
        file_storage.seek(0)
        raw = pd.read_excel(file_storage, header=None, dtype=object)

    # 1) Tìm dòng header chính: ô đầu tiên (đã strip/upper) = "MÃ KHO"
    header_row = None
    for i in range(min(15, len(raw))):
        first_cell = raw.iat[i, 0]
        if first_cell is not None and str(first_cell).strip().upper() == 'MÃ KHO':
            header_row = i
            break
    if header_row is None:
        raise ValueError('Không tìm thấy dòng tiêu đề "Mã kho" trong file. Vui lòng kiểm tra lại đúng file "Tổng hợp tồn kho".')

    # 2) Dò cột "Cuối kỳ" - "Số lượng" bằng cách quét dòng header chính (tên
    #    nhóm cột, ví dụ "Cuối kỳ" chỉ xuất hiện ở ô đầu tiên của nhóm do bị
    #    merge - các ô còn lại là NaN) và dòng ngay dưới nó (tên cột con,
    #    "Số lượng"/"Giá trị").
    group_row = raw.iloc[header_row]
    sub_row = raw.iloc[header_row + 1] if header_row + 1 < len(raw) else None

    qty_col = None
    current_group = ''
    for c in range(raw.shape[1]):
        cell = group_row.iat[c]
        if cell is not None and str(cell).strip() != '' and str(cell).strip().lower() != 'nan':
            current_group = str(cell).strip().lower()
        if 'cuối kỳ' in current_group and sub_row is not None:
            sub_cell = sub_row.iat[c]
            if sub_cell is not None and 'số lượng' in str(sub_cell).strip().lower():
                qty_col = c
                break
    if qty_col is None:
        raise ValueError('Không tìm thấy cột "Cuối kỳ - Số lượng" trong file.')

    # 3) Dữ liệu bắt đầu sau 2 dòng header (header_row + 2)
    data_start = header_row + 2
    kho_col, part_col, name_col, unit_col = 0, 1, 2, 3

    # ------------------------------------------------------------------
    # Xử lý VECTOR HOÁ bằng pandas thay vì lặp từng dòng bằng Python (vòng
    # lặp for + .iat cho mỗi ô rất chậm với file nhiều nghìn dòng, đặc biệt
    # trên Render free tier có CPU rất yếu - từng gây timeout/crash worker).
    # ------------------------------------------------------------------
    data = raw.iloc[data_start:, [kho_col, part_col, name_col, unit_col, qty_col]].copy()
    data.columns = ['kho', 'part_code', 'part_name', 'unit', 'qty']

    data['kho'] = data['kho'].astype(str).str.strip()
    data['part_code'] = data['part_code'].astype(str).str.strip()
    valid_mask = (
        data['kho'].notna() & data['part_code'].notna()
        & (data['kho'] != '') & (data['kho'].str.lower() != 'none')
        & (data['part_code'] != '') & (data['part_code'].str.lower() != 'none')
    )
    data = data[valid_mask]

    # Tách "KPT NS1" -> prefix="KPT", suffix="NS1". Chỉ nhận dòng có đúng 2
    # từ (giống hệt logic _split_warehouse_code cũ).
    kho_parts = data['kho'].str.upper().str.split()
    valid_len_mask = kho_parts.str.len() == 2
    skipped_rows = int((~valid_len_mask).sum())
    data = data[valid_len_mask]
    kho_parts = kho_parts[valid_len_mask]
    data['prefix'] = kho_parts.str[0]
    data['suffix'] = kho_parts.str[1]

    allowed_mask = data['prefix'].isin(_INVENTORY_ALLOWED_PREFIXES) & data['suffix'].isin(_INVENTORY_ALLOWED_STORES)
    skipped_rows += int((~allowed_mask).sum())
    data = data[allowed_mask]

    data['part_name'] = data['part_name'].fillna('').astype(str).str.strip()
    data['unit'] = data['unit'].fillna('').astype(str).str.strip()
    data['qty'] = pd.to_numeric(data['qty'], errors='coerce').fillna(0.0)

    warnings = []
    rows = []

    if not data.empty:
        # Tên/đơn vị: lấy theo lần xuất hiện ĐẦU TIÊN của mỗi mã hàng trong
        # file (giữ đúng thứ tự gốc) - giống hành vi parts_map.setdefault cũ.
        name_unit = data.groupby('part_code', sort=False).agg(
            part_name=('part_name', 'first'),
            unit=('unit', 'first'),
        )

        # Số lượng: cộng dồn theo (Mã hàng, Cửa hàng) - xử lý trường hợp 1 mã
        # hàng xuất hiện ở nhiều nhóm tiền tố kho khác nhau nhưng cùng 1
        # cửa hàng (cộng dồn thay vì ghi đè, giống logic cũ).
        qty_grouped = data.groupby(['part_code', 'suffix'], sort=False).agg(
            quantity=('qty', 'sum'),
            n=('qty', 'size'),
        ).reset_index()

        dup_rows = qty_grouped[qty_grouped['n'] > 1]
        warnings = [
            f'Mã hàng {r.part_code} tại {r.suffix} xuất hiện ở nhiều nhóm kho khác nhau - đã cộng dồn số lượng.'
            for r in dup_rows.itertuples()
        ]

        result = qty_grouped.merge(name_unit, left_on='part_code', right_index=True, how='left')
        result = result.rename(columns={'suffix': 'store_code'})
        rows = result[['part_code', 'part_name', 'unit', 'store_code', 'quantity']].to_dict(orient='records')

    return rows, skipped_rows, warnings


def get_summary_from_data(combined_data):
    total = len(combined_data)
    debt = sum(1 for x in combined_data if x['status'] == 'Nợ')
    shipping = sum(1 for x in combined_data if x['status'] == 'Đang vận chuyển')
    received = sum(1 for x in combined_data if x['status'] == 'Đã nhận hàng')

    valid_dates = []
    for x in combined_data:
        d_str = x.get('order_date')
        if d_str and d_str != 'Chưa có dữ liệu':
            try:
                valid_dates.append(datetime.strptime(d_str, '%d/%m/%Y'))
            except Exception:
                pass

    min_date = min(valid_dates).strftime('%d/%m/%Y') if valid_dates else 'Chưa có dữ liệu'
    max_date = max(valid_dates).strftime('%d/%m/%Y') if valid_dates else 'Chưa có dữ liệu'

    return {
        'total': total,
        'debt': debt,
        'shipping': shipping,
        'received': received,
        'min_date': min_date,
        'max_date': max_date
    }


def parse_qty(val):
    """Chuyển giá trị số lượng về số (float). Trả về 0 nếu không đọc được
    (ô trống, chữ, NaN...)."""
    try:
        qty = pd.to_numeric(val)
        if pd.isna(qty):
            return 0
        return qty
    except Exception:
        return 0


# Ánh xạ mã "Phân loại đơn hàng" (cột trong file Danh sách PO) sang tên
# hiển thị tiếng Việt. Nhận diện theo mã số đứng đầu (10/22/26...) để không
# phụ thuộc chính xác vào phần chữ tiếng Anh phía sau.
_ORDER_TYPE_MAP = [
    ('10', 'Đơn khẩn'),
    ('22', 'Đơn định kỳ'),
    ('26', 'Đơn Bình, điện, lốp, Dầu nhớt'),
]


def map_order_type(raw_val):
    if raw_val is None or pd.isna(raw_val):
        return 'Chưa có dữ liệu'
    val = str(raw_val).strip()
    if not val or val.lower() == 'nan':
        return 'Chưa có dữ liệu'

    val_lower = val.lower()
    for code, label in _ORDER_TYPE_MAP:
        if val.startswith(code + '-') or val.startswith(code + ' ') or val == code:
            return label
    if 'urgent' in val_lower:
        return 'Đơn khẩn'
    if 'stock order' in val_lower:
        return 'Đơn định kỳ'
    if 'drop shipment' in val_lower:
        return 'Đơn Bình, điện, lốp, Dầu nhớt'

    # Không nhận diện được mã -> hiển thị nguyên giá trị gốc để không mất dữ liệu
    return val


def process_data(ds_po_df, po_detail_df, receipt_df):
    """
    Tính bảng đối soát. ĐÃ VECTOR HÓA bằng pandas (groupby/merge/map) thay
    vì lặp qua từng dòng bằng .iterrows() như bản cũ - .iterrows() tạo 1
    Series mới cho mỗi dòng nên rất chậm (chậm hơn hàng chục-hàng trăm lần
    so với thao tác vector hóa) khi số dòng lên tới hàng chục nghìn, và là
    nguyên nhân chính gây chậm/timeout khi dữ liệu lớn.

    po_detail_df LUÔN được load_store_dataframes() chuẩn bị sẵn với đúng 3
    cột đã làm sạch: 'po_code', 'part_code', 'quantity' (lấy trực tiếp từ
    CSDL, không phải file Excel gốc) nên không cần dò tên cột / clean_str /
    parse_qty lại cho DataFrame này như 2 DataFrame còn lại.
    """
    ds_po_df = ds_po_df.copy()
    receipt_df = receipt_df.copy()
    ds_po_df.columns = [str(c).strip() for c in ds_po_df.columns]
    receipt_df.columns = [str(c).strip() for c in receipt_df.columns]

    ds_po_col = find_col(ds_po_df.columns, ['mã đơn hàng mua', 'mã đơn hàng', 'mã po', 'order number', 'po'])
    ds_date_col = find_col(ds_po_df.columns, ['ngày tạo đơn hàng mua', 'ngày tạo', 'ngày đặt', 'ngày gửi đơn đặt hàng', 'ngày'])
    ds_status_col = find_col(ds_po_df.columns, ['trạng thái đơn hàng mua', 'trạng thái đơn hàng', 'trạng thái po'])
    ds_order_type_col = find_col(ds_po_df.columns, ['phân loại đơn hàng phụ tùng', 'phân loại đơn hàng', 'loại đơn hàng', 'order type'])

    rec_po_col = find_col(receipt_df.columns, ['siebel po number', 'po number', 'mã đơn hàng mua', 'mã đơn hàng', 'po'])
    rec_part_col = find_col(receipt_df.columns, ['part#', 'part #', 'part number', 'mã phụ tùng', 'part'])
    rec_status_col = find_col(receipt_df.columns, ['mrn status', 'trạng thái', 'status'])

    if not all([ds_po_col, ds_date_col]):
        raise ValueError("File Danh sách PO thiếu cột 'Mã PO' hoặc 'Ngày đặt'.")
    if not all([rec_po_col, rec_part_col, rec_status_col]):
        raise ValueError("File Chi tiết nhận hàng thiếu cột 'Mã PO', 'Mã phụ tùng' hoặc 'Trạng thái'.")
    for required_col in ('po_code', 'part_code', 'quantity'):
        if required_col not in po_detail_df.columns:
            raise ValueError("Dữ liệu Chi tiết PO thiếu cột bắt buộc để tính toán.")

    if po_detail_df.empty:
        return pd.DataFrame()

    # ---------- 1) Danh sách PO: ngày đặt sớm nhất / PO bị huỷ / loại đơn hàng ----------
    ds = pd.DataFrame({'po_val': ds_po_df[ds_po_col].map(clean_str)})
    ds = ds[ds['po_val'] != '']
    ds['order_date'] = pd.to_datetime(ds_po_df.loc[ds.index, ds_date_col], dayfirst=True, errors='coerce')

    # Ngày đặt sớm nhất cho mỗi Mã PO (thay cho "if po_val not in date_lookup or order_date < date_lookup[po_val]")
    date_lookup = ds.dropna(subset=['order_date']).groupby('po_val')['order_date'].min()

    cancelled_pos = set()
    if ds_status_col:
        status_vals = ds_po_df.loc[ds.index, ds_status_col].map(clean_str)
        cancelled_pos = set(ds.loc[status_vals == 'CANCELLED', 'po_val'])

    order_type_lookup = {}
    if ds_order_type_col:
        raw_types = ds_po_df.loc[ds.index, ds_order_type_col]
        valid_type = raw_types.notna() & (raw_types.astype(str).str.strip() != '')
        if valid_type.any():
            # Giữ lần xuất hiện ĐẦU TIÊN của mỗi Mã PO (giống "if po_val not in order_type_lookup" cũ)
            order_type_lookup = (
                pd.DataFrame({'po_val': ds['po_val'][valid_type], 'type': raw_types[valid_type]})
                .drop_duplicates('po_val', keep='first')
                .set_index('po_val')['type']
                .to_dict()
            )

    # ---------- 2) Chi tiết nhận hàng: trạng thái mỗi cặp (Mã PO, Mã phụ tùng) ----------
    rec = pd.DataFrame({
        'po_val': receipt_df[rec_po_col].map(clean_str),
        'part_val': receipt_df[rec_part_col].map(clean_str),
        'status_val': receipt_df[rec_status_col].map(clean_str),
    })
    rec = rec[(rec['po_val'] != '') & (rec['part_val'] != '')]

    if rec.empty:
        receipt_lookup = {}
    else:
        # OPEN luôn thắng nếu có ít nhất 1 dòng OPEN; nếu không thì lấy
        # trạng thái của lần xuất hiện đầu tiên - đúng bằng logic gốc
        # "if key not in receipt_lookup or status_val == 'OPEN': receipt_lookup[key] = status_val"
        has_open = rec.assign(is_open=rec['status_val'] == 'OPEN').groupby(['po_val', 'part_val'])['is_open'].any()
        first_status = rec.drop_duplicates(['po_val', 'part_val'], keep='first').set_index(['po_val', 'part_val'])['status_val']
        receipt_lookup = first_status.where(~has_open, 'OPEN').to_dict()

    # ---------- 3) Chi tiết PO: ghép với 2 bảng trên ----------
    detail = po_detail_df[['po_code', 'part_code', 'quantity']].copy()
    detail = detail[(detail['po_code'] != '') & (detail['part_code'] != '')]
    detail = detail[~detail['po_code'].isin(cancelled_pos)]
    # Chỉ giữ lần xuất hiện ĐẦU TIÊN của mỗi cặp (Mã PO, Mã phụ tùng) - giống "seen_pairs" cũ
    detail = detail.drop_duplicates(subset=['po_code', 'part_code'], keep='first')

    if detail.empty:
        return pd.DataFrame()

    today = datetime.now()
    order_date = pd.to_datetime(detail['po_code'].map(date_lookup))
    has_date = order_date.notna()
    days_diff = (today - order_date).dt.days
    detail['order_date'] = order_date.dt.strftime('%d/%m/%Y').where(has_date, 'Chưa có dữ liệu')

    rec_keys = pd.Series(list(zip(detail['po_code'], detail['part_code'])), index=detail.index)
    rec_status = rec_keys.map(receipt_lookup)

    detail['status'] = np.select(
        [rec_status == 'OPEN', rec_status == 'CLOSED'],
        ['Đang vận chuyển', 'Đã nhận hàng'],
        default='Nợ',
    )

    # show_days = trạng thái đang "Nợ" hoặc "Đang vận chuyển" (phụ tùng chưa nhận đủ hàng)
    show_days = detail['status'].isin(['Nợ', 'Đang vận chuyển'])
    detail['days_debt'] = np.where(show_days & has_date, days_diff.fillna(0).astype(int), 0)
    qty_numeric = pd.to_numeric(detail['quantity'], errors='coerce').fillna(0)
    detail['qty_debt'] = np.where(show_days, qty_numeric, 0)
    detail['order_type'] = detail['po_code'].map(order_type_lookup).map(map_order_type)

    df = detail[['po_code', 'part_code', 'order_date', 'order_type', 'status', 'days_debt', 'qty_debt']].reset_index(drop=True)

    # Cột "cộng dồn": tổng số lượng nợ (trạng thái Nợ + Đang vận chuyển)
    # của TẤT CẢ các PO có cùng mã phụ tùng.
    df['qty_debt_total'] = df.groupby('part_code')['qty_debt'].transform('sum')

    sort_key = np.where(df['status'].isin(['Nợ', 'Đang vận chuyển']), -df['days_debt'], 0)
    df = df.assign(_sort=sort_key).sort_values(by=['_sort', 'po_code']).drop(columns=['_sort']).reset_index(drop=True)

    return df


# ----------------------------------------------------------------------------
# Data storage helpers (thay thế / cộng dồn / dọn dẹp)
# ----------------------------------------------------------------------------

def cleanup_old_po_detail(cursor):
    """Xoá các dòng 'Chi tiết PO' đã được nhập quá PO_DETAIL_RETENTION_DAYS
    ngày. Các dữ liệu khác (Danh sách PO, Chi tiết nhận hàng, users...) không
    bị ảnh hưởng."""
    cursor.execute(
        "DELETE FROM po_detail_items WHERE upload_time < NOW() - (%s || ' days')::interval",
        (PO_DETAIL_RETENTION_DAYS,)
    )


def save_ds_po(cursor, store_code, ds_po_file, ds_po_df, upload_time):
    """Danh sách PO: XOÁ SẠCH dữ liệu cũ của cửa hàng này và THAY THẾ hoàn
    toàn bằng dữ liệu mới. Không đụng tới dữ liệu Chi tiết nhận hàng đang có,
    để có thể tải riêng lẻ từng loại file mà không làm mất dữ liệu loại kia."""
    ds_po_json = dumps_json(ds_po_df.to_dict(orient='records'))

    cursor.execute('''
        INSERT INTO latest_uploads (store_code, ds_po_filename, ds_po_json, ds_po_upload_time)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (store_code) DO UPDATE SET
            ds_po_filename = EXCLUDED.ds_po_filename,
            ds_po_json = EXCLUDED.ds_po_json,
            ds_po_upload_time = EXCLUDED.ds_po_upload_time
    ''', (store_code, ds_po_file.filename, ds_po_json, upload_time))


def save_receipt(cursor, store_code, receipt_file, receipt_df, upload_time):
    """Chi tiết nhận hàng: XOÁ SẠCH dữ liệu cũ của cửa hàng này và THAY THẾ
    hoàn toàn bằng dữ liệu mới. Không đụng tới dữ liệu Danh sách PO đang có,
    để có thể tải riêng lẻ từng loại file mà không làm mất dữ liệu loại kia."""
    receipt_json = dumps_json(receipt_df.to_dict(orient='records'))

    cursor.execute('''
        INSERT INTO latest_uploads (store_code, receipt_filename, receipt_json, receipt_upload_time)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (store_code) DO UPDATE SET
            receipt_filename = EXCLUDED.receipt_filename,
            receipt_json = EXCLUDED.receipt_json,
            receipt_upload_time = EXCLUDED.receipt_upload_time
    ''', (store_code, receipt_file.filename, receipt_json, upload_time))


def append_po_detail(cursor, store_code, po_detail_file, po_detail_df, upload_time):
    """Chi tiết PO: GHI THÊM vào dữ liệu cũ (không xoá gì cả), NHƯNG sẽ tự
    động BỎ QUA (không ghi vào CSDL) các dòng đã tồn tại sẵn - xác định
    trùng lặp dựa trên bộ 3 giá trị (Mã PO, Mã phụ tùng, Số lượng) của cùng
    1 cửa hàng. Việc kiểm tra này áp dụng cho cả:
      - Dữ liệu đã có sẵn trong CSDL từ các lần tải trước.
      - Các dòng trùng lặp ngay trong chính file đang tải lên lần này.

    Việc chống trùng với dữ liệu ĐÃ CÓ trong CSDL được giao hẳn cho Postgres
    xử lý qua UNIQUE INDEX (store_code, po_code, part_code, quantity) +
    "ON CONFLICT DO NOTHING", thay vì SELECT toàn bộ khoá cũ của cửa hàng
    vào RAM Python rồi so sánh từng dòng như trước - cách cũ càng chậm và
    tốn RAM hơn khi po_detail_items tích luỹ càng nhiều theo thời gian (bảng
    này KHÔNG bị xoá sạch mỗi lần tải như 2 loại file kia).

    Việc dọn dẹp dữ liệu quá hạn 120 ngày được xử lý riêng ở
    cleanup_old_po_detail().

    Trả về (số dòng đã ghi thêm, số dòng bị bỏ qua vì trùng lặp)."""
    detail_po_col = find_col(po_detail_df.columns, ['order number', 'mã đơn hàng mua', 'mã đơn hàng', 'mã po', 'po'])
    detail_part_col = find_col(po_detail_df.columns, ['part#', 'part #', 'part number', 'mã phụ tùng', 'part'])
    detail_qty_col = find_col(po_detail_df.columns, ['quantity requested', 'số lượng yêu cầu', 'số lượng', 'quantity'])

    if not detail_po_col or not detail_part_col:
        raise ValueError("File Chi tiết PO thiếu cột 'Mã PO' hoặc 'Mã phụ tùng'.")
    if not detail_qty_col:
        raise ValueError("File Chi tiết PO thiếu cột 'Số lượng' (Quantity Requested).")

    detail = po_detail_df.copy()
    detail['_po_code'] = detail[detail_po_col].map(clean_str)
    detail['_part_code'] = detail[detail_part_col].map(clean_str)
    detail['_qty'] = pd.to_numeric(detail[detail_qty_col], errors='coerce').fillna(0.0).astype(float)

    detail = detail[(detail['_po_code'] != '') & (detail['_part_code'] != '')]

    # Bỏ các dòng trùng lặp NGAY TRONG chính file đang tải lên (giữ dòng
    # xuất hiện đầu tiên) - vector hóa bằng drop_duplicates() thay cho vòng
    # lặp Python + set thủ công như trước.
    detail = detail.drop_duplicates(subset=['_po_code', '_part_code', '_qty'], keep='first')

    total_candidates = len(detail)
    if total_candidates == 0:
        return 0, 0

    records = detail.drop(columns=['_po_code', '_part_code', '_qty']).to_dict(orient='records')
    rows_to_insert = [
        (store_code, po, part, qty, dumps_json(rec), po_detail_file.filename, upload_time)
        for po, part, qty, rec in zip(detail['_po_code'], detail['_part_code'], detail['_qty'], records)
    ]

    inserted_rows = execute_values(
        cursor,
        '''INSERT INTO po_detail_items (store_code, po_code, part_code, quantity, row_json, filename, upload_time)
           VALUES %s
           ON CONFLICT (store_code, po_code, part_code, quantity) DO NOTHING
           RETURNING id''',
        rows_to_insert,
        fetch=True,
    )
    inserted_count = len(inserted_rows)
    skipped_count = total_candidates - inserted_count

    return inserted_count, skipped_count


def load_store_dataframes(cursor, store_code):
    """Tải dữ liệu hiện có của 1 cửa hàng để tính toán bảng đối soát:
    - ds_po_df / receipt_df: lấy bản MỚI NHẤT (đã bị thay thế mỗi lần upload).
    - po_detail_df: lấy TOÀN BỘ dữ liệu còn hiệu lực (đã cộng dồn, chưa quá 120 ngày).
    Trả về (ds_po_df, po_detail_df, receipt_df) hoặc None nếu chưa đủ dữ liệu.
    """
    cursor.execute("SELECT ds_po_json, receipt_json FROM latest_uploads WHERE store_code = %s", (store_code,))
    latest = cursor.fetchone()
    if not latest or not latest['ds_po_json'] or not latest['receipt_json']:
        return None

    # Lấy TRỰC TIẾP 3 cột đã CHUẨN HOÁ (po_code/part_code/quantity - vốn đã
    # được clean_str()/pd.to_numeric() một lần khi ghi ở append_po_detail())
    # thay vì đọc cột row_json (chứa nguyên văn mọi cột gốc của file Excel,
    # nặng hơn nhiều) rồi json.loads() cho TỪNG dòng bằng Python. Với bảng
    # càng lớn (cộng dồn theo thời gian), bỏ được bước này giúp giảm cả
    # dung lượng truyền từ Postgres về lẫn thời gian parse JSON trong app.
    # Thứ tự upload_time giảm dần chỉ còn ý nghĩa lịch sử, không ảnh hưởng
    # kết quả vì process_data() dùng drop_duplicates(keep='first') để giữ
    # đúng 1 dòng cho mỗi cặp (Mã PO, Mã phụ tùng) như hành vi seen_pairs cũ.
    cursor.execute(
        "SELECT po_code, part_code, quantity FROM po_detail_items WHERE store_code = %s ORDER BY upload_time DESC",
        (store_code,)
    )
    detail_rows = cursor.fetchall()
    if not detail_rows:
        return None

    ds_po_df = pd.DataFrame(loads_json(latest['ds_po_json']))
    receipt_df = pd.DataFrame(loads_json(latest['receipt_json']))
    po_detail_df = pd.DataFrame(detail_rows, columns=['po_code', 'part_code', 'quantity'])

    return ds_po_df, po_detail_df, receipt_df


def compute_result_for_store(cursor, store_code):
    """Tính bảng đối soát hiện tại cho 1 cửa hàng, dựa trên dữ liệu mới nhất
    của Danh sách PO / Chi tiết nhận hàng và toàn bộ dữ liệu Chi tiết PO còn
    hiệu lực (trong 120 ngày)."""
    dfs = load_store_dataframes(cursor, store_code)
    if dfs is None:
        return []

    ds_po_df, po_detail_df, receipt_df = dfs
    try:
        result_df = process_data(ds_po_df, po_detail_df, receipt_df)
    except Exception:
        return []

    return result_df.to_dict(orient='records')


# Cache đơn giản trong bộ nhớ (RAM) của tiến trình app: /api/data trước đây
# tính lại toàn bộ bảng đối soát (đọc JSON lớn + merge bằng pandas) MỖI LẦN
# được gọi, kể cả khi dữ liệu không hề thay đổi giữa 2 lần gọi liên tiếp
# (ví dụ do polling hoặc bấm refresh nhiều lần).
#
# Mỗi cửa hàng giờ có version RIÊNG (xem get_all_store_data_versions), nên
# cache lưu dạng {store_code: (version, data)} - so sánh version của ĐÚNG
# cửa hàng đó, thay vì trước đây dùng 1 version toàn cục khiến cache của
# TẤT CẢ cửa hàng bị xoá sạch mỗi khi bất kỳ cửa hàng nào có upload mới.
_result_cache = {}


def get_global_data_version(cursor):
    """Trả về mốc thời gian mới nhất trong 3 nguồn dữ liệu ảnh hưởng tới
    bảng đối soát (Danh sách PO / Chi tiết nhận hàng / Chi tiết PO) ở TẤT CẢ
    cửa hàng. CHỈ dùng cho /api/version (frontend poll giá trị này để biết
    "có gì đó vừa đổi ở đâu đó" - không cần chi tiết đổi ở cửa hàng nào)."""
    cursor.execute('''
        SELECT GREATEST(
            COALESCE((SELECT MAX(ds_po_upload_time) FROM latest_uploads), 'epoch'::timestamp),
            COALESCE((SELECT MAX(receipt_upload_time) FROM latest_uploads), 'epoch'::timestamp),
            COALESCE((SELECT MAX(upload_time) FROM po_detail_items), 'epoch'::timestamp)
        ) AS v
    ''')
    return cursor.fetchone()['v']


def get_store_data_version(cursor, store_code):
    """Giống get_global_data_version nhưng chỉ tính cho MỘT cửa hàng. Dùng
    khi /api/data chỉ cần bảng đối soát của 1 cửa hàng cụ thể (không phải
    'ALL'), để tránh việc 1 cửa hàng khác vừa upload làm cache của cửa hàng
    này bị coi là cũ một cách oan uổng."""
    cursor.execute('''
        SELECT GREATEST(
            COALESCE((SELECT ds_po_upload_time FROM latest_uploads WHERE store_code = %s), 'epoch'::timestamp),
            COALESCE((SELECT receipt_upload_time FROM latest_uploads WHERE store_code = %s), 'epoch'::timestamp),
            COALESCE((SELECT MAX(upload_time) FROM po_detail_items WHERE store_code = %s), 'epoch'::timestamp)
        ) AS v
    ''', (store_code, store_code, store_code))
    return cursor.fetchone()['v']


def get_all_store_data_versions(cursor):
    """Tính version RIÊNG cho từng cửa hàng trong 1 lần truy vấn (thay vì
    gọi get_store_data_version() lặp lại cho mỗi cửa hàng - tránh N round-trip
    tới DB khi admin xem 'ALL'). Trả về dict {store_code: version}.

    Nhờ có version riêng theo từng cửa hàng, khi 1 cửa hàng upload dữ liệu
    mới, CHỈ cache của đúng cửa hàng đó bị vô hiệu - cache của các cửa hàng
    khác (không hề đổi gì) vẫn dùng lại được, thay vì trước đây dùng chung 1
    version toàn cục khiến TOÀN BỘ cửa hàng đều bị tính lại mỗi khi có bất kỳ
    upload nào xảy ra ở bất kỳ đâu."""
    cursor.execute('''
        SELECT store_code, MAX(ts) AS v FROM (
            SELECT store_code, ds_po_upload_time AS ts FROM latest_uploads
            UNION ALL
            SELECT store_code, receipt_upload_time AS ts FROM latest_uploads
            UNION ALL
            SELECT store_code, upload_time AS ts FROM po_detail_items
        ) combined
        WHERE ts IS NOT NULL
        GROUP BY store_code
    ''')
    return {r['store_code']: r['v'] for r in cursor.fetchall()}


def compute_result_for_store_cached(cursor, store_code, version):
    """Giống compute_result_for_store nhưng có cache 2 TẦNG:
    1) RAM của tiến trình hiện tại (_result_cache) - nhanh nhất, không tốn
       round-trip tới DB, nhưng CHỈ dùng được trong đúng worker đã tính ra nó.
    2) Bảng computed_cache trong Postgres - CHIA SẺ được giữa NHIỀU worker
       (nếu Render chạy nhiều tiến trình Flask cùng lúc): worker nào tính
       trước sẽ ghi kết quả vào đây, các worker khác đọc lại thay vì phải
       tính lại từ đầu (đọc JSON từ DB vẫn rẻ hơn nhiều so với việc load lại
       toàn bộ Danh sách PO/Chi tiết PO/Chi tiết nhận hàng rồi merge bằng
       pandas).
    Cả 2 tầng đều tự động vô hiệu khi "version" của ĐÚNG cửa hàng đó đổi -
    không ảnh hưởng tới cache của các cửa hàng khác.
    """
    global _result_cache
    version_str = str(version)

    cached_entry = _result_cache.get(store_code)
    if cached_entry is not None and cached_entry[0] == version_str:
        return cached_entry[1]

    cursor.execute(
        'SELECT data_json FROM computed_cache WHERE store_code = %s AND version = %s',
        (store_code, version_str)
    )
    cached_row = cursor.fetchone()
    if cached_row:
        data = loads_json(cached_row['data_json'])
        _result_cache[store_code] = (version_str, data)
        return data

    data = compute_result_for_store(cursor, store_code)
    _result_cache[store_code] = (version_str, data)

    # Ghi lại vào DB cho các worker khác dùng chung. Dùng try/except riêng vì
    # đây chỉ là tối ưu hiệu năng - nếu ghi cache lỗi (ví dụ race condition
    # hiếm gặp) thì vẫn trả kết quả đã tính đúng cho request hiện tại, không
    # để lỗi cache làm hỏng cả API.
    try:
        cursor.execute('''
            INSERT INTO computed_cache (store_code, version, data_json, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (store_code) DO UPDATE SET
                version = EXCLUDED.version,
                data_json = EXCLUDED.data_json,
                updated_at = EXCLUDED.updated_at
        ''', (store_code, version_str, dumps_json(data)))
        cursor.connection.commit()
    except Exception:
        try:
            cursor.connection.rollback()
        except Exception:
            pass

    return data


# ----------------------------------------------------------------------------
# Routes & Endpoints
# ----------------------------------------------------------------------------

@app.route('/')
def index():
    if 'user' not in session:
        return redirect(url_for('login'))
    return render_template('index.html', user=session['user'], role=session['role'], store_code=session['store_code'])


def _is_hashed_password(stored_value):
    """Nhận diện mật khẩu đã được hash bằng werkzeug (các thuật toán werkzeug
    hỗ trợ đều lưu dưới dạng 'method:params$salt$hash', ví dụ
    'pbkdf2:sha256:...' hoặc 'scrypt:...'). Mật khẩu cũ (từ trước khi áp
    dụng hash) là chuỗi thường, không có dạng này."""
    return isinstance(stored_value, str) and stored_value.startswith(('pbkdf2:', 'scrypt:', 'argon2:'))


def verify_and_upgrade_password(cursor, db, user, password):
    """Kiểm tra mật khẩu, hỗ trợ cả 2 trường hợp:
    - Mật khẩu đã hash (trường hợp bình thường) -> so sánh bằng check_password_hash.
    - Mật khẩu cũ còn plain text (tài khoản tạo/đổi từ trước khi nâng cấp
      bảo mật) -> so sánh trực tiếp, và nếu đúng thì TỰ ĐỘNG cập nhật lại
      thành dạng hash ngay trong DB, để những lần đăng nhập sau không còn
      là plain text nữa. Không cần chạy migration riêng, không mất tài
      khoản nào - mỗi user chỉ cần đăng nhập lại 1 lần là tự nâng cấp."""
    stored = user['password']

    if _is_hashed_password(stored):
        return check_password_hash(stored, password)

    # Mật khẩu cũ dạng plain text
    if stored == password:
        new_hash = generate_password_hash(password)
        cursor.execute("UPDATE users SET password = %s WHERE username = %s", (new_hash, user['username']))
        db.commit()
        return True

    return False


@app.route('/login', methods=['GET', 'POST'])
@limiter.limit("10 per minute")
def login():
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = (request.form.get('password') or '').strip()

        db = get_db()
        cursor = db.cursor()
        cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()

        # So sánh mật khẩu (tự hỗ trợ nâng cấp tài khoản cũ còn plain text
        # lên hash ngay khi đăng nhập thành công - xem verify_and_upgrade_password).
        if user and verify_and_upgrade_password(cursor, db, user, password):
            cursor.close()
            session.clear()
            session['user'] = user['username']
            session['role'] = user['role']
            session['store_code'] = user['store_code']
            session.permanent = True
            return redirect(url_for('index'))
        else:
            cursor.close()
            return render_template('login.html', error="Sai tên đăng nhập hoặc mật khẩu!")
    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/api/upload', methods=['POST'])
def upload_files():
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

    store_code = session['store_code']
    if store_code == 'ALL':
        return jsonify({'error': 'Admin không trực tiếp tải file cửa hàng.'}), 400

    ds_po_file = request.files.get('ds_po_file')
    po_detail_file = request.files.get('po_detail_file')
    receipt_file = request.files.get('receipt_file')

    # Cho phép tải lên MỘT hoặc MỘT VÀI file trong số 3 file, không bắt buộc
    # phải đủ cả 3 mỗi lần. Ví dụ: "Chi tiết PO" là dữ liệu cộng dồn nên có
    # thể tải riêng lẻ nhiều lần mà không cần đi kèm 2 file kia; 2 file
    # "Danh sách PO" / "Chi tiết nhận hàng" (nếu có) vẫn sẽ thay thế dữ liệu
    # cũ của đúng loại đó, các loại không được gửi lên sẽ giữ nguyên.
    if not ds_po_file and not po_detail_file and not receipt_file:
        return jsonify({'error': 'Vui lòng chọn ít nhất một file để tải lên.'}), 400

    try:
        upload_time = vn_now()
        upload_time_str = upload_time.strftime('%Y-%m-%d %H:%M:%S')

        db = get_db()
        cursor = db.cursor()

        # 1) Danh sách PO: nếu có file mới thì xoá sạch & thay thế
        if ds_po_file:
            ds_po_df = read_any(ds_po_file)
            save_ds_po(cursor, store_code, ds_po_file, ds_po_df, upload_time)

        # 2) Chi tiết nhận hàng: nếu có file mới thì xoá sạch & thay thế
        if receipt_file:
            receipt_df = read_any(receipt_file)
            save_receipt(cursor, store_code, receipt_file, receipt_df, upload_time)

        # 3) Chi tiết PO: nếu có file mới thì ghi thêm vào dữ liệu cũ (cộng
        #    dồn), tự động bỏ qua các dòng đã trùng (Mã PO + Mã phụ tùng +
        #    Số lượng) để tránh ghi lặp vào CSDL.
        po_detail_inserted = None
        po_detail_skipped = None
        if po_detail_file:
            po_detail_df = read_any(po_detail_file)
            po_detail_inserted, po_detail_skipped = append_po_detail(cursor, store_code, po_detail_file, po_detail_df, upload_time)

        # 4) Dọn dẹp dữ liệu Chi tiết PO đã quá 120 ngày (các dữ liệu khác giữ nguyên)
        cleanup_old_po_detail(cursor)

        # 5) Ghi log lượt tải (chỉ phục vụ hiển thị lịch sử) - file nào không
        #    được gửi lên lần này sẽ ghi log là NULL.
        cursor.execute('''
            INSERT INTO upload_log (store_code, upload_time, ds_po_filename, po_detail_filename, receipt_filename)
            VALUES (%s, %s, %s, %s, %s)
        ''', (
            store_code, upload_time_str,
            ds_po_file.filename if ds_po_file else None,
            po_detail_file.filename if po_detail_file else None,
            receipt_file.filename if receipt_file else None,
        ))

        db.commit()

        # 6) Tính lại bảng đối soát mới nhất cho cửa hàng này
        data_dicts = compute_result_for_store(cursor, store_code)
        summary = get_summary_from_data(data_dicts)
        cursor.close()

        response = {'success': True, 'data': data_dicts, 'summary': summary, 'upload_time': upload_time_str}

        # Thông báo cho cửa hàng biết số dòng "Chi tiết PO" đã được ghi mới
        # và số dòng bị bỏ qua vì đã tồn tại (trùng Mã PO + Mã phụ tùng + Số lượng).
        if po_detail_file is not None:
            response['po_detail_inserted'] = po_detail_inserted
            response['po_detail_skipped'] = po_detail_skipped

        # Nếu cửa hàng chưa từng có đủ "Danh sách PO" + "Chi tiết nhận hàng"
        # (ví dụ đây là lần tải đầu tiên và chỉ chọn mỗi file Chi tiết PO),
        # bảng đối soát sẽ trống. Vẫn báo tải file thành công nhưng kèm cảnh
        # báo để cửa hàng biết cần bổ sung đủ 3 loại file ít nhất 1 lần.
        if not data_dicts:
            response['warning'] = ('Đã lưu file thành công, nhưng chưa đủ dữ liệu để đối soát. '
                                    'Vui lòng đảm bảo cửa hàng đã tải đủ "Danh sách PO" và '
                                    '"Chi tiết nhận hàng" ít nhất 1 lần.')

        return jsonify(response)
    except Exception as e:
        # In đầy đủ traceback ra Render Logs để chẩn đoán mà không cần mò
        # DevTools của trình duyệt mỗi lần có lỗi.
        app.logger.error("Lỗi /api/upload (store=%s): %s\n%s", store_code, e, traceback.format_exc())
        try:
            get_db().rollback()
        except Exception:
            pass
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/upload-inventory', methods=['POST'])
def upload_inventory():
    """Admin tải file "Tổng hợp tồn kho" lên. Dữ liệu sẽ GHI ĐÈ TOÀN BỘ
    (không cộng dồn) - vì đây là số liệu tồn cuối kỳ tại 1 thời điểm."""
    if 'user' not in session or session['role'] != 'admin':
        return jsonify({'error': 'Forbidden'}), 403

    inventory_file = request.files.get('inventory_file')
    if not inventory_file:
        return jsonify({'error': 'Vui lòng chọn file tồn kho để tải lên.'}), 400

    try:
        rows, skipped_rows, warnings = parse_inventory_excel(inventory_file)
        if not rows:
            return jsonify({'error': 'Không đọc được mã hàng nào thuộc 18 mã kho quy định trong file này.'}), 400

        upload_time = vn_now()
        db = get_db()
        cursor = db.cursor()

        # Ghi đè toàn bộ: xoá sạch dữ liệu tồn kho cũ rồi nạp lại từ đầu.
        cursor.execute('TRUNCATE TABLE inventory_items')
        execute_values(
            cursor,
            '''INSERT INTO inventory_items (part_code, part_name, unit, store_code, quantity)
               VALUES %s''',
            [(r['part_code'], r['part_name'], r['unit'], r['store_code'], r['quantity']) for r in rows]
        )

        distinct_parts = len({r['part_code'] for r in rows})

        cursor.execute('''
            INSERT INTO inventory_meta (id, filename, uploaded_by, upload_time, total_parts, skipped_rows)
            VALUES (1, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                filename = EXCLUDED.filename,
                uploaded_by = EXCLUDED.uploaded_by,
                upload_time = EXCLUDED.upload_time,
                total_parts = EXCLUDED.total_parts,
                skipped_rows = EXCLUDED.skipped_rows
        ''', (inventory_file.filename, session['user'], upload_time, distinct_parts, skipped_rows))

        db.commit()
        cursor.close()

        return jsonify({
            'success': True,
            'total_parts': distinct_parts,
            'skipped_rows': skipped_rows,
            'warnings': warnings,
        })
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        app.logger.error("Lỗi /api/admin/upload-inventory: %s\n%s", e, traceback.format_exc())
        try:
            get_db().rollback()
        except Exception:
            pass
        return jsonify({'error': str(e)}), 500


@app.route('/api/inventory', methods=['GET'])
def get_inventory():
    """Trả về tồn kho hệ thống dạng pivot (1 dòng/mã hàng, 6 cột theo cửa
    hàng). Mọi user (admin lẫn store) đều xem được TOÀN BỘ hệ thống - đây
    là ngoại lệ có chủ đích so với dữ liệu PO (vốn giới hạn theo cửa hàng)."""
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

    db = get_db()
    cursor = db.cursor()
    cursor.execute('SELECT part_code, part_name, unit, store_code, quantity FROM inventory_items')
    items = cursor.fetchall()

    pivot = {}
    for it in items:
        p = pivot.setdefault(it['part_code'], {
            'part_code': it['part_code'],
            'part_name': it['part_name'],
            'unit': it['unit'],
            'NS1': 0, 'NS2': 0, 'NS3': 0, 'NS4': 0, 'NS5': 0, 'NSM1': 0,
        })
        qty = float(it['quantity']) if it['quantity'] is not None else 0
        p[it['store_code']] = qty

    data = sorted(pivot.values(), key=lambda x: x['part_code'])

    cursor.execute('SELECT filename, uploaded_by, upload_time, total_parts, skipped_rows FROM inventory_meta WHERE id = 1')
    meta_row = cursor.fetchone()
    meta = dict(meta_row) if meta_row else None
    if meta and meta.get('upload_time'):
        meta['upload_time'] = meta['upload_time'].strftime('%d/%m/%Y %H:%M')

    cursor.close()

    return jsonify({'success': True, 'data': data, 'meta': meta})


@app.route('/api/version', methods=['GET'])
def get_version():
    """Trả về "chữ ký phiên bản" hiện tại của từng mảng dữ liệu (Dashboard,
    Lịch sử, Tồn kho, Danh sách user). Được frontend gọi định kỳ (poll) với
    tần suất thấp (nhẹ, chỉ vài giá trị) để phát hiện có thay đổi hay không,
    từ đó tự động tải lại đúng phần dữ liệu đã đổi mà KHÔNG cần F5 lại trang
    và không cần tải lại những phần chưa đổi."""
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

    db = get_db()
    cursor = db.cursor()

    # Dashboard/Kết quả đối soát: đổi khi có upload mới (Danh sách PO, Chi
    # tiết nhận hàng, hoặc Chi tiết PO) ở bất kỳ cửa hàng nào.
    data_version = get_global_data_version(cursor)

    cursor.execute('SELECT MAX(id) AS v FROM upload_log')
    history_version = cursor.fetchone()['v']

    cursor.execute('SELECT upload_time AS v FROM inventory_meta WHERE id = 1')
    inv_row = cursor.fetchone()
    inventory_version = inv_row['v'] if inv_row else None

    # Danh sách user: đổi khi thêm/xoá user hoặc đổi mật khẩu (dùng hash gộp
    # cả bảng thay vì thêm cột "updated_at" mới để tránh phải sửa schema cũ).
    cursor.execute('''
        SELECT MD5(COALESCE(string_agg(username || ':' || role || ':' || store_code || ':' || password, ',' ORDER BY username), '')) AS v
        FROM users
    ''')
    users_version = cursor.fetchone()['v']

    # Luân chuyển nội bộ: đổi khi có yêu cầu mới, hoặc yêu cầu cũ được phản
    # hồi/cập nhật trạng thái soạn hàng (tất cả các thao tác đều cập nhật
    # cột updated_at, nên chỉ cần lấy mốc lớn nhất).
    cursor.execute('SELECT MAX(updated_at) AS v FROM transfer_requests')
    transfer_row = cursor.fetchone()
    transfer_version = transfer_row['v'] if transfer_row else None

    cursor.close()

    return jsonify({
        'success': True,
        'data_version': str(data_version) if data_version else None,
        'history_version': history_version,
        'inventory_version': str(inventory_version) if inventory_version else None,
        'users_version': users_version,
        'transfer_version': str(transfer_version) if transfer_version else None,
    })


@app.route('/api/data', methods=['GET'])
def get_data():
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

    role = session['role']
    store_code_session = session['store_code']
    target_store = request.args.get('store', store_code_session)

    db = get_db()
    cursor = db.cursor()

    if role == 'store':
        target_store = store_code_session

    # Dọn dẹp dữ liệu Chi tiết PO quá hạn trước khi tính toán
    cleanup_old_po_detail(cursor)
    db.commit()

    combined_data = []

    if target_store == 'ALL' and role == 'admin':
        cursor.execute('''
            SELECT DISTINCT store_code FROM (
                SELECT store_code FROM latest_uploads
                UNION
                SELECT store_code FROM po_detail_items
            ) AS stores
        ''')
        store_codes = [r['store_code'] for r in cursor.fetchall()]

        # Lấy version của TỪNG cửa hàng trong 1 lần truy vấn duy nhất, để
        # cache của cửa hàng A không bị coi là "cũ" chỉ vì cửa hàng B vừa
        # upload dữ liệu mới (xem chi tiết ở get_all_store_data_versions()).
        versions_by_store = get_all_store_data_versions(cursor)

        for sc in store_codes:
            sc_version = versions_by_store.get(sc, 'epoch')
            items = compute_result_for_store_cached(cursor, sc, sc_version)
            for itm in items:
                itm['store_code'] = sc
                combined_data.append(itm)
    else:
        version = get_store_data_version(cursor, target_store)
        items = compute_result_for_store_cached(cursor, target_store, version)
        for itm in items:
            itm['store_code'] = target_store
            combined_data.append(itm)

    cursor.close()

    summary = get_summary_from_data(combined_data)

    return jsonify({
        'success': True,
        'data': combined_data,
        'summary': summary
    })


@app.route('/api/history', methods=['GET'])
def get_history():
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    store_code = session['store_code'] if session['role'] == 'store' else request.args.get('store', 'ALL')

    db = get_db()
    cursor = db.cursor()
    if store_code == 'ALL':
        cursor.execute('SELECT id, store_code, upload_time, ds_po_filename, po_detail_filename, receipt_filename FROM upload_log ORDER BY upload_time DESC LIMIT 50')
    else:
        cursor.execute('SELECT id, store_code, upload_time, ds_po_filename, po_detail_filename, receipt_filename FROM upload_log WHERE store_code = %s ORDER BY upload_time DESC', (store_code,))

    history = [dict(row) for row in cursor.fetchall()]
    cursor.close()
    for h in history:
        h['upload_time'] = format_vi_datetime(h['upload_time'])
    return jsonify({'success': True, 'history': history})


@app.route('/api/admin/users', methods=['GET', 'POST'])
def admin_users():
    if 'user' not in session or session['role'] != 'admin':
        return jsonify({'error': 'Forbidden'}), 403

    db = get_db()
    cursor = db.cursor()
    if request.method == 'POST':
        data = request.json
        username = data.get('username')
        new_password = data.get('password')
        if username and new_password:
            hashed_password = generate_password_hash(new_password)
            cursor.execute("UPDATE users SET password = %s WHERE username = %s", (hashed_password, username))
            db.commit()
            cursor.close()
            return jsonify({'success': True})
        cursor.close()
        return jsonify({'error': 'Thiếu thông tin'}), 400

    cursor.execute("SELECT username, role, store_code FROM users")
    users = [dict(row) for row in cursor.fetchall()]
    cursor.close()
    return jsonify({'success': True, 'users': users})


@app.route('/api/admin/db-size', methods=['GET'])
def admin_db_size():
    """Trả về dung lượng tổng của database và dung lượng từng bảng chính,
    để admin theo dõi mức sử dụng so với hạn mức 500MB của gói Free
    Supabase ngay trên giao diện, không cần vào SQL Editor."""
    if 'user' not in session or session['role'] != 'admin':
        return jsonify({'error': 'Forbidden'}), 403

    db = get_db()
    cursor = db.cursor()

    cursor.execute("SELECT pg_database_size(current_database()) AS bytes")
    total_bytes = cursor.fetchone()['bytes']

    cursor.execute('''
        SELECT
            relname AS table_name,
            pg_total_relation_size(relid) AS bytes,
            n_live_tup AS row_count
        FROM pg_stat_user_tables
        ORDER BY pg_total_relation_size(relid) DESC
    ''')
    tables = [dict(row) for row in cursor.fetchall()]
    cursor.close()

    # Hạn mức gói Free của Supabase (500 MB) - đổi số này nếu bạn nâng gói.
    LIMIT_BYTES = 500 * 1024 * 1024

    return jsonify({
        'success': True,
        'total_bytes': total_bytes,
        'limit_bytes': LIMIT_BYTES,
        'percent_used': round(total_bytes / LIMIT_BYTES * 100, 2),
        'tables': tables
    })


@app.route('/api/admin/delete-store-data', methods=['POST'])
def delete_store_data():
    """Xoá SẠCH toàn bộ dữ liệu PO đã lưu (Danh sách PO, Chi tiết PO, Chi
    tiết nhận hàng và lịch sử tải lên) của MỘT chi nhánh cụ thể.
    Không xoá tài khoản đăng nhập (bảng users) của chi nhánh đó."""
    if 'user' not in session or session['role'] != 'admin':
        return jsonify({'error': 'Forbidden'}), 403

    data = request.json or {}
    store_code = (data.get('store_code') or '').strip()

    db = get_db()
    cursor = db.cursor()

    # Chỉ cho phép xoá các chi nhánh (role = 'store') có thật trong hệ thống,
    # tránh xoá nhầm do dữ liệu gửi lên sai hoặc bị chỉnh sửa.
    cursor.execute("SELECT DISTINCT store_code FROM users WHERE role = 'store'")
    valid_stores = {r['store_code'] for r in cursor.fetchall()}

    if not store_code or store_code not in valid_stores:
        cursor.close()
        return jsonify({'error': 'Cửa hàng không hợp lệ'}), 400

    cursor.execute("DELETE FROM po_detail_items WHERE store_code = %s", (store_code,))
    cursor.execute("DELETE FROM latest_uploads WHERE store_code = %s", (store_code,))
    cursor.execute("DELETE FROM upload_log WHERE store_code = %s", (store_code,))
    db.commit()
    cursor.close()

    return jsonify({'success': True, 'store_code': store_code})




# ----------------------------------------------------------------------------
# LUÂN CHUYỂN NỘI BỘ GIỮA CÁC CỬA HÀNG
# ----------------------------------------------------------------------------

def _valid_store_codes(cursor):
    """Danh sách mã cửa hàng hợp lệ (role='store'), dùng để chặn việc gửi
    yêu cầu tới một 'cửa hàng' không tồn tại trong hệ thống."""
    cursor.execute("SELECT DISTINCT store_code FROM users WHERE role = 'store'")
    return {r['store_code'] for r in cursor.fetchall()}


def _fetch_transfer_items(cursor, request_ids):
    """Lấy toàn bộ dòng mã hàng (transfer_items) thuộc danh sách phiếu
    request_ids, trả về dict {request_id: [item, ...]} để gắn vào từng
    phiếu khi serialize - tránh N+1 query (1 query DUY NHẤT cho mọi phiếu)."""
    if not request_ids:
        return {}
    cursor.execute(
        'SELECT * FROM transfer_items WHERE request_id = ANY(%s) ORDER BY id',
        (list(request_ids),)
    )
    out = {}
    for it in cursor.fetchall():
        d = dict(it)
        if d.get('quantity') is not None:
            d['quantity'] = float(d['quantity'])
        if d.get('received_at'):
            d['received_at'] = format_vi_datetime(d['received_at'])
        out.setdefault(d['request_id'], []).append(d)
    return out


def _serialize_transfer_row(row, items, session_store, role):
    d = dict(row)
    if d.get('created_at'):
        d['created_at'] = format_vi_datetime(d['created_at'])
    if d.get('responded_at'):
        d['responded_at'] = format_vi_datetime(d['responded_at'])
    d['items'] = items
    d['item_count'] = len(items)
    d['total_quantity'] = sum((it['quantity'] or 0) for it in items)
    d['all_received'] = bool(items) and all(it['received'] for it in items)
    d['any_received'] = any(it['received'] for it in items)
    # Gắn nhãn chiều của yêu cầu so với cửa hàng đang đăng nhập, để frontend
    # tách hiển thị "Đã gửi" / "Nhận được" mà không cần tự so sánh store_code.
    if role == 'store':
        d['direction'] = 'sent' if d['from_store'] == session_store else 'received'
    else:
        d['direction'] = None
    return d


@app.route('/api/transfer/list', methods=['GET'])
def transfer_list():
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

    role = session['role']
    store_code = session['store_code']

    db = get_db()
    cursor = db.cursor()

    if role == 'admin':
        filter_store = request.args.get('store')
        if filter_store and filter_store != 'ALL':
            cursor.execute('''
                SELECT * FROM transfer_requests
                WHERE from_store = %s OR to_store = %s
                ORDER BY updated_at DESC
            ''', (filter_store, filter_store))
        else:
            cursor.execute('SELECT * FROM transfer_requests ORDER BY updated_at DESC')
    else:
        cursor.execute('''
            SELECT * FROM transfer_requests
            WHERE from_store = %s OR to_store = %s
            ORDER BY updated_at DESC
        ''', (store_code, store_code))

    rows = cursor.fetchall()
    items_by_request = _fetch_transfer_items(cursor, [r['id'] for r in rows])
    cursor.close()

    requests_out = [
        _serialize_transfer_row(r, items_by_request.get(r['id'], []), store_code, role)
        for r in rows
    ]
    return jsonify({'success': True, 'requests': requests_out})


def _create_transfer_request(cursor, from_store, to_store, note, created_by, items):
    """Tạo 1 phiếu luân chuyển + toàn bộ dòng mã hàng của phiếu đó trong
    CÙNG 1 transaction. `items` là list dict {part_code, part_name, quantity}
    đã được làm sạch/kiểm tra hợp lệ từ trước. Trả về id phiếu vừa tạo."""
    now = vn_now()
    cursor.execute('''
        INSERT INTO transfer_requests (from_store, to_store, note, status, created_by, created_at, updated_at)
        VALUES (%s, %s, %s, 'pending', %s, %s, %s)
        RETURNING id
    ''', (from_store, to_store, note, created_by, now, now))
    new_id = cursor.fetchone()['id']
    execute_values(
        cursor,
        '''INSERT INTO transfer_items (request_id, part_code, part_name, quantity)
           VALUES %s''',
        [(new_id, it['part_code'], it.get('part_name'), it['quantity']) for it in items]
    )
    return new_id


def _clean_transfer_items(raw_items):
    """Chuẩn hoá + kiểm tra danh sách mã hàng gửi lên (từ form nhập tay hoặc
    từ file Excel import). Trả về (items_sạch, lỗi_hoặc_None). Nếu cùng 1 mã
    hàng xuất hiện nhiều lần thì cộng dồn số lượng lại thay vì tạo 2 dòng."""
    if not raw_items or not isinstance(raw_items, list):
        return None, 'Vui lòng nhập ít nhất 1 mã hàng cần xin.'

    merged = {}
    order = []
    for it in raw_items:
        part_code = str((it.get('part_code') or '')).strip()
        if not part_code:
            continue
        try:
            qty = float(it.get('quantity'))
        except (TypeError, ValueError):
            qty = None
        if qty is None or qty <= 0:
            return None, f'Số lượng không hợp lệ cho mã hàng "{part_code}".'
        part_name = (it.get('part_name') or '').strip() or None
        key = part_code.upper()
        if key in merged:
            merged[key]['quantity'] += qty
            if not merged[key]['part_name'] and part_name:
                merged[key]['part_name'] = part_name
        else:
            merged[key] = {'part_code': part_code, 'part_name': part_name, 'quantity': qty}
            order.append(key)

    if not merged:
        return None, 'Vui lòng nhập ít nhất 1 mã hàng cần xin hợp lệ.'
    return [merged[k] for k in order], None


@app.route('/api/transfer/create', methods=['POST'])
def transfer_create():
    """Cửa hàng tạo 1 phiếu xin luân chuyển - có thể chứa NHIỀU mã hàng
    cùng lúc (mỗi mã hàng kèm số lượng riêng), gửi tới 1 cửa hàng khác."""
    if 'user' not in session or session['role'] != 'store':
        return jsonify({'error': 'Chỉ tài khoản cửa hàng mới được tạo yêu cầu luân chuyển.'}), 403

    data = request.json or {}
    from_store = session['store_code']
    to_store = (data.get('to_store') or '').strip().upper()
    note = (data.get('note') or '').strip() or None

    items, err = _clean_transfer_items(data.get('items'))
    if err:
        return jsonify({'error': err}), 400

    if not to_store:
        return jsonify({'error': 'Vui lòng chọn cửa hàng cần xin.'}), 400
    if to_store == from_store:
        return jsonify({'error': 'Không thể tự xin luân chuyển từ chính cửa hàng của mình.'}), 400

    db = get_db()
    cursor = db.cursor()

    if to_store not in _valid_store_codes(cursor):
        cursor.close()
        return jsonify({'error': 'Cửa hàng cần xin không hợp lệ.'}), 400

    new_id = _create_transfer_request(cursor, from_store, to_store, note, session['user'], items)
    db.commit()
    cursor.close()

    return jsonify({'success': True, 'id': new_id, 'item_count': len(items)})


@app.route('/api/transfer/import-excel', methods=['POST'])
def transfer_import_excel():
    """Cửa hàng tạo 1 phiếu xin luân chuyển bằng cách IMPORT danh sách mã
    hàng + số lượng từ 1 file Excel (thay vì gõ tay từng dòng) - tiện cho
    trường hợp cần xin nhiều mã hàng cùng lúc từ 1 cửa hàng."""
    if 'user' not in session or session['role'] != 'store':
        return jsonify({'error': 'Chỉ tài khoản cửa hàng mới được tạo yêu cầu luân chuyển.'}), 403

    from_store = session['store_code']
    to_store = (request.form.get('to_store') or '').strip().upper()
    note = (request.form.get('note') or '').strip() or None
    excel_file = request.files.get('file')

    if not excel_file:
        return jsonify({'error': 'Vui lòng chọn file Excel danh sách mã hàng cần xin.'}), 400
    if not to_store:
        return jsonify({'error': 'Vui lòng chọn cửa hàng cần xin.'}), 400
    if to_store == from_store:
        return jsonify({'error': 'Không thể tự xin luân chuyển từ chính cửa hàng của mình.'}), 400

    try:
        df = read_any(excel_file)
    except Exception as e:
        return jsonify({'error': f'Không đọc được file Excel: {e}'}), 400

    part_col = find_col(df.columns, ['mã phụ tùng', 'mã hàng', 'part code', 'part#', 'part #', 'part number', 'part'])
    qty_col = find_col(df.columns, ['số lượng yêu cầu', 'số lượng', 'sl', 'quantity'])
    name_col = find_col(df.columns, ['tên hàng', 'tên phụ tùng', 'part name', 'description', 'tên'])

    if not part_col or not qty_col:
        return jsonify({'error': 'Không tìm thấy cột "Mã hàng"/"Mã phụ tùng" và "Số lượng" trong file. Vui lòng kiểm tra lại file Excel.'}), 400

    raw_items = []
    skipped = 0
    for _, r in df.iterrows():
        part_code = str(r.get(part_col, '') or '').strip()
        if not part_code or part_code.lower() == 'nan':
            continue
        try:
            qty = float(r.get(qty_col))
            if math.isnan(qty):
                raise ValueError()
        except (TypeError, ValueError):
            skipped += 1
            continue
        if qty <= 0:
            skipped += 1
            continue
        part_name = str(r.get(name_col, '') or '').strip() if name_col else ''
        if part_name.lower() == 'nan':
            part_name = ''
        raw_items.append({'part_code': part_code, 'part_name': part_name, 'quantity': qty})

    items, err = _clean_transfer_items(raw_items)
    if err:
        return jsonify({'error': f'File không có dòng dữ liệu hợp lệ nào. {err}'}), 400

    db = get_db()
    cursor = db.cursor()

    if to_store not in _valid_store_codes(cursor):
        cursor.close()
        return jsonify({'error': 'Cửa hàng cần xin không hợp lệ.'}), 400

    new_id = _create_transfer_request(cursor, from_store, to_store, note, session['user'], items)
    db.commit()
    cursor.close()

    return jsonify({'success': True, 'id': new_id, 'item_count': len(items), 'skipped_rows': skipped})


@app.route('/api/transfer/respond', methods=['POST'])
def transfer_respond():
    """Cửa hàng được xin (to_store) phản hồi CẢ PHIẾU (mọi mã hàng trong
    phiếu): Đồng ý hoặc Từ chối (kèm lý do). Không còn bước "Đã soạn/Chưa
    soạn" trung gian - đồng ý là đồng ý ngay."""
    if 'user' not in session or session['role'] != 'store':
        return jsonify({'error': 'Chỉ tài khoản cửa hàng mới được phản hồi yêu cầu.'}), 403

    data = request.json or {}
    req_id = data.get('id')
    action = data.get('action')
    reason = (data.get('reason') or '').strip()

    if not req_id or action not in ('approve', 'reject'):
        return jsonify({'error': 'Dữ liệu không hợp lệ.'}), 400
    if action == 'reject' and not reason:
        return jsonify({'error': 'Vui lòng nhập lý do từ chối.'}), 400

    db = get_db()
    cursor = db.cursor()
    cursor.execute('SELECT * FROM transfer_requests WHERE id = %s', (req_id,))
    row = cursor.fetchone()

    if not row:
        cursor.close()
        return jsonify({'error': 'Không tìm thấy yêu cầu.'}), 404
    if row['to_store'] != session['store_code']:
        cursor.close()
        return jsonify({'error': 'Bạn không có quyền xử lý yêu cầu này.'}), 403
    if row['status'] not in ('pending', 'approved', 'rejected'):
        cursor.close()
        return jsonify({'error': 'Yêu cầu này không còn ở trạng thái có thể xử lý.'}), 400

    now = vn_now()
    if action == 'approve':
        cursor.execute('''
            UPDATE transfer_requests
            SET status = 'approved', reject_reason = NULL,
                responded_by = %s, responded_at = %s, updated_at = %s
            WHERE id = %s
        ''', (session['user'], now, now, req_id))
    else:
        cursor.execute('''
            UPDATE transfer_requests
            SET status = 'rejected', reject_reason = %s,
                responded_by = %s, responded_at = %s, updated_at = %s
            WHERE id = %s
        ''', (reason, session['user'], now, now, req_id))

    db.commit()
    cursor.close()
    return jsonify({'success': True})


@app.route('/api/transfer/revert', methods=['POST'])
def transfer_revert():
    """Cửa hàng được xin (to_store) ĐỔI LẠI lựa chọn đã đồng ý/từ chối
    trước đó, đưa phiếu về trạng thái "Chờ Xử Lý" để chọn lại. Chỉ cho phép
    khi CHƯA có mã hàng nào trong phiếu được đánh dấu đã nhận hàng (nếu
    hàng đã nhận thực tế rồi thì không thể đổi ý được nữa)."""
    if 'user' not in session or session['role'] != 'store':
        return jsonify({'error': 'Chỉ tài khoản cửa hàng mới được thao tác.'}), 403

    data = request.json or {}
    req_id = data.get('id')
    if not req_id:
        return jsonify({'error': 'Dữ liệu không hợp lệ.'}), 400

    db = get_db()
    cursor = db.cursor()
    cursor.execute('SELECT * FROM transfer_requests WHERE id = %s', (req_id,))
    row = cursor.fetchone()

    if not row:
        cursor.close()
        return jsonify({'error': 'Không tìm thấy yêu cầu.'}), 404
    if row['to_store'] != session['store_code']:
        cursor.close()
        return jsonify({'error': 'Bạn không có quyền thao tác yêu cầu này.'}), 403
    if row['status'] not in ('approved', 'rejected'):
        cursor.close()
        return jsonify({'error': 'Chỉ có thể đổi lại lựa chọn cho yêu cầu đã đồng ý hoặc đã từ chối.'}), 400

    cursor.execute('SELECT COUNT(*) AS c FROM transfer_items WHERE request_id = %s AND received = TRUE', (req_id,))
    if cursor.fetchone()['c'] > 0:
        cursor.close()
        return jsonify({'error': 'Cửa hàng xin đã nhận một phần hàng của phiếu này, không thể đổi lại lựa chọn nữa.'}), 400

    now = vn_now()
    cursor.execute('''
        UPDATE transfer_requests
        SET status = 'pending', reject_reason = NULL, responded_by = NULL, responded_at = NULL, updated_at = %s
        WHERE id = %s
    ''', (now, req_id))
    db.commit()
    cursor.close()
    return jsonify({'success': True})


@app.route('/api/transfer/mark-received', methods=['POST'])
def transfer_mark_received():
    """Cửa hàng đã gửi yêu cầu (from_store) tick "Đã nhận hàng" cho 1 mã
    hàng cụ thể trong phiếu đã được đồng ý. Khi tick, ghi chú tô màu tồn
    kho (đỏ/xanh lá) của đúng mã hàng đó ở cả 2 cửa hàng sẽ tự biến mất."""
    if 'user' not in session or session['role'] != 'store':
        return jsonify({'error': 'Chỉ tài khoản cửa hàng mới được thao tác.'}), 403

    data = request.json or {}
    item_id = data.get('item_id')
    received = data.get('received', True)
    if not item_id:
        return jsonify({'error': 'Dữ liệu không hợp lệ.'}), 400

    db = get_db()
    cursor = db.cursor()
    cursor.execute('''
        SELECT ti.*, tr.from_store, tr.status AS request_status
        FROM transfer_items ti JOIN transfer_requests tr ON tr.id = ti.request_id
        WHERE ti.id = %s
    ''', (item_id,))
    row = cursor.fetchone()

    if not row:
        cursor.close()
        return jsonify({'error': 'Không tìm thấy mã hàng trong phiếu.'}), 404
    if row['from_store'] != session['store_code']:
        cursor.close()
        return jsonify({'error': 'Bạn không có quyền thao tác mã hàng này.'}), 403
    if row['request_status'] != 'approved':
        cursor.close()
        return jsonify({'error': 'Chỉ có thể đánh dấu nhận hàng cho phiếu đã được đồng ý.'}), 400

    now = vn_now()
    cursor.execute(
        'UPDATE transfer_items SET received = %s, received_at = %s WHERE id = %s',
        (bool(received), now if received else None, item_id)
    )
    # Cũng cập nhật updated_at của phiếu cha để cơ chế poll-version (dựa
    # trên MAX(updated_at) của transfer_requests) phát hiện được thay đổi.
    cursor.execute('UPDATE transfer_requests SET updated_at = %s WHERE id = %s', (now, row['request_id']))
    db.commit()
    cursor.close()
    return jsonify({'success': True})


@app.route('/api/transfer/cancel', methods=['POST'])
def transfer_cancel():
    """Cửa hàng đã gửi (from_store) tự huỷ yêu cầu của mình khi còn đang chờ xử lý."""
    if 'user' not in session or session['role'] != 'store':
        return jsonify({'error': 'Chỉ tài khoản cửa hàng mới được huỷ yêu cầu.'}), 403

    data = request.json or {}
    req_id = data.get('id')
    if not req_id:
        return jsonify({'error': 'Dữ liệu không hợp lệ.'}), 400

    db = get_db()
    cursor = db.cursor()
    cursor.execute('SELECT * FROM transfer_requests WHERE id = %s', (req_id,))
    row = cursor.fetchone()

    if not row:
        cursor.close()
        return jsonify({'error': 'Không tìm thấy yêu cầu.'}), 404
    if row['from_store'] != session['store_code']:
        cursor.close()
        return jsonify({'error': 'Bạn không có quyền huỷ yêu cầu này.'}), 403
    if row['status'] != 'pending':
        cursor.close()
        return jsonify({'error': 'Chỉ có thể huỷ yêu cầu đang chờ xử lý.'}), 400

    cursor.execute("UPDATE transfer_requests SET status = 'cancelled', updated_at = %s WHERE id = %s", (vn_now(), req_id))
    db.commit()
    cursor.close()
    return jsonify({'success': True})


@app.route('/api/transfer/highlights', methods=['GET'])
def transfer_highlights():
    """Trả về dữ liệu để tô màu bảng Tồn Kho Hệ Thống theo các phiếu luân
    chuyển ĐÃ ĐỒNG Ý nhưng CHƯA nhận hàng xong:
      - 'given': ở cửa hàng CHO (to_store) - cột tồn kho của mã hàng đó tại
        cửa hàng này sẽ tô ĐỎ, kèm danh sách "cửa hàng nào xin bao nhiêu".
      - 'receiving': ở cửa hàng XIN (from_store) - cột tồn kho của mã hàng
        đó tại cửa hàng này sẽ tô XANH LÁ, kèm "đang nhận từ cửa hàng nào,
        số lượng bao nhiêu".
    Khi 1 dòng mã hàng được tick "Đã nhận hàng" (received = TRUE) thì dòng
    đó không còn xuất hiện trong dữ liệu trả về nữa -> ghi chú màu tự mất."""
    if 'user' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

    db = get_db()
    cursor = db.cursor()
    cursor.execute('''
        SELECT tr.from_store, tr.to_store, ti.part_code, ti.quantity
        FROM transfer_items ti
        JOIN transfer_requests tr ON tr.id = ti.request_id
        WHERE tr.status = 'approved' AND ti.received = FALSE
    ''')
    rows = cursor.fetchall()
    cursor.close()

    given = {}      # given[part_code][to_store] = [{store, quantity}, ...]  (tô đỏ ở to_store)
    receiving = {}  # receiving[part_code][from_store] = [{store, quantity}, ...]  (tô xanh ở from_store)

    for r in rows:
        part_code = r['part_code']
        qty = float(r['quantity']) if r['quantity'] is not None else 0
        given.setdefault(part_code, {}).setdefault(r['to_store'], []).append(
            {'store': r['from_store'], 'quantity': qty}
        )
        receiving.setdefault(part_code, {}).setdefault(r['from_store'], []).append(
            {'store': r['to_store'], 'quantity': qty}
        )

    return jsonify({'success': True, 'given': given, 'receiving': receiving})


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)