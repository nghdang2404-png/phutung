import os
import json
from datetime import datetime
import pandas as pd
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, g
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv

# Nạp các biến bảo mật từ file .env
load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'honda_po_secret_key_secure_2026')

# Lấy chuỗi kết nối bảo mật từ biến môi trường
DATABASE_URL = os.environ.get('DATABASE_URL')

# Số ngày lưu trữ dữ liệu "Chi tiết PO" trước khi tự động dọn dẹp
PO_DETAIL_RETENTION_DAYS = 120


def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


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
        #    hưởng tới các dữ liệu khác.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS po_detail_items (
                id SERIAL PRIMARY KEY,
                store_code VARCHAR(20) NOT NULL,
                po_code VARCHAR(100) NOT NULL,
                part_code VARCHAR(100) NOT NULL,
                row_json TEXT NOT NULL,
                filename TEXT,
                upload_time TIMESTAMP NOT NULL
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_po_detail_store ON po_detail_items(store_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_po_detail_upload_time ON po_detail_items(upload_time)')

        # 5. Seed default users nếu chưa có
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
            cursor.executemany("INSERT INTO users (username, password, role, store_code) VALUES (%s, %s, %s, %s) ON CONFLICT (username) DO NOTHING", default_users)

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
    'quantity requested', 'ngày tạo đơn hàng mua', 'ngày tạo', 'ngày đặt',
    'ngày gửi đơn đặt hàng', 'trạng thái đơn hàng mua', 'trạng thái đơn hàng',
    'trạng thái po', 'mrn status', 'trạng thái', 'status', 'part',
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


def process_data(ds_po_df, po_detail_df, receipt_df):
    ds_po_df.columns = [str(c).strip() for c in ds_po_df.columns]
    po_detail_df.columns = [str(c).strip() for c in po_detail_df.columns]
    receipt_df.columns = [str(c).strip() for c in receipt_df.columns]

    ds_po_col = find_col(ds_po_df.columns, ['mã đơn hàng mua', 'mã đơn hàng', 'mã po', 'order number', 'po'])
    ds_date_col = find_col(ds_po_df.columns, ['ngày tạo đơn hàng mua', 'ngày tạo', 'ngày đặt', 'ngày gửi đơn đặt hàng', 'ngày'])
    ds_status_col = find_col(ds_po_df.columns, ['trạng thái đơn hàng mua', 'trạng thái đơn hàng', 'trạng thái po'])

    detail_po_col = find_col(po_detail_df.columns, ['order number', 'mã đơn hàng mua', 'mã đơn hàng', 'mã po', 'po'])
    detail_part_col = find_col(po_detail_df.columns, ['part#', 'part #', 'part number', 'mã phụ tùng', 'part'])

    rec_po_col = find_col(receipt_df.columns, ['siebel po number', 'po number', 'mã đơn hàng mua', 'mã đơn hàng', 'po'])
    rec_part_col = find_col(receipt_df.columns, ['part#', 'part #', 'part number', 'mã phụ tùng', 'part'])
    rec_status_col = find_col(receipt_df.columns, ['mrn status', 'trạng thái', 'status'])

    if not all([ds_po_col, ds_date_col]):
        raise ValueError("File Danh sách PO thiếu cột 'Mã PO' hoặc 'Ngày đặt'.")
    if not all([detail_po_col, detail_part_col]):
        raise ValueError("File Chi tiết PO thiếu cột 'Mã PO' hoặc 'Mã phụ tùng'.")
    if not all([rec_po_col, rec_part_col, rec_status_col]):
        raise ValueError("File Chi tiết nhận hàng thiếu cột 'Mã PO', 'Mã phụ tùng' hoặc 'Trạng thái'.")

    date_lookup = {}
    cancelled_pos = set()
    for _, row in ds_po_df.iterrows():
        po_val = clean_str(row[ds_po_col])
        if not po_val:
            continue
        try:
            order_date = pd.to_datetime(row[ds_date_col], dayfirst=True)
        except Exception:
            order_date = None

        if order_date is not None and not pd.isna(order_date):
            if po_val not in date_lookup or order_date < date_lookup[po_val]:
                date_lookup[po_val] = order_date

        if ds_status_col:
            if clean_str(row[ds_status_col]) == 'CANCELLED':
                cancelled_pos.add(po_val)

    receipt_lookup = {}
    for _, row in receipt_df.iterrows():
        po_val = clean_str(row[rec_po_col])
        part_val = clean_str(row[rec_part_col])
        status_val = clean_str(row[rec_status_col])
        if po_val and part_val:
            if (po_val, part_val) not in receipt_lookup or status_val == 'OPEN':
                receipt_lookup[(po_val, part_val)] = status_val

    today = datetime.now()
    seen_pairs = set()
    results = []

    for _, row in po_detail_df.iterrows():
        po_code = clean_str(row[detail_po_col])
        part_code = clean_str(row[detail_part_col])

        if not po_code or not part_code or po_code in cancelled_pos:
            continue

        pair_key = (po_code, part_code)
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)

        order_date = date_lookup.get(po_code)
        if order_date is not None:
            days_diff = (today - order_date).days
            date_str = order_date.strftime('%d/%m/%Y')
        else:
            days_diff = 0
            date_str = 'Chưa có dữ liệu'

        rec_status = receipt_lookup.get(pair_key)
        if rec_status == 'OPEN':
            final_status = 'Đang vận chuyển'
        elif rec_status == 'CLOSED':
            final_status = 'Đã nhận hàng'
        else:
            final_status = 'Nợ'

        show_days = final_status in ('Nợ', 'Đang vận chuyển')

        results.append({
            'po_code': po_code,
            'part_code': part_code,
            'order_date': date_str,
            'status': final_status,
            'days_debt': days_diff if show_days else 0
        })

    df = pd.DataFrame(results)
    if not df.empty:
        df['_sort'] = df.apply(lambda r: -r['days_debt'] if r['status'] in ('Nợ', 'Đang vận chuyển') else 0, axis=1)
        df = df.sort_values(by=['_sort', 'po_code']).drop(columns=['_sort']).reset_index(drop=True)

    return df


# ----------------------------------------------------------------------------
# Data storage helpers (thay thế / cộng dồn / dọn dẹp)
# ----------------------------------------------------------------------------

def cleanup_old_po_detail(cursor):
    """Xoá các dòng 'Chi tiết PO' đã được nhập quá PO_DETAIL_RETENTION_DAYS
    ngày. Các dữ liệu khác (Danh sách PO, Chi tiết nhận hàng, users...) không
    bị ảnh hưởng."""
    cursor.execute(
        "DELETE FROM po_detail_items WHERE upload_time < NOW() - INTERVAL '%s days'" % PO_DETAIL_RETENTION_DAYS
    )


def save_ds_po_and_receipt(cursor, store_code, ds_po_file, ds_po_df, receipt_file, receipt_df, upload_time):
    """Danh sách PO và Chi tiết nhận hàng: XOÁ SẠCH dữ liệu cũ của cửa hàng
    này và THAY THẾ hoàn toàn bằng dữ liệu mới."""
    ds_po_json = json.dumps(ds_po_df.to_dict(orient='records'), ensure_ascii=False, default=str)
    receipt_json = json.dumps(receipt_df.to_dict(orient='records'), ensure_ascii=False, default=str)

    cursor.execute('''
        INSERT INTO latest_uploads (store_code, ds_po_filename, ds_po_json, ds_po_upload_time,
                                     receipt_filename, receipt_json, receipt_upload_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (store_code) DO UPDATE SET
            ds_po_filename = EXCLUDED.ds_po_filename,
            ds_po_json = EXCLUDED.ds_po_json,
            ds_po_upload_time = EXCLUDED.ds_po_upload_time,
            receipt_filename = EXCLUDED.receipt_filename,
            receipt_json = EXCLUDED.receipt_json,
            receipt_upload_time = EXCLUDED.receipt_upload_time
    ''', (store_code, ds_po_file.filename, ds_po_json, upload_time,
          receipt_file.filename, receipt_json, upload_time))


def append_po_detail(cursor, store_code, po_detail_file, po_detail_df, upload_time):
    """Chi tiết PO: GHI THÊM vào dữ liệu cũ (không xoá gì cả). Việc dọn dẹp
    dữ liệu quá hạn 120 ngày được xử lý riêng ở cleanup_old_po_detail()."""
    detail_po_col = find_col(po_detail_df.columns, ['order number', 'mã đơn hàng mua', 'mã đơn hàng', 'mã po', 'po'])
    detail_part_col = find_col(po_detail_df.columns, ['part#', 'part #', 'part number', 'mã phụ tùng', 'part'])

    if not detail_po_col or not detail_part_col:
        raise ValueError("File Chi tiết PO thiếu cột 'Mã PO' hoặc 'Mã phụ tùng'.")

    rows_to_insert = []
    for _, row in po_detail_df.iterrows():
        po_code = clean_str(row[detail_po_col])
        part_code = clean_str(row[detail_part_col])
        if not po_code or not part_code:
            continue
        row_json = json.dumps(row.to_dict(), ensure_ascii=False, default=str)
        rows_to_insert.append((store_code, po_code, part_code, row_json, po_detail_file.filename, upload_time))

    if rows_to_insert:
        execute_values(
            cursor,
            '''INSERT INTO po_detail_items (store_code, po_code, part_code, row_json, filename, upload_time)
               VALUES %s''',
            rows_to_insert
        )


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

    # Lấy theo thứ tự upload_time giảm dần: bản ghi nhập SAU sẽ được ưu tiên
    # nếu có cùng cặp (Mã PO, Mã phụ tùng) xuất hiện ở nhiều lần nhập khác nhau.
    cursor.execute(
        "SELECT row_json FROM po_detail_items WHERE store_code = %s ORDER BY upload_time DESC",
        (store_code,)
    )
    detail_rows = cursor.fetchall()
    if not detail_rows:
        return None

    ds_po_df = pd.DataFrame(json.loads(latest['ds_po_json']))
    receipt_df = pd.DataFrame(json.loads(latest['receipt_json']))
    po_detail_df = pd.DataFrame([json.loads(r['row_json']) for r in detail_rows])

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


# ----------------------------------------------------------------------------
# Routes & Endpoints
# ----------------------------------------------------------------------------

@app.route('/')
def index():
    if 'user' not in session:
        return redirect(url_for('login'))
    return render_template('index.html', user=session['user'], role=session['role'], store_code=session['store_code'])


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username').strip()
        password = request.form.get('password').strip()

        db = get_db()
        cursor = db.cursor()
        cursor.execute("SELECT * FROM users WHERE username = %s AND password = %s", (username, password))
        user = cursor.fetchone()
        cursor.close()

        if user:
            session['user'] = user['username']
            session['role'] = user['role']
            session['store_code'] = user['store_code']
            return redirect(url_for('index'))
        else:
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

    if not ds_po_file or not po_detail_file or not receipt_file:
        return jsonify({'error': 'Vui lòng tải lên đầy đủ cả 3 file.'}), 400

    try:
        ds_po_df = read_any(ds_po_file)
        po_detail_df = read_any(po_detail_file)
        receipt_df = read_any(receipt_file)

        upload_time = datetime.now()
        upload_time_str = upload_time.strftime('%Y-%m-%d %H:%M:%S')

        db = get_db()
        cursor = db.cursor()

        # 1) Danh sách PO + Chi tiết nhận hàng: xoá sạch & thay thế
        save_ds_po_and_receipt(cursor, store_code, ds_po_file, ds_po_df, receipt_file, receipt_df, upload_time)

        # 2) Chi tiết PO: ghi thêm vào dữ liệu cũ
        append_po_detail(cursor, store_code, po_detail_file, po_detail_df, upload_time)

        # 3) Dọn dẹp dữ liệu Chi tiết PO đã quá 120 ngày (các dữ liệu khác giữ nguyên)
        cleanup_old_po_detail(cursor)

        # 4) Ghi log lượt tải (chỉ phục vụ hiển thị lịch sử)
        cursor.execute('''
            INSERT INTO upload_log (store_code, upload_time, ds_po_filename, po_detail_filename, receipt_filename)
            VALUES (%s, %s, %s, %s, %s)
        ''', (store_code, upload_time_str, ds_po_file.filename, po_detail_file.filename, receipt_file.filename))

        db.commit()

        # 5) Tính lại bảng đối soát mới nhất cho cửa hàng này
        data_dicts = compute_result_for_store(cursor, store_code)
        summary = get_summary_from_data(data_dicts)
        cursor.close()

        return jsonify({'success': True, 'data': data_dicts, 'summary': summary, 'upload_time': upload_time_str})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


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

        for sc in store_codes:
            items = compute_result_for_store(cursor, sc)
            for itm in items:
                itm['store_code'] = sc
                combined_data.append(itm)
    else:
        items = compute_result_for_store(cursor, target_store)
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
            cursor.execute("UPDATE users SET password = %s WHERE username = %s", (new_password, username))
            db.commit()
            cursor.close()
            return jsonify({'success': True})
        cursor.close()
        return jsonify({'error': 'Thiếu thông tin'}), 400

    cursor.execute("SELECT username, role, store_code FROM users")
    users = [dict(row) for row in cursor.fetchall()]
    cursor.close()
    return jsonify({'success': True, 'users': users})


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


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)