import os
import json
from datetime import datetime
import pandas as pd
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, g
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Nạp các biến bảo mật từ file .env
load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'honda_po_secret_key_secure_2026')

# Lấy chuỗi kết nối bảo mật từ biến môi trường
DATABASE_URL = os.environ.get('DATABASE_URL')

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
        
        # 2. Tạo bảng upload_history
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS upload_history (
                id SERIAL PRIMARY KEY,
                store_code VARCHAR(20) NOT NULL,
                upload_time TIMESTAMP NOT NULL,
                ds_po_filename TEXT,
                po_detail_filename TEXT,
                receipt_filename TEXT,
                summary_json TEXT,
                result_json TEXT
            )
        ''')
        
        # 3. Seed default users nếu chưa có
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

def read_any(file_storage):
    """
    Hàm đọc file thông minh: Tự động nhận diện file Excel hoặc tự động thử 
    các bảng mã của file CSV để chống triệt để lỗi 'utf-8 codec can't decode'.
    """
    filename = (file_storage.filename or "").lower()
    
    # Xử lý file Excel
    if filename.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(file_storage)
    
    # Xử lý file CSV với cơ chế dự phòng nhiều bảng mã khác nhau
    elif filename.endswith('.csv'):
        encodings = ['utf-8-sig', 'utf-8', 'latin1', 'cp1258', 'cp1252']
        df = None
        for enc in encodings:
            try:
                file_storage.seek(0)
                df = pd.read_csv(file_storage, encoding=enc)
                break
            except UnicodeDecodeError:
                continue
            except Exception:
                continue
        
        # Nếu vẫn không đọc được, dùng latin1 ép buộc đọc qua các ký tự lỗi
        if df is None:
            file_storage.seek(0)
            df = pd.read_csv(file_storage, encoding='latin1', errors='ignore')
    else:
        # Trường hợp định dạng khác, thử đọc như Excel trước, lỗi thì đọc như CSV
        try:
            file_storage.seek(0)
            df = pd.read_excel(file_storage)
        except Exception:
            file_storage.seek(0)
            df = pd.read_csv(file_storage, encoding='latin1', errors='ignore')

    df.columns = [str(c).strip() for c in df.columns]
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
            except:
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
        if not po_val: continue
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
        if pair_key in seen_pairs: continue
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

        res_df = process_data(ds_po_df, po_detail_df, receipt_df)
        data_dicts = res_df.to_dict(orient='records')
        
        summary = get_summary_from_data(data_dicts)

        upload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        result_json = json.dumps(data_dicts, ensure_ascii=False)
        summary_json = json.dumps(summary)

        db = get_db()
        cursor = db.cursor()
        cursor.execute('''
            INSERT INTO upload_history (store_code, upload_time, ds_po_filename, po_detail_filename, receipt_filename, summary_json, result_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (store_code, upload_time, ds_po_file.filename, po_detail_file.filename, receipt_file.filename, summary_json, result_json))
        db.commit()
        cursor.close()

        return jsonify({'success': True, 'data': data_dicts, 'summary': summary, 'upload_time': upload_time})
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

    if target_store == 'ALL' and role == 'admin':
        cursor.execute('''
            SELECT * FROM upload_history h1 
            WHERE upload_time = (SELECT MAX(upload_time) FROM upload_history h2 WHERE h2.store_code = h1.store_code)
        ''')
    else:
        cursor.execute('SELECT * FROM upload_history WHERE store_code = %s ORDER BY upload_time DESC LIMIT 1', (target_store,))

    rows = cursor.fetchall()
    cursor.close()
    
    combined_data = []
    for row in rows:
        if row['result_json']:
            items = json.loads(row['result_json'])
            for itm in items:
                itm['store_code'] = row['store_code']
                combined_data.append(itm)

    summary = get_summary_from_data(combined_data)

    return jsonify({
        'success': True,
        'data': combined_data,
        'summary': summary
    })

@app.route('/api/history', methods=['GET'])
def get_history():
    if 'user' not in session: return jsonify({'error': 'Unauthorized'}), 401
    store_code = session['store_code'] if session['role'] == 'store' else request.args.get('store', 'ALL')
    
    db = get_db()
    cursor = db.cursor()
    if store_code == 'ALL':
        cursor.execute('SELECT id, store_code, upload_time, ds_po_filename, po_detail_filename, receipt_filename FROM upload_history ORDER BY upload_time DESC LIMIT 50')
    else:
        cursor.execute('SELECT id, store_code, upload_time, ds_po_filename, po_detail_filename, receipt_filename FROM upload_history WHERE store_code = %s ORDER BY upload_time DESC', (store_code,))
    
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

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)