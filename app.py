#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import io
import time
import datetime as dt
from typing import List, Dict, Any

import streamlit as st
import pandas as pd
import pymysql
from dotenv import load_dotenv

# ------------- ENV -------------
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "")

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")

# ------------- CONSTANTS -------------
ORDER_STATUS_DISPATCHED = 15

STORE_MAP = {
    "IDM": [1, 3],
    "TKS": [7, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 23, 25, 32, 34, 35, 77, 85, 92, 93],
    "ZMN": [4],
}

# ---- File-based logging (no DB needed) ----
APP_DIR = os.path.dirname(os.path.abspath(__file__))  # absolute base dir of this app
LOGS_DIR = os.path.join(APP_DIR, "logs")
LOG_FILE = os.path.join(LOGS_DIR, "download_logs.csv")

# Keep exactly what you had (hidden columns commented)
PRODUCT_KEYS_ORDER = [
    "Medications",
    "GenericName",
    # "Manufacturer Name",
    # "Indian Brand",
    "Packages",
    "Dosage",
    "Qty",
    # "Price",
    # "ProductTotal",
]

# ---- File-based logging (no DB needed) ----
LOGS_DIR = "logs"
LOG_FILE = os.path.join(LOGS_DIR, "download_logs.csv")

# ------------- DB UTILS (for main query, unchanged) -------------
def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

# ------------- FILE LOGGING UTILS (replace DB logging) -------------
def ensure_logs_file():
    """Create logs directory and CSV with header if not present."""
    os.makedirs(LOGS_DIR, exist_ok=True)
    if not os.path.exists(LOG_FILE):
        import csv
        with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["username", "store_key", "date_from", "date_to", "rows_count", "downloaded_at"])

def append_download_log(username: str, store_key: str, date_from: dt.date, date_to: dt.date, rows_count: int):
    """Append one download row to CSV."""
    ensure_logs_file()
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    import csv
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([username, store_key, f"{date_from} 00:00:00", f"{date_to} 23:59:59", rows_count, ts])

def fetch_logs_file(limit: int = 1000) -> pd.DataFrame:
    """Read CSV logs and return newest first."""
    ensure_logs_file()
    if not os.path.exists(LOG_FILE):
        return pd.DataFrame(columns=["username", "store_key", "date_from", "date_to", "rows_count", "downloaded_at"])
    df = pd.read_csv(LOG_FILE)
    # coerce types
    for c in ["date_from", "date_to", "downloaded_at"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    if "rows_count" in df.columns:
        df["rows_count"] = pd.to_numeric(df["rows_count"], errors="coerce")
    # newest first
    if "downloaded_at" in df.columns:
        df = df.sort_values("downloaded_at", ascending=False)
    return df.head(limit)

# ------------- PARSING -------------
def php_like_parse_order_products(product_string: str, split_key: str = "Medications") -> List[Dict[str, str]]:
    if not product_string:
        return []
    entries = [e.strip() for e in product_string.split("||")]
    kv = []
    pattern = re.compile(r"Label:(.*?),\s*Value:(.*)")
    for entry in entries:
        if not entry:
            continue
        m = pattern.match(entry)
        if not m:
            continue
        kv.append((m.group(1).strip(), m.group(2).strip()))

    products, current = [], {}
    for key, value in kv:
        if key == split_key and current:
            products.append(current)
            current = {}
        current[key] = value
    if current:
        products.append(current)
    return products

# ------------- DATA EXPANSION + SANITIZATION -------------
def expand_rows(order_rows: List[Dict[str, Any]], store_key: str = "IDM") -> pd.DataFrame:
    """
    - IDM: split on 'Medications'
    - TKS/ZMN: split on 'Packages' (fallback 'Product')
    - First row per order has order info; subsequent rows blank order columns
    - Adds S.No only on order rows (first product row per order)
    """
    expanded = []
    is_alt = store_key in ("TKS", "ZMN")
    serial_no = 1  # S.No counter only for first product rows

    for row in order_rows:
        raw = row.get("order_products", "") or ""
        if is_alt:
            products = php_like_parse_order_products(raw, split_key="Packages")
            if not products or all("Product" not in p for p in products):
                products = php_like_parse_order_products(raw, split_key="Product")
        else:
            products = php_like_parse_order_products(raw, split_key="Medications")

        if not products:
            # No products row – still treat as a single order row with S.No
            out = {
                "S.No": serial_no,
                "OrderDate": row.get("OrderDate"),
                "StoreOrderID": row.get("store_order_id"),
                "FullName": row.get("FullName"),
                "ShippingCountry": row.get("shipping_country"),
                "OrderTotal": row.get("total"),
                "CurrentOrderStatus": row.get("CurrentOrderStatus"),
                "DispatchedAt": row.get("dispatched_at"),
            }
            for k in PRODUCT_KEYS_ORDER:
                if is_alt:
                    out[k] = "-" if k in ("GenericName", "Manufacturer Name", "Indian Brand") else ""
                else:
                    out[k] = "-" if k in ("Manufacturer Name", "Indian Brand") else ""
            expanded.append(out)
            serial_no += 1
            continue

        first = True
        for p in products:
            base = {
                "S.No": serial_no if first else pd.NA,  # only first product row gets a serial
                "OrderDate": row.get("OrderDate") if first else "",
                "StoreOrderID": row.get("store_order_id") if first else "",
                "FullName": row.get("FullName") if first else "",
                "ShippingCountry": row.get("shipping_country") if first else "",
                "OrderTotal": row.get("total") if first else "",
                "CurrentOrderStatus": row.get("CurrentOrderStatus") if first else "",
                "DispatchedAt": row.get("dispatched_at") if first else "",
            }

            for k in PRODUCT_KEYS_ORDER:
                if is_alt:
                    if k == "Medications":
                        base[k] = p.get("Product", "")
                    elif k == "Packages":
                        base[k] = p.get("Packages", "")
                    elif k == "Dosage":
                        base[k] = p.get("Dosage", "")
                    elif k == "Qty":
                        base[k] = p.get("Quantity", "")
                    elif k in ("GenericName", "Manufacturer Name", "Indian Brand"):
                        base[k] = "-"
                    elif k == "Price":
                        base[k] = p.get("Unit Price", "")
                    elif k == "ProductTotal":
                        base[k] = p.get("Total", "")
                    else:
                        base[k] = p.get(k, "")
                else:
                    if k == "ProductTotal":
                        base[k] = p.get("Total", "")
                    elif k in ("Manufacturer Name", "Indian Brand"):
                        base[k] = "-"
                    else:
                        base[k] = p.get(k, "")

            expanded.append(base)
            if first:
                serial_no += 1
                first = False

    df = pd.DataFrame(expanded)
    # Column order (S.No first)
    cols = [
        "S.No",
        "OrderDate", "StoreOrderID", "FullName", "ShippingCountry",
        "OrderTotal", "CurrentOrderStatus", "DispatchedAt",
    ] + PRODUCT_KEYS_ORDER
    cols = [c for c in cols if c in df.columns]
    df = df[cols] if not df.empty else df
    return sanitize_for_arrow(df)

def sanitize_for_arrow(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # S.No as nullable integer
    if "S.No" in df.columns:
        df["S.No"] = df["S.No"].replace({"": pd.NA})
        df["S.No"] = pd.to_numeric(df["S.No"], errors="coerce").astype("Int64")

    # Treat blank strings as NA for numerics before casting
    for c in ["Qty", "Price", "ProductTotal", "OrderTotal"]:
        if c in df.columns:
            df[c] = df[c].replace({"": pd.NA})
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Datetime columns
    for c in ["OrderDate", "DispatchedAt"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # Strings for the rest
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype("string")

    return df

# ------------- QUERY -------------
def run_query(date_from: dt.date, date_to: dt.date, store_ids: list, store_key: str) -> pd.DataFrame:
    start_ts = f"{date_from.strftime('%Y-%m-%d')} 00:00:00"
    end_ts = f"{date_to.strftime('%Y-%m-%d')} 23:59:59"
    in_placeholders = ",".join(["%s"] * len(store_ids))

    # NOTE: ascending by p.dispatched_at per your request
    sql = (
        "SELECT "
        "o.date_added as OrderDate, "
        "som.store_order_id, "
        "CONCAT(o.firstname, ' ', o.lastname) AS FullName, "
        "o.shipping_country, "
        "o.total, "
        "CASE WHEN pos.name IS NOT NULL AND pos.name <> '' "
        "THEN CONCAT(os.name, ' / ', pos.name) ELSE os.name END AS CurrentOrderStatus, "
        "p.dispatched_at, "
        "("
        "SELECT GROUP_CONCAT("
        "CONCAT('Label:', IFNULL(ocl.name, ''), ', Value:', IFNULL(occ.col_value, '')) "
        "ORDER BY occ.col_row, occ.col_order SEPARATOR ' || '"
        ") FROM order_cart_col occ "
        "LEFT JOIN order_cart_label ocl ON ocl.order_cart_label_id = occ.order_cart_label_id "
        "WHERE occ.order_id = o.order_id"
        ") AS order_products "
        "FROM ("
        "SELECT oh.order_id, MAX(oh.date_added) AS dispatched_at "
        "FROM order_history oh "
        "WHERE oh.order_status_id = %s "
        "AND oh.date_added >= %s "
        "AND oh.date_added <= %s "
        "GROUP BY oh.order_id"
        ") p "
        "JOIN `order` o ON o.order_id = p.order_id "
        f"AND o.store_id IN ({in_placeholders}) "
        "LEFT JOIN ("
        "SELECT DISTINCT order_id, order_parallel_status_id "
        "FROM order_parallel_status "
        "WHERE order_parallel_status_id = %s"
        ") ops ON ops.order_id = o.order_id "
        "JOIN store_order_mapping som ON som.order_id = o.order_id AND som.store_id = o.store_id "
        "LEFT JOIN customer_type ct ON ct.customer_id = o.customer_id "
        "LEFT JOIN order_status os ON os.order_status_id = o.order_status_id "
        "LEFT JOIN order_status pos ON pos.order_status_id = ops.order_parallel_status_id "
        "WHERE (ct.type_id IS NULL OR ct.type_id != 3) "
        "AND (o.order_status_id = %s OR ops.order_id IS NOT NULL) "
        "ORDER BY p.dispatched_at ASC"  # ASCENDING
    )

    params = [ORDER_STATUS_DISPATCHED, start_ts, end_ts, *store_ids, ORDER_STATUS_DISPATCHED, ORDER_STATUS_DISPATCHED]

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    df_orders = pd.DataFrame(rows)
    return expand_rows(df_orders.to_dict(orient="records"), store_key=store_key)

def download_excel(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        # Excel should match the on-screen order (already ASC by query)
        df.to_excel(writer, index=False, sheet_name="Data")
    return output.getvalue()

# ------------- UI -------------
st.set_page_config(page_title="Orders Dashboard", layout="wide")

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "username" not in st.session_state:
    st.session_state.username = None
if "last_df" not in st.session_state:
    st.session_state.last_df = pd.DataFrame()
if "page" not in st.session_state:
    st.session_state.page = "Dashboard"
if "has_run" not in st.session_state:
    st.session_state.has_run = False

def login_form():
    st.title("🔐 Login")
    with st.form("login_form", clear_on_submit=False):
        u = st.text_input("Username", value="", autocomplete="username")
        p = st.text_input("Password", value="", type="password", autocomplete="current-password")
        submitted = st.form_submit_button("Sign in")
        if submitted:
            if u == ADMIN_USERNAME and p == ADMIN_PASSWORD:
                st.session_state.authenticated = True
                st.session_state.username = u
                st.success("Logged in successfully.")
                st.rerun()
            else:
                st.error("Invalid credentials.")

def make_display_df(df: pd.DataFrame) -> pd.DataFrame:
    """Pretty display copy: no None/NaT shown, datetimes as string, and numeric cols shown as strings (so no Arrow dtype issues)."""
    if df.empty:
        return df.copy()

    out = df.copy()

    # Format datetimes as strings for display
    for c in ["OrderDate", "DispatchedAt"]:
        if c in out.columns and pd.api.types.is_datetime64_any_dtype(out[c]):
            out[c] = out[c].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Pretty S.No (blank for NA, integers otherwise)
    if "S.No" in out.columns:
        out["S.No"] = out["S.No"].map(lambda v: "" if pd.isna(v) else f"{int(v)}")

    # Render numeric columns as strings for display to avoid mixing "" with numbers
    numeric_for_display = ["OrderTotal", "Qty", "Price", "ProductTotal"]
    for c in numeric_for_display:
        if c in out.columns:
            out[c] = out[c].map(lambda v: "" if pd.isna(v) else f"{v}")

    # Replace any remaining NA-ish with blanks
    out = out.replace({pd.NaT: "", None: ""}).fillna("")
    return out

def header_nav() -> str:
    # Header with nav buttons (no sidebar)
    left, mid, right1 = st.columns([3, 6, 1.2])
    with left:
        st.markdown("### 📊 Dispatched Orders")
    with right1:
        if st.button("Dashboard", use_container_width=True, type=("primary" if st.session_state.page=="Dashboard" else "secondary")):
            st.session_state.page = "Dashboard"
            st.rerun()
    return st.session_state.page

def dashboard():
    st.markdown("#### Dashboard")
    col1, col2, col3, col4 = st.columns([1.1,1.1,1,1.2])
    with col1:
        store_key = st.selectbox("Store", options=list(STORE_MAP.keys()), index=0, help="Select IDM, TKS, or ZMN")
    today = dt.date.today()
    with col2:
        date_from = st.date_input("From date", value=today)
    with col3:
        date_to = st.date_input("To date", value=today)
    with col4:
        page_size = st.number_input("Rows per page", min_value=10, max_value=2000, value=100, step=10)

    run_btn = st.button("Run Report", type="primary")

    if run_btn:
        with st.spinner("Fetching data..."):
            df = run_query(date_from, date_to, STORE_MAP[store_key], store_key)
            st.session_state.last_df = df
            st.session_state.has_run = True

    df = st.session_state.last_df

    if not df.empty:
        total_rows = len(df)
        total_pages = max((total_rows - 1) // page_size + 1, 1)
        page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)
        start = (page - 1) * page_size
        end = start + page_size
        page_df = df.iloc[start:end]

        st.caption(f"Showing rows {start+1}–{min(end, total_rows)} of {total_rows} (Page {page}/{total_pages})")
        # Pretty display: hide index, no None/NaT strings
        display_df = make_display_df(page_df)
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        excel_bytes = download_excel(df)
        download_key = f"dl-{int(time.time())}"
        clicked = st.download_button(
            "Download Excel (all rows, ascending by date)",
            data=excel_bytes,
            file_name=f"orders_{date_from}_{date_to}_{store_key}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=download_key,
        )

        if clicked:
            # Fall back to 'anonymous' if username wasn’t set for any reason
            user = (st.session_state.get("username") or "anonymous")
            try:
                append_download_log(
                    username=user,
                    store_key=store_key,
                    date_from=date_from,
                    date_to=date_to,
                    rows_count=total_rows,
                )
                st.toast("✅ Download logged", icon="💾")
                # No rerun here; we let the log write complete and keep the page state stable
            except Exception as e:
                st.error(f"Failed to write download log: {e}")

    else:
        # Show "No results found" only after a search attempt
        if st.session_state.has_run:
            st.warning("No results found.")
        else:
            st.info("Run the report to see results.")

def logs_page():
    st.markdown("#### Download Logs")
    st.caption(f"Log file path: {LOG_FILE}")
    try:
        ensure_logs_file()
        df = fetch_logs_file(limit=1000)
        if df.empty:
            st.info("No downloads logged yet.")
            return
        # Pretty display
        disp = df.copy()
        for c in ["date_from", "date_to", "downloaded_at"]:
            if c in disp.columns:
                disp[c] = pd.to_datetime(disp[c], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        disp = disp.replace({pd.NaT: "", None: ""}).fillna("")
        st.dataframe(disp, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Could not load logs: {e}")

# ------------- ROUTER -------------
if not st.session_state.authenticated:
    login_form()
else:
    ensure_logs_file()  # CSV logger setup
    page = header_nav()
    if page == "Dashboard":
        dashboard()
    else:
        logs_page()
