# Python Dashboard (Streamlit)

A minimal two-page dashboard (Login + Table) that executes your MySQL query, parses `order_products` into per-product rows, supports pagination, and exports to Excel.

## Features
- **Login page** using credentials from `.env`
- **DB connectivity** via `.env` (MySQL)
- **Filters**: From date, To date, Store dropdown (IDM/TKS/ZMN → mapped IDs)
- **Table view** with per-product expansion (first row includes order info, subsequent rows show only product columns)
- **Pagination** and **Excel export**

## Setup

1. **Create and activate venv (optional)**
   ```bash
   python -m venv .venv
   # Windows
   .venv\Scripts\activate
   # macOS/Linux
   source .venv/bin/activate
   ```

2. **Install requirements**
   ```bash
   pip install -r requirements.txt
   ```

3. **Create `.env`**
   Copy `.env.example` to `.env` and fill DB + admin creds.
   ```bash
   copy .env.example .env   # Windows
   # or
   cp .env.example .env     # macOS/Linux
   ```

4. **Run**
   ```bash
   streamlit run app.py
   ```

## Notes
- The SQL filters the dispatched status (`15`) between the selected dates, restricts to the chosen store IDs, and excludes `customer_type.type_id = 3`.
- If the table is large, the app paginates in the UI layer after expanding product rows.