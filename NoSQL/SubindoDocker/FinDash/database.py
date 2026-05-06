"""
FinDash — Database Layer
SQLite database operations for the financial dashboard.
"""

import sqlite3
import os
import uuid
import calendar

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'findash.db')

MONTHS_PT = [
    'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
    'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'
]


def generate_id():
    return uuid.uuid4().hex[:16]


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def dict_from_row(row):
    return dict(row) if row else None


def dicts_from_rows(rows):
    return [dict(r) for r in rows]


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS transactions (
            id TEXT PRIMARY KEY,
            date TEXT NOT NULL,
            description TEXT NOT NULL,
            category TEXT NOT NULL,
            type TEXT NOT NULL CHECK(type IN ('receita', 'despesa')),
            value REAL NOT NULL,
            recurring_id TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS recurring_transactions (
            id TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            category TEXT NOT NULL,
            type TEXT NOT NULL CHECK(type IN ('receita', 'despesa')),
            value REAL NOT NULL,
            day_of_month INTEGER NOT NULL CHECK(day_of_month BETWEEN 1 AND 31),
            active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS goals (
            category TEXT PRIMARY KEY,
            limit_value REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_tx_date ON transactions(date);
        CREATE INDEX IF NOT EXISTS idx_tx_recurring ON transactions(recurring_id);
    """)
    conn.commit()
    conn.close()


# ========================
# TRANSACTIONS
# ========================

def get_transactions(year=None, month=None):
    conn = get_db()
    if year is not None and month is not None:
        rows = conn.execute(
            "SELECT * FROM transactions WHERE strftime('%Y', date)=? AND strftime('%m', date)=? ORDER BY date ASC",
            (str(year), f"{month:02d}")
        ).fetchall()
    elif year is not None:
        rows = conn.execute(
            "SELECT * FROM transactions WHERE strftime('%Y', date)=? ORDER BY date ASC",
            (str(year),)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM transactions ORDER BY date DESC").fetchall()
    conn.close()
    return dicts_from_rows(rows)


def create_transaction(data):
    conn = get_db()
    tx_id = data.get('id') or generate_id()
    conn.execute(
        "INSERT INTO transactions (id, date, description, category, type, value, recurring_id) VALUES (?,?,?,?,?,?,?)",
        (tx_id, data['date'], data['description'], data['category'], data['type'], data['value'], data.get('recurring_id'))
    )
    conn.commit()
    tx = dict_from_row(conn.execute("SELECT * FROM transactions WHERE id=?", (tx_id,)).fetchone())
    conn.close()
    return tx


def update_transaction(tx_id, data):
    conn = get_db()
    conn.execute(
        "UPDATE transactions SET date=?, description=?, category=?, type=?, value=? WHERE id=?",
        (data['date'], data['description'], data['category'], data['type'], data['value'], tx_id)
    )
    conn.commit()
    tx = dict_from_row(conn.execute("SELECT * FROM transactions WHERE id=?", (tx_id,)).fetchone())
    conn.close()
    return tx


def delete_transaction(tx_id):
    conn = get_db()
    conn.execute("DELETE FROM transactions WHERE id=?", (tx_id,))
    conn.commit()
    conn.close()


# ========================
# RECURRING TRANSACTIONS
# ========================

def get_recurring():
    conn = get_db()
    rows = conn.execute("SELECT * FROM recurring_transactions ORDER BY day_of_month ASC").fetchall()
    conn.close()
    return dicts_from_rows(rows)


def create_recurring(data):
    conn = get_db()
    rec_id = generate_id()
    conn.execute(
        "INSERT INTO recurring_transactions (id, description, category, type, value, day_of_month, active) VALUES (?,?,?,?,?,?,?)",
        (rec_id, data['description'], data['category'], data['type'], data['value'], data['day_of_month'], data.get('active', 1))
    )
    conn.commit()
    rec = dict_from_row(conn.execute("SELECT * FROM recurring_transactions WHERE id=?", (rec_id,)).fetchone())
    conn.close()
    return rec


def update_recurring(rec_id, data):
    conn = get_db()
    fields, values = [], []
    for key in ['description', 'category', 'type', 'value', 'day_of_month', 'active']:
        if key in data:
            fields.append(f"{key}=?")
            values.append(data[key])
    if fields:
        values.append(rec_id)
        conn.execute(f"UPDATE recurring_transactions SET {', '.join(fields)} WHERE id=?", values)
        conn.commit()
    rec = dict_from_row(conn.execute("SELECT * FROM recurring_transactions WHERE id=?", (rec_id,)).fetchone())
    conn.close()
    return rec


def delete_recurring(rec_id):
    conn = get_db()
    conn.execute("DELETE FROM recurring_transactions WHERE id=?", (rec_id,))
    conn.commit()
    conn.close()


def generate_recurring(year, month):
    conn = get_db()
    recurring = conn.execute("SELECT * FROM recurring_transactions WHERE active=1").fetchall()
    generated = []
    days_in_month = calendar.monthrange(year, month)[1]
    month_str = f"{year}-{month:02d}"

    for rec in recurring:
        existing = conn.execute(
            "SELECT id FROM transactions WHERE recurring_id=? AND strftime('%Y-%m', date)=?",
            (rec['id'], month_str)
        ).fetchone()
        if existing:
            continue

        day = min(rec['day_of_month'], days_in_month)
        date_str = f"{year}-{month:02d}-{day:02d}"
        tx_id = generate_id()

        conn.execute(
            "INSERT INTO transactions (id, date, description, category, type, value, recurring_id) VALUES (?,?,?,?,?,?,?)",
            (tx_id, date_str, rec['description'], rec['category'], rec['type'], rec['value'], rec['id'])
        )
        generated.append({
            'id': tx_id, 'date': date_str, 'description': rec['description'],
            'category': rec['category'], 'type': rec['type'], 'value': rec['value'],
            'recurring_id': rec['id'],
        })

    conn.commit()
    conn.close()
    return generated


# ========================
# GOALS
# ========================

def get_goals():
    conn = get_db()
    rows = conn.execute("SELECT * FROM goals").fetchall()
    conn.close()
    return dicts_from_rows(rows)


def save_goals(goals_list):
    conn = get_db()
    conn.execute("DELETE FROM goals")
    for g in goals_list:
        if g.get('limit_value', 0) > 0:
            conn.execute("INSERT INTO goals (category, limit_value) VALUES (?,?)", (g['category'], g['limit_value']))
    conn.commit()
    conn.close()


# ========================
# ANNUAL SUMMARY
# ========================

def get_annual_summary(year):
    conn = get_db()
    year_str = str(year)
    months = []

    for m in range(1, 13):
        ms = f"{year}-{m:02d}"
        receitas = conn.execute(
            "SELECT COALESCE(SUM(value),0) FROM transactions WHERE type='receita' AND strftime('%Y-%m',date)=?", (ms,)
        ).fetchone()[0]
        despesas = conn.execute(
            "SELECT COALESCE(SUM(value),0) FROM transactions WHERE type='despesa' AND strftime('%Y-%m',date)=?", (ms,)
        ).fetchone()[0]
        saldo = receitas - despesas
        economia = ((receitas - despesas) / receitas * 100) if receitas > 0 else 0
        months.append({
            'month': m, 'name': MONTHS_PT[m - 1],
            'receitas': receitas, 'despesas': despesas,
            'saldo': saldo, 'economia': round(economia, 1),
        })

    top_categories = conn.execute("""
        SELECT category, COALESCE(SUM(value),0) as total
        FROM transactions WHERE type='despesa' AND strftime('%Y',date)=?
        GROUP BY category ORDER BY total DESC LIMIT 5
    """, (year_str,)).fetchall()

    conn.close()

    total_rec = sum(m['receitas'] for m in months)
    total_desp = sum(m['despesas'] for m in months)
    active = [m for m in months if m['receitas'] > 0 or m['despesas'] > 0]
    media_eco = (sum(m['economia'] for m in active) / len(active)) if active else 0

    return {
        'year': year, 'months': months,
        'top_categories': [{'category': r['category'], 'total': r['total']} for r in top_categories],
        'total_receitas': total_rec, 'total_despesas': total_desp,
        'saldo_anual': total_rec - total_desp, 'media_economia': round(media_eco, 1),
    }


# ========================
# EXPORT / IMPORT
# ========================

def export_all():
    conn = get_db()
    data = {
        'transactions': dicts_from_rows(conn.execute("SELECT * FROM transactions ORDER BY date").fetchall()),
        'recurring': dicts_from_rows(conn.execute("SELECT * FROM recurring_transactions").fetchall()),
        'goals': dicts_from_rows(conn.execute("SELECT * FROM goals").fetchall()),
    }
    conn.close()
    return data


def import_all(data):
    conn = get_db()
    if 'transactions' in data:
        for tx in data['transactions']:
            tx_id = tx.get('id') or generate_id()
            if not conn.execute("SELECT id FROM transactions WHERE id=?", (tx_id,)).fetchone():
                conn.execute(
                    "INSERT INTO transactions (id,date,description,category,type,value,recurring_id) VALUES (?,?,?,?,?,?,?)",
                    (tx_id, tx['date'], tx['description'], tx['category'], tx['type'], tx['value'], tx.get('recurring_id'))
                )
    if 'goals' in data:
        for g in data['goals']:
            cat = g.get('category')
            lim = g.get('limit_value') or g.get('limit')
            if cat and lim and float(lim) > 0:
                conn.execute("INSERT OR REPLACE INTO goals (category, limit_value) VALUES (?,?)", (cat, float(lim)))
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    conn.close()
    return count
