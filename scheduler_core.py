import sqlite3
from datetime import date, datetime, time, timedelta
from typing import Iterable, List, Tuple

import pandas as pd

DB_PATH = "scheduler.db"


def init_schema(conn: sqlite3.Connection) -> sqlite3.Connection:
    """Create tables and migrate lightweight column additions."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS polls(
            poll_id TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            start_date TEXT,
            end_date TEXT,
            start_time TEXT,
            end_time TEXT,
            slot_minutes INTEGER,
            poll_password TEXT,
            final_start_ts TEXT,
            final_end_ts TEXT,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS options(
            option_id INTEGER PRIMARY KEY AUTOINCREMENT,
            poll_id TEXT,
            start_ts TEXT,
            end_ts TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS votes(
            vote_id INTEGER PRIMARY KEY AUTOINCREMENT,
            poll_id TEXT,
            voter_name TEXT,
            option_id INTEGER,
            available INTEGER,
            comment TEXT,
            voter_password TEXT,
            UNIQUE(poll_id, voter_name, option_id)
        )
        """
    )
    # graceful migrations if columns were missing
    try:
        conn.execute("ALTER TABLE polls ADD COLUMN poll_password TEXT")
    except Exception:
        pass
    for col in ("final_start_ts", "final_end_ts"):
        try:
            conn.execute(f"ALTER TABLE polls ADD COLUMN {col} TEXT")
        except Exception:
            pass
    try:
        conn.execute("ALTER TABLE votes ADD COLUMN voter_password TEXT")
    except Exception:
        pass
    return conn


def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    return init_schema(conn)


def generate_slots(
    start_d: date, end_d: date, start_t: time, end_t: time, minutes: int
) -> List[Tuple[datetime, datetime]]:
    slots: List[Tuple[datetime, datetime]] = []
    current_day = start_d
    while current_day <= end_d:
        cursor = datetime.combine(current_day, start_t)
        end_dt = datetime.combine(current_day, end_t)
        while cursor < end_dt:
            slot_end = min(cursor + timedelta(minutes=minutes), end_dt)
            slots.append((cursor, slot_end))
            cursor = slot_end
        current_day += timedelta(days=1)
    return slots


def slot_label(start_ts: str, end_ts: str) -> str:
    start = datetime.fromisoformat(start_ts)
    end = datetime.fromisoformat(end_ts)
    day_label = start.strftime("%m/%d (%a)")
    return f"{day_label} {start.strftime('%H:%M')} - {end.strftime('%H:%M')}"


def load_polls(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql("SELECT poll_id, title FROM polls ORDER BY created_at DESC", conn)

