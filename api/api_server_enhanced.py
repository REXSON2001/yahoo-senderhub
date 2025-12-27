# api_server_enhanced.py

from fastapi import FastAPI, HTTPException, Query, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import os
from datetime import datetime
from typing import List, Dict, Optional
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
from dotenv import load_dotenv
import logging

# ----------------------------------------
# Environment & logging
# ----------------------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------------------------------
# Database Manager (PRODUCTION SAFE)
# ----------------------------------------
class DatabaseManager:
    def __init__(self, db_config: Dict = None):
        self.db_config = db_config or {
            "host": os.getenv("DB_HOST", "localhost"),
            "database": os.getenv("DB_NAME", "sender_hub"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", ""),
            "port": os.getenv("DB_PORT", "5432"),
        }

        if not self.db_config["password"]:
            logging.warning("Database password not set!")

        self.pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            **self.db_config
        )

    def get_connection(self):
        return self.pool.getconn()

    @contextmanager
    def get_cursor(self):
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            self.pool.putconn(conn)

    # ---------- Queries ----------
    def get_latest_domain_stats(self, domain: str) -> Optional[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT ds1.* FROM domain_stats ds1
                INNER JOIN (
                    SELECT domain_name, MAX(timestamp) AS max_timestamp
                    FROM domain_stats
                    WHERE domain_name = %s
                    GROUP BY domain_name
                ) ds2
                ON ds1.domain_name = ds2.domain_name
                AND ds1.timestamp = ds2.max_timestamp
                LIMIT 1
            """, (domain,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_domains_stats(self, limit: int = 100) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT ds1.* FROM domain_stats ds1
                INNER JOIN (
                    SELECT domain_name, MAX(timestamp) AS max_timestamp
                    FROM domain_stats
                    GROUP BY domain_name
                ) ds2
                ON ds1.domain_name = ds2.domain_name
                AND ds1.timestamp = ds2.max_timestamp
                ORDER BY ds1.timestamp DESC
                LIMIT %s
            """, (limit,))
            return [dict(r) for r in cursor.fetchall()]

    def get_accounts(self) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT email, name, last_used, total_sessions, is_active
                FROM scraping_accounts
                ORDER BY last_used DESC
            """)
            return [dict(r) for r in cursor.fetchall()]

    def get_domain_history(self, domain: str) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM domain_stats
                WHERE domain_name = %s
                ORDER BY timestamp DESC
            """, (domain,))
            return [dict(r) for r in cursor.fetchall()]

    def get_domains_by_status(self, status: str) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT ds1.* FROM domain_stats ds1
                INNER JOIN (
                    SELECT domain_name, MAX(timestamp) AS max_timestamp
                    FROM domain_stats
                    GROUP BY domain_name
                ) ds2
                ON ds1.domain_name = ds2.domain_name
                AND ds1.timestamp = ds2.max_timestamp
                WHERE ds1.status = %s
            """, (status,))
            return [dict(r) for r in cursor.fetchall()]

    def get_domains_with_data(self) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT ds1.* FROM domain_stats ds1
                INNER JOIN (
                    SELECT domain_name, MAX(timestamp) AS max_timestamp
                    FROM domain_stats
                    GROUP BY domain_name
                ) ds2
                ON ds1.domain_name = ds2.domain_name
                AND ds1.timestamp = ds2.max_timestamp
                WHERE ds1.has_data = TRUE
            """)
            return [dict(r) for r in cursor.fetchall()]

# ----------------------------------------
# API Key protection (OPTIONAL BUT SAFE)
# ----------------------------------------
API_KEY = os.getenv("API_KEY")  # if not set → no auth

def verify_api_key(x_api_key: str = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

# ----------------------------------------
# FastAPI App
# ----------------------------------------
app = FastAPI(
    title="Yahoo Sender Hub API",
    version="2.1.0"
)

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = DatabaseManager()

# ----------------------------------------
# Endpoints
# ----------------------------------------
@app.get("/")
async def root():
    return {
        "status": "running",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/accounts", dependencies=[Depends(verify_api_key)])
async def accounts():
    return {"accounts": db.get_accounts()}

@app.get("/domains", dependencies=[Depends(verify_api_key)])
async def domains(limit: int = 100, offset: int = 0):
    data = db.get_all_domains_stats(limit + offset)
    return {
        "domains": data[offset:offset + limit],
        "total": len(data)
    }

@app.get("/domains/{domain_name}", dependencies=[Depends(verify_api_key)])
async def domain(domain_name: str):
    stats = db.get_latest_domain_stats(domain_name)
    if not stats:
        raise HTTPException(status_code=404, detail="Domain not found")
    return stats

@app.get("/domains/{domain_name}/history", dependencies=[Depends(verify_api_key)])
async def domain_history(domain_name: str):
    history = db.get_domain_history(domain_name)
    if not history:
        raise HTTPException(status_code=404, detail="No history found")
    return {"domain": domain_name, "history": history}

@app.get("/domains/status/{status}", dependencies=[Depends(verify_api_key)])
async def domains_by_status(status: str):
    return {"domains": db.get_domains_by_status(status)}

@app.get("/domains/with-data", dependencies=[Depends(verify_api_key)])
async def domains_with_data():
    return {"domains": db.get_domains_with_data()}

@app.get("/screenshot/{domain_name}", dependencies=[Depends(verify_api_key)])
async def screenshot(domain_name: str):
    stats = db.get_latest_domain_stats(domain_name)
    if not stats:
        raise HTTPException(status_code=404, detail="Domain not found")

    path = stats.get("screenshot_path") or f"screenshots/{domain_name}_180_days.png"
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Screenshot not found")

    return FileResponse(path, media_type="image/png")

@app.get("/health")
async def health():
    try:
        db.get_all_domains_stats(1)
        return {"status": "healthy", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
