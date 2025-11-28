# api_server_enhanced.py
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import uvicorn
import os
from datetime import datetime
from typing import List, Dict, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from contextlib import contextmanager
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class DatabaseManager:
    def __init__(self, db_config: Dict = None):
        # Get database configuration from environment variables with fallbacks
        self.db_config = db_config or {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'sender_hub'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        # Validate that we have the minimum required credentials
        if not self.db_config['password']:
            print("❌ WARNING: Database password not set in environment variables!")
    
    def get_connection(self):
        """Get PostgreSQL connection"""
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            print(f"❌ Database connection failed: {str(e)}")
            print(f"🔧 Connection details: host={self.db_config['host']}, db={self.db_config['database']}, user={self.db_config['user']}")
            raise
    
    @contextmanager
    def get_cursor(self):
        """Context manager for database connections"""
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
            conn.close()
    
    def get_latest_domain_stats(self, domain: str) -> Optional[Dict]:
        """Get latest statistics for a domain (across all accounts)"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute('''
                    SELECT ds1.* FROM domain_stats ds1
                    INNER JOIN (
                        SELECT domain_name, MAX(timestamp) as max_timestamp
                        FROM domain_stats
                        WHERE domain_name = %s
                        GROUP BY domain_name
                    ) ds2 ON ds1.domain_name = ds2.domain_name AND ds1.timestamp = ds2.max_timestamp
                    WHERE ds1.domain_name = %s
                    ORDER BY ds1.timestamp DESC 
                    LIMIT 1
                ''', (domain, domain))
                
                row = cursor.fetchone()
                if row:
                    return dict(row)
                return None
                
        except Exception as e:
            print(f"Database error: {e}")
            return None
    
    def get_all_domains_stats(self, limit: int = 100) -> List[Dict]:
        """Get all domains statistics - only latest per domain"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute('''
                    SELECT ds1.* FROM domain_stats ds1
                    INNER JOIN (
                        SELECT domain_name, MAX(timestamp) as max_timestamp
                        FROM domain_stats
                        GROUP BY domain_name
                    ) ds2 ON ds1.domain_name = ds2.domain_name AND ds1.timestamp = ds2.max_timestamp
                    ORDER BY ds1.timestamp DESC 
                    LIMIT %s
                ''', (limit,))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            print(f"Database error: {e}")
            return []
    
    def get_accounts(self) -> List[Dict]:
        """Get all accounts that have been used for scraping"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute('''
                    SELECT email, name, last_used, total_sessions, is_active
                    FROM scraping_accounts 
                    ORDER BY last_used DESC
                ''')
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            print(f"Database error getting accounts: {e}")
            return []
    
    def get_domain_history(self, domain: str) -> List[Dict]:
        """Get complete history for a domain across all accounts"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute('''
                    SELECT * FROM domain_stats 
                    WHERE domain_name = %s
                    ORDER BY timestamp DESC
                ''', (domain,))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            print(f"Database error getting domain history: {e}")
            return []
    
    def get_domains_by_status(self, status: str) -> List[Dict]:
        """Get domains by status (Verified, Unverified, etc.)"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute('''
                    SELECT ds1.* FROM domain_stats ds1
                    INNER JOIN (
                        SELECT domain_name, MAX(timestamp) as max_timestamp
                        FROM domain_stats
                        GROUP BY domain_name
                    ) ds2 ON ds1.domain_name = ds2.domain_name AND ds1.timestamp = ds2.max_timestamp
                    WHERE ds1.status = %s
                    ORDER BY ds1.timestamp DESC
                ''', (status,))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            print(f"Database error getting domains by status: {e}")
            return []
    
    def get_domains_with_data(self) -> List[Dict]:
        """Get domains that have actual data (not 'No data')"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute('''
                    SELECT ds1.* FROM domain_stats ds1
                    INNER JOIN (
                        SELECT domain_name, MAX(timestamp) as max_timestamp
                        FROM domain_stats
                        GROUP BY domain_name
                    ) ds2 ON ds1.domain_name = ds2.domain_name AND ds1.timestamp = ds2.max_timestamp
                    WHERE ds1.has_data = TRUE
                    ORDER BY ds1.timestamp DESC
                ''')
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            print(f"Database error getting domains with data: {e}")
            return []

# Create FastAPI app
app = FastAPI(
    title="Yahoo Sender Hub API - Domain Focus",
    description="API for accessing Yahoo Sender Hub domain statistics - Domain-based filtering",
    version="2.1.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database with environment variables
db = DatabaseManager()

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Yahoo Sender Hub API - Domain Focus", 
        "status": "running",
        "timestamp": datetime.now().isoformat(),
        "endpoints": {
            "accounts": "/accounts",
            "domains": "/domains",
            "domain_details": "/domains/{domain_name}",
            "domain_history": "/domains/{domain_name}/history",
            "screenshot": "/screenshot/{domain_name}",
            "domains_by_status": "/domains/status/{status}",
            "domains_with_data": "/domains/with-data",
            "health": "/health",
            "summary": "/stats/summary"
        }
    }

@app.get("/accounts")
async def get_all_accounts():
    """Get all accounts used for scraping"""
    try:
        accounts = db.get_accounts()
        return {
            "accounts": accounts,
            "count": len(accounts)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/domains")
async def get_all_domains(
    limit: int = 100, 
    offset: int = 0
):
    """Get all domains statistics with pagination (domain-based)"""
    try:
        stats = db.get_all_domains_stats(limit + offset)
        # Simple pagination
        paginated_stats = stats[offset:offset + limit]
        
        return {
            "domains": paginated_stats,
            "pagination": {
                "total": len(stats),
                "limit": limit,
                "offset": offset,
                "has_more": len(stats) > offset + limit
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/domains/{domain_name}")
async def get_domain_stats(domain_name: str):
    """Get latest statistics for a specific domain (across all accounts)"""
    try:
        stats = db.get_latest_domain_stats(domain_name)
        if not stats:
            raise HTTPException(status_code=404, detail=f"Domain '{domain_name}' not found")
        return stats
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/domains/{domain_name}/history")
async def get_domain_history(domain_name: str):
    """Get complete history for a domain across all accounts"""
    try:
        history = db.get_domain_history(domain_name)
        if not history:
            raise HTTPException(status_code=404, detail=f"No history found for domain '{domain_name}'")
        
        return {
            "domain": domain_name,
            "history": history,
            "total_records": len(history)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/domains/status/{status}")
async def get_domains_by_status(status: str):
    """Get domains by status (Verified, Unverified, etc.)"""
    try:
        domains = db.get_domains_by_status(status)
        return {
            "status": status,
            "domains": domains,
            "count": len(domains)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/domains/with-data")
async def get_domains_with_data():
    """Get domains that have actual data (not 'No data')"""
    try:
        domains = db.get_domains_with_data()
        return {
            "domains": domains,
            "count": len(domains)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/screenshot/{domain_name}")
async def get_domain_screenshot(domain_name: str):
    """Get screenshot for a domain"""
    try:
        # Get domain stats to find screenshot path
        stats = db.get_latest_domain_stats(domain_name)
        if not stats:
            raise HTTPException(status_code=404, detail=f"Domain '{domain_name}' not found")
        
        screenshot_path = stats.get('screenshot_path')
        if not screenshot_path or not os.path.exists(screenshot_path):
            # Try to find the screenshot in the default location
            default_path = f"screenshots/{domain_name}_180_days.png"
            if os.path.exists(default_path):
                screenshot_path = default_path
            else:
                raise HTTPException(status_code=404, detail=f"Screenshot not found for domain '{domain_name}'")
        
        return FileResponse(
            screenshot_path, 
            media_type='image/png',
            filename=f"{domain_name}_screenshot.png"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        stats = db.get_all_domains_stats(1)
        accounts = db.get_accounts()
        db_status = "connected"
        
        # Check screenshots directory
        screenshots_dir = "screenshots"
        screenshots_exist = os.path.exists(screenshots_dir) and any(
            f.endswith('.png') for f in os.listdir(screenshots_dir)
        )
        
    except Exception as e:
        db_status = f"disconnected: {str(e)}"
        stats = []
        accounts = []
        screenshots_exist = False
    
    return {
        "status": "healthy", 
        "timestamp": datetime.now().isoformat(),
        "database": db_status,
        "domains_in_database": len(stats),
        "accounts_in_database": len(accounts),
        "screenshots_available": screenshots_exist
    }

@app.get("/stats/summary")
async def get_stats_summary():
    """Get summary statistics (domain-based)"""
    try:
        all_stats = db.get_all_domains_stats(1000)
        
        if not all_stats:
            return {
                "summary": {
                    "total_domains": 0,
                    "verified_domains": 0,
                    "domains_with_data": 0,
                    "domains_with_screenshots": 0,
                    "total_scrapes": 0,
                    "average_delivered": 0,
                    "average_complaint_rate": 0,
                },
                "latest_scrape": None
            }
        
        # Calculate summary
        domains_set = set()
        verified_count = 0
        has_data_count = 0
        has_screenshot_count = 0
        delivered_counts = []
        complaint_rates = []
        latest_timestamp = None
        
        for stat in all_stats:
            domain = stat.get('domain_name')
            if domain:
                domains_set.add(domain)
            
            if stat.get('verified'):
                verified_count += 1
            
            if stat.get('has_data'):
                has_data_count += 1
            
            if stat.get('screenshot_path'):
                has_screenshot_count += 1
            
            delivered = stat.get('delivered_count')
            if delivered is not None:
                delivered_counts.append(delivered)
            
            complaint = stat.get('complaint_rate')
            if complaint is not None:
                complaint_rates.append(complaint)
            
            # Track latest timestamp
            stat_timestamp = stat.get('timestamp')
            if stat_timestamp:
                if latest_timestamp is None or stat_timestamp > latest_timestamp:
                    latest_timestamp = stat_timestamp
        
        total_domains = len(domains_set)
        
        return {
            "summary": {
                "total_domains": total_domains,
                "verified_domains": verified_count,
                "domains_with_data": has_data_count,
                "domains_with_screenshots": has_screenshot_count,
                "total_scrapes": len(all_stats),
                "average_delivered": sum(delivered_counts) / len(delivered_counts) if delivered_counts else 0,
                "average_complaint_rate": sum(complaint_rates) / len(complaint_rates) if complaint_rates else 0,
            },
            "latest_scrape": latest_timestamp
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/search/domains")
async def search_domains(
    query: str = Query(..., description="Domain name search query"),
    limit: int = 10
):
    """Search domains by name"""
    try:
        all_stats = db.get_all_domains_stats(1000)  # Get more for searching
        matching_domains = []
        
        for stat in all_stats:
            domain_name = stat.get('domain_name', '').lower()
            if query.lower() in domain_name:
                matching_domains.append(stat)
                if len(matching_domains) >= limit:
                    break
        
        return {
            "query": query,
            "domains": matching_domains,
            "count": len(matching_domains)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def start_server(port: int = 8001):
    """Start the FastAPI server"""
    print(f"🚀 Starting Domain-Focused FastAPI Server on port {port}...")
    print(f"📊 API will be available at: http://localhost:{port}")
    print(f"📚 API Documentation: http://localhost:{port}/docs")
    print("💡 Press CTRL+C to stop the server")
    
    uvicorn.run(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    start_server()