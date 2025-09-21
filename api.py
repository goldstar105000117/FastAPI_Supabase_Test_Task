from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
from typing import Optional, Dict, List
import os
import uvicorn
from dotenv import load_dotenv

load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="Publisher Statistics API",
    description="API for accessing distributed feed revenue data",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API key to traffic source mapping
API_KEYS = {
    "test_key_66": 66,
    "test_key_67": 67,
}

def get_db_connection():
    """Get database connection with proper error handling."""
    try:
        return psycopg2.connect(
            os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/revenue_db'),
            cursor_factory=RealDictCursor
        )
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {e}")

def validate_api_key(key: str) -> int:
    """Validate API key and return traffic source ID."""
    if key not in API_KEYS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return API_KEYS[key]

def validate_traffic_source(key_ts: int, param_ts: int):
    """Validate traffic source parameter matches API key."""
    if key_ts != param_ts:
        raise HTTPException(
            status_code=403, 
            detail=f"Traffic source mismatch. Key is for TS {key_ts}, but requested TS {param_ts}"
        )

def validate_date_format(date_str: str, field_name: str) -> date:
    """Validate and parse date string."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid {field_name} format. Use YYYY-MM-DD"
        )

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "Publisher Statistics API",
        "version": "1.0.0",
        "endpoints": {
            "/pubstats": "Get publisher statistics",
            "/health": "Health check",
            "/docs": "API documentation"
        },
        "example": "/pubstats?ts=66&from=2025-01-15&to=2025-01-20&key=test_key_66"
    }

@app.get("/pubstats")
async def get_publisher_stats(
    ts: int = Query(..., description="Traffic source ID", example=66),
    from_date: str = Query(..., alias="from", description="Start date (YYYY-MM-DD)", example="2025-01-15"), 
    to_date: str = Query(..., alias="to", description="End date (YYYY-MM-DD)", example="2025-01-20"),
    key: str = Query(..., description="API key", example="test_key_66")
):
    """
    Get publisher statistics for a given traffic source and date range.
    
    - **ts**: Traffic source ID (must match your API key)
    - **from**: Start date in YYYY-MM-DD format
    - **to**: End date in YYYY-MM-DD format  
    - **key**: Your API key
    
    Returns only records with pub_revenue > 0, sorted by date DESC, campaign_id ASC.
    """
    
    # Validate API key
    traffic_source_id = validate_api_key(key)
    
    # Validate traffic source parameter
    validate_traffic_source(traffic_source_id, ts)
    
    # Validate and parse dates
    start_date = validate_date_format(from_date, "start date")
    end_date = validate_date_format(to_date, "end date")
    
    if start_date > end_date:
        raise HTTPException(
            status_code=400, 
            detail="Start date must be before or equal to end date"
        )
    
    # Check for reasonable date range (optional safety check)
    date_diff = (end_date - start_date).days
    if date_diff > 365:
        raise HTTPException(
            status_code=400,
            detail="Date range cannot exceed 365 days"
        )
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Query distributed stats with filters
        cursor.execute("""
            SELECT 
                date,
                campaign_id,
                campaign_name,
                total_searches,
                monetized_searches,
                paid_clicks,
                ROUND(pub_revenue::numeric, 2) as revenue,
                fp_feed_id as feed_id
            FROM distributed_stats
            WHERE traffic_source_id = %s
              AND date >= %s 
              AND date <= %s
              AND is_feed_data = true
              AND pub_revenue > 0
            ORDER BY date DESC, campaign_id ASC
        """, (traffic_source_id, start_date, end_date))
        
        results = cursor.fetchall()
        
        # Convert to proper format for JSON serialization
        formatted_results = []
        for row in results:
            row_dict = dict(row)
            row_dict['date'] = row_dict['date'].isoformat()
            row_dict['revenue'] = float(row_dict['revenue'])
            formatted_results.append(row_dict)
        
        # Calculate summary statistics
        total_revenue = sum(row['revenue'] for row in formatted_results)
        total_searches = sum(row['total_searches'] for row in formatted_results)
        unique_campaigns = len(set(row['campaign_id'] for row in formatted_results))
        unique_feeds = len(set(row['feed_id'] for row in formatted_results))
        
        conn.close()
        
        return {
            "status": "success",
            "traffic_source_id": traffic_source_id,
            "date_range": {
                "from": from_date,
                "to": to_date
            },
            "summary": {
                "record_count": len(formatted_results),
                "total_revenue": round(total_revenue, 2),
                "total_searches": total_searches,
                "unique_campaigns": unique_campaigns,
                "unique_feeds": unique_feeds
            },
            "data": formatted_results
        }
        
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {e}")

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Test database connectivity
        cursor.execute("SELECT 1 as status")
        result = cursor.fetchone()
        
        # Test table existence
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('distributed_stats', 'campaign_clicks', 'feed_provider_data')
        """)
        tables = [row['table_name'] for row in cursor.fetchall()]
        
        # Get record counts
        cursor.execute("SELECT COUNT(*) as count FROM distributed_stats")
        stats_count = cursor.fetchone()['count']
        
        conn.close()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database": {
                "connected": True,
                "tables_found": tables,
                "distributed_stats_records": stats_count
            }
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }

@app.get("/debug/feeds")
async def debug_feeds(key: str = Query(..., description="API key")):
    """Debug endpoint to see available feeds (for development)."""
    # Validate API key
    traffic_source_id = validate_api_key(key)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT fp_feed_id, traffic_source_id, 
                   MIN(date) as first_date, MAX(date) as last_date,
                   COUNT(*) as record_count
            FROM distributed_stats
            WHERE traffic_source_id = %s
            GROUP BY fp_feed_id, traffic_source_id
            ORDER BY fp_feed_id
        """, (traffic_source_id,))
        
        results = cursor.fetchall()
        
        formatted_results = []
        for row in results:
            row_dict = dict(row)
            row_dict['first_date'] = row_dict['first_date'].isoformat()
            row_dict['last_date'] = row_dict['last_date'].isoformat()
            formatted_results.append(row_dict)
        
        conn.close()
        
        return {
            "traffic_source_id": traffic_source_id,
            "feeds": formatted_results
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")

if __name__ == "__main__":
    port = int(os.getenv('PORT', 8000))
    
    print("🚀 Starting Publisher Statistics API")
    print(f"📡 Server will run on http://localhost:{port}")
    print("📚 API docs available at http://localhost:{port}/docs")
    print("🔍 Health check at http://localhost:{port}/health")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=port,
        log_level="info"
    )