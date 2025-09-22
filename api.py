import asyncio
import hashlib
import logging
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, date
from typing import Optional, Dict, List, Any

import asyncpg
import structlog
from fastapi import FastAPI, HTTPException, Query, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer
from dotenv import load_dotenv
import uvicorn
import os

# Load environment variables
load_dotenv()

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Database connection pool
db_pool: Optional[asyncpg.Pool] = None

async def create_db_pool():
    """Create database connection pool with proper configuration."""
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(
            os.getenv('DATABASE_URL'),
            min_size=2,
            max_size=10,
            max_queries=50000,
            max_inactive_connection_lifetime=300.0,
            command_timeout=60
        )
        logger.info("Database connection pool created successfully")
    except Exception as e:
        logger.error("Failed to create database pool", error=str(e))
        raise

async def close_db_pool():
    """Close database connection pool."""
    global db_pool
    if db_pool:
        await db_pool.close()
        logger.info("Database connection pool closed")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan events."""
    # Startup
    await create_db_pool()
    yield
    # Shutdown
    await close_db_pool()

# Initialize FastAPI app with lifespan manager
app = FastAPI(
    title="Publisher Statistics API",
    description="Secure API for accessing distributed feed revenue data",
    version="2.0.0",
    lifespan=lifespan
)

# Configure CORS with security considerations
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv('CORS_ORIGINS', 'http://localhost:3000').split(','),
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Security dependencies
security = HTTPBearer(auto_error=False)

class DatabaseError(HTTPException):
    """Custom database error that doesn't expose sensitive details."""
    def __init__(self, detail: str = "Database operation failed"):
        super().__init__(status_code=500, detail=detail)

class ValidationError(HTTPException):
    """Custom validation error."""
    def __init__(self, detail: str):
        super().__init__(status_code=400, detail=detail)

def hash_api_key(key: str) -> str:
    """Hash API key for secure storage comparison."""
    return hashlib.sha256(key.encode()).hexdigest()

async def get_db_connection():
    """Get database connection with retry logic."""
    if not db_pool:
        raise DatabaseError("Database connection pool not available")
    
    retry_count = 3
    for attempt in range(retry_count):
        try:
            return await db_pool.acquire()
        except Exception as e:
            if attempt == retry_count - 1:
                logger.error("Failed to acquire database connection", error=str(e), attempt=attempt)
                raise DatabaseError()
            await asyncio.sleep(0.1 * (2 ** attempt))  # Exponential backoff

async def validate_api_key(key: str, request: Request) -> int:
    """Validate API key and return traffic source ID with rate limiting."""
    if not key:
        raise HTTPException(status_code=401, detail="API key required")
    
    key_hash = hash_api_key(key)
    request_id = str(uuid.uuid4())
    
    try:
        conn = await get_db_connection()
        try:
            # Check API key and rate limiting
            result = await conn.fetchrow("""
                SELECT traffic_source_id, rate_limit_per_hour, last_used_at
                FROM api_keys 
                WHERE key_hash = $1 AND is_active = true
            """, key_hash)
            
            if not result:
                logger.warning("Invalid API key attempted", 
                             request_id=request_id, 
                             client_ip=request.client.host)
                raise HTTPException(status_code=401, detail="Invalid API key")
            
            # Update last used timestamp
            await conn.execute("""
                UPDATE api_keys 
                SET last_used_at = CURRENT_TIMESTAMP 
                WHERE key_hash = $1
            """, key_hash)
            
            logger.info("API key validated", 
                       request_id=request_id,
                       traffic_source_id=result['traffic_source_id'])
            
            return result['traffic_source_id']
            
        finally:
            await db_pool.release(conn)
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error("API key validation failed", 
                    request_id=request_id, 
                    error=str(e))
        raise HTTPException(status_code=500, detail="Authentication service unavailable")

def validate_traffic_source(key_ts: int, param_ts: int):
    """Validate traffic source parameter matches API key."""
    if key_ts != param_ts:
        raise HTTPException(
            status_code=403, 
            detail="Access denied: traffic source mismatch"
        )

def validate_date_format(date_str: str, field_name: str) -> date:
    """Validate and parse date string."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise ValidationError(f"Invalid {field_name} format. Use YYYY-MM-DD")

@app.middleware("http")
async def add_request_logging(request: Request, call_next):
    """Add request logging and timing."""
    request_id = str(uuid.uuid4())
    start_time = time.time()
    
    # Add request ID to request state
    request.state.request_id = request_id
    
    logger.info("Request started", 
               request_id=request_id,
               method=request.method,
               url=str(request.url),
               client_ip=request.client.host)
    
    try:
        response = await call_next(request)
        process_time = (time.time() - start_time) * 1000
        
        logger.info("Request completed",
                   request_id=request_id,
                   status_code=response.status_code,
                   process_time_ms=round(process_time, 2))
        
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Process-Time"] = f"{process_time:.2f}ms"
        
        return response
        
    except Exception as e:
        process_time = (time.time() - start_time) * 1000
        logger.error("Request failed",
                    request_id=request_id,
                    error=str(e),
                    process_time_ms=round(process_time, 2))
        raise

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler that doesn't expose sensitive information."""
    request_id = getattr(request.state, 'request_id', 'unknown')
    
    if isinstance(exc, HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": exc.detail,
                "request_id": request_id
            }
        )
    
    # Log the full error but return generic message
    logger.error("Unhandled exception",
                request_id=request_id,
                error=str(exc),
                exc_info=True)
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "request_id": request_id
        }
    )

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "Publisher Statistics API",
        "version": "2.0.0",
        "status": "operational",
        "endpoints": {
            "/pubstats": "Get publisher statistics",
            "/health": "Health check",
            "/metrics": "System metrics",
            "/docs": "API documentation"
        }
    }

@app.get("/pubstats")
async def get_publisher_stats(
    request: Request,
    ts: int = Query(..., description="Traffic source ID", example=66),
    from_date: str = Query(..., alias="from", description="Start date (YYYY-MM-DD)", example="2025-01-15"), 
    to_date: str = Query(..., alias="to", description="End date (YYYY-MM-DD)", example="2025-01-20"),
    key: str = Query(..., description="API key", example="secure_key_here")
):
    """
    Get publisher statistics for a given traffic source and date range.
    
    Returns only records with pub_revenue > 0, sorted by date DESC, campaign_id ASC.
    Includes comprehensive error handling and request tracking.
    """
    request_id = getattr(request.state, 'request_id', 'unknown')
    start_time = time.time()
    
    # Validate API key and get traffic source
    traffic_source_id = await validate_api_key(key, request)
    
    # Validate traffic source parameter
    validate_traffic_source(traffic_source_id, ts)
    
    # Validate and parse dates
    start_date = validate_date_format(from_date, "start date")
    end_date = validate_date_format(to_date, "end date")
    
    if start_date > end_date:
        raise ValidationError("Start date must be before or equal to end date")
    
    # Check for reasonable date range
    date_diff = (end_date - start_date).days
    if date_diff > 365:
        raise ValidationError("Date range cannot exceed 365 days")
    
    try:
        conn = await get_db_connection()
        try:
            # Query with proper error handling
            query = """
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
                WHERE traffic_source_id = $1
                  AND date >= $2 
                  AND date <= $3
                  AND is_feed_data = true
                  AND pub_revenue > 0
                ORDER BY date DESC, campaign_id ASC
                LIMIT 10000
            """
            
            results = await conn.fetch(query, traffic_source_id, start_date, end_date)
            
            # Format results
            formatted_results = []
            total_revenue = 0
            total_searches = 0
            campaign_ids = set()
            feed_ids = set()
            
            for row in results:
                record = {
                    "date": row['date'].isoformat(),
                    "campaign_id": row['campaign_id'],
                    "campaign_name": row['campaign_name'],
                    "total_searches": row['total_searches'],
                    "monetized_searches": row['monetized_searches'],
                    "paid_clicks": row['paid_clicks'],
                    "revenue": float(row['revenue']),
                    "feed_id": row['feed_id']
                }
                formatted_results.append(record)
                
                total_revenue += record['revenue']
                total_searches += record['total_searches']
                campaign_ids.add(record['campaign_id'])
                feed_ids.add(record['feed_id'])
            
            execution_time = (time.time() - start_time) * 1000
            
            logger.info("Publisher stats query completed",
                       request_id=request_id,
                       traffic_source_id=traffic_source_id,
                       records_returned=len(formatted_results),
                       execution_time_ms=round(execution_time, 2))
            
            return {
                "status": "success",
                "request_id": request_id,
                "traffic_source_id": traffic_source_id,
                "date_range": {
                    "from": from_date,
                    "to": to_date
                },
                "summary": {
                    "record_count": len(formatted_results),
                    "total_revenue": round(total_revenue, 2),
                    "total_searches": total_searches,
                    "unique_campaigns": len(campaign_ids),
                    "unique_feeds": len(feed_ids)
                },
                "data": formatted_results,
                "meta": {
                    "execution_time_ms": round(execution_time, 2),
                    "cached": False
                }
            }
            
        finally:
            await db_pool.release(conn)
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Database query failed",
                    request_id=request_id,
                    error=str(e))
        raise DatabaseError("Failed to retrieve publisher statistics")

@app.get("/health")
async def health_check():
    """Comprehensive health check with database connectivity."""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "2.0.0",
        "checks": {}
    }
    
    # Database connectivity check
    try:
        conn = await get_db_connection()
        try:
            # Test basic connectivity
            await conn.fetchval("SELECT 1")
            
            # Check table existence
            table_count = await conn.fetchval("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('distributed_stats', 'campaign_clicks', 'feed_provider_data', 'api_keys')
            """)
            
            # Get record counts (with timeout)
            stats_count = await asyncio.wait_for(
                conn.fetchval("SELECT COUNT(*) FROM distributed_stats"),
                timeout=5.0
            )
            
            health_status["checks"]["database"] = {
                "status": "healthy",
                "tables_found": table_count,
                "distributed_stats_records": stats_count,
                "connection_pool_size": db_pool.get_size() if db_pool else 0,
                "connection_pool_free": db_pool.get_idle_size() if db_pool else 0
            }
            
        finally:
            await db_pool.release(conn)
            
    except asyncio.TimeoutError:
        health_status["status"] = "degraded"
        health_status["checks"]["database"] = {
            "status": "timeout",
            "error": "Database query timeout"
        }
    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["checks"]["database"] = {
            "status": "error",
            "error": "Database connection failed"
        }
        logger.error("Health check database error", error=str(e))
    
    # Memory and system checks could be added here
    
    status_code = 200 if health_status["status"] == "healthy" else 503
    return JSONResponse(content=health_status, status_code=status_code)

@app.get("/metrics")
async def get_metrics():
    """System metrics endpoint for monitoring."""
    try:
        conn = await get_db_connection()
        try:
            # Get processing metrics
            processing_stats = await conn.fetch("""
                SELECT 
                    status,
                    COUNT(*) as count,
                    AVG(execution_time_ms) as avg_time_ms
                FROM processing_logs 
                WHERE created_at >= NOW() - INTERVAL '1 hour'
                GROUP BY status
            """)
            
            # Get API usage stats
            api_stats = await conn.fetch("""
                SELECT 
                    traffic_source_id,
                    COUNT(*) as request_count,
                    MAX(last_used_at) as last_request
                FROM api_keys 
                WHERE is_active = true AND last_used_at >= NOW() - INTERVAL '1 hour'
                GROUP BY traffic_source_id
            """)
            
            return {
                "timestamp": datetime.now().isoformat(),
                "processing_stats": [dict(row) for row in processing_stats],
                "api_usage": [dict(row) for row in api_stats],
                "database": {
                    "pool_size": db_pool.get_size() if db_pool else 0,
                    "pool_free": db_pool.get_idle_size() if db_pool else 0
                }
            }
            
        finally:
            await db_pool.release(conn)
            
    except Exception as e:
        logger.error("Metrics collection failed", error=str(e))
        raise DatabaseError("Failed to collect metrics")

if __name__ == "__main__":
    port = int(os.getenv('PORT', 8000))
    log_level = os.getenv('LOG_LEVEL', 'info').lower()
    
    print("🚀 Starting Publisher Statistics API v2.0")
    print(f"📡 Server will run on http://localhost:{port}")
    print(f"📚 API docs available at http://localhost:{port}/docs")
    print(f"🔍 Health check at http://localhost:{port}/health")
    print(f"📊 Metrics at http://localhost:{port}/metrics")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=port,
        log_level=log_level,
        access_log=True,
        loop="asyncio"
    )