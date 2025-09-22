import asyncio
import argparse
import csv
import hashlib
import uuid
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
import os
from datetime import datetime
import sys
import time

import asyncpg
import structlog
from dotenv import load_dotenv

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

class ProcessingError(Exception):
    """Custom processing error."""
    pass

class RevenueDistributor:
    def __init__(self, db_connection_string: str, batch_id: Optional[str] = None):
        """Initialize with database connection and batch tracking."""
        self.db_url = db_connection_string
        self.batch_id = batch_id or str(uuid.uuid4())
        self.pool: Optional[asyncpg.Pool] = None
        
        logger.info("Initializing revenue distributor", batch_id=self.batch_id)

    async def create_connection_pool(self):
        """Create database connection pool."""
        try:
            self.pool = await asyncpg.create_pool(
                self.db_url,
                min_size=2,
                max_size=5,
                max_queries=50000,
                max_inactive_connection_lifetime=300.0,
                command_timeout=60
            )
            logger.info("Database connection pool created", batch_id=self.batch_id)
        except Exception as e:
            logger.error("Failed to create database pool", batch_id=self.batch_id, error=str(e))
            raise ProcessingError(f"Database connection failed: {e}")

    async def close_connection_pool(self):
        """Close database connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed", batch_id=self.batch_id)

    async def log_operation(self, operation: str, status: str, message: str, 
                           records_processed: int = 0, execution_time_ms: int = 0):
        """Log operation for monitoring and debugging."""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO processing_logs 
                    (batch_id, operation, status, message, records_processed, execution_time_ms)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """, self.batch_id, operation, status, message, records_processed, execution_time_ms)
        except Exception as e:
            logger.error("Failed to log operation", error=str(e))

    def largest_remainder_method(self, total: int, weights: List[float]) -> List[int]:
        """
        Distribute integer using largest remainder method to ensure sum equals total.
        
        Args:
            total: Integer total to distribute
            weights: List of weights (will be normalized)
            
        Returns:
            List of integers that sum to total
        """
        if not weights or sum(weights) == 0:
            return [0] * len(weights)
        
        # Normalize weights to sum to 1
        weight_sum = sum(weights)
        normalized_weights = [w / weight_sum for w in weights]
        
        # Calculate initial distribution (rounded down)
        distributed = [int(total * w) for w in normalized_weights]
        remainder = total - sum(distributed)
        
        if remainder > 0:
            # Calculate fractional parts
            fractions = [(total * w) % 1 for w in normalized_weights]
            # Sort by largest fraction, then by index for deterministic results
            indices = sorted(range(len(weights)), 
                           key=lambda i: (-fractions[i], i))
            
            # Distribute remaining units
            for i in indices[:remainder]:
                distributed[i] += 1
        
        return distributed

    def normalize_feed_id(self, feed_id: str) -> str:
        """Normalize feed ID to handle case sensitivity."""
        return feed_id.upper().strip()

    def calculate_file_hash(self, file_path: str) -> str:
        """Calculate SHA256 hash of file for idempotency checking."""
        hash_sha256 = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            logger.error("Failed to calculate file hash", file_path=file_path, error=str(e))
            raise ProcessingError(f"Failed to hash file {file_path}: {e}")

    async def check_processing_idempotency(self, clicks_file: str, feeds_file: str) -> bool:
        """Check if files have already been processed in this batch."""
        try:
            clicks_hash = self.calculate_file_hash(clicks_file)
            feeds_hash = self.calculate_file_hash(feeds_file)
            
            async with self.pool.acquire() as conn:
                # Check if these exact files were processed recently
                existing = await conn.fetchval("""
                    SELECT COUNT(*) FROM processing_logs 
                    WHERE batch_id != $1 
                    AND operation = 'file_import'
                    AND status = 'success'
                    AND message LIKE $2
                    AND created_at >= NOW() - INTERVAL '1 day'
                """, self.batch_id, f"%clicks:{clicks_hash[:16]}%feeds:{feeds_hash[:16]}%")
                
                if existing > 0:
                    logger.warning("Files already processed recently", 
                                 batch_id=self.batch_id,
                                 clicks_hash=clicks_hash[:16], 
                                 feeds_hash=feeds_hash[:16])
                    return True
                    
            return False
            
        except Exception as e:
            logger.error("Idempotency check failed", error=str(e))
            return False

    async def import_csv_data(self, clicks_file: str, feeds_file: str, dry_run: bool = False):
        """Import CSV data with proper error handling and duplicate management."""
        start_time = time.time()
        
        # Check idempotency
        if await self.check_processing_idempotency(clicks_file, feeds_file):
            await self.log_operation("file_import", "warning", 
                                    "Files already processed recently - skipping import")
            if not dry_run:
                return
        
        logger.info("Starting CSV import", 
                   batch_id=self.batch_id,
                   clicks_file=clicks_file, 
                   feeds_file=feeds_file,
                   dry_run=dry_run)
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    if not dry_run:
                        # Clear existing data for this batch (keep last import strategy)
                        await conn.execute("DELETE FROM campaign_clicks WHERE created_at < NOW() - INTERVAL '1 day'")
                        await conn.execute("DELETE FROM feed_provider_data WHERE created_at < NOW() - INTERVAL '1 day'")
                    
                    # Import clicks data
                    clicks_imported = 0
                    clicks_skipped = 0
                    clicks_errors = []
                    
                    logger.info("Processing clicks file", file=clicks_file)
                    
                    if not os.path.exists(clicks_file):
                        raise ProcessingError(f"Clicks file not found: {clicks_file}")
                    
                    with open(clicks_file, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        
                        # Validate headers
                        required_headers = ['date', 'campaign_id', 'campaign_name', 'fp_feed_id', 'traffic_source_id', 'clicks']
                        if not all(header in reader.fieldnames for header in required_headers):
                            raise ProcessingError(f"Missing required headers in clicks file. Required: {required_headers}")
                        
                        batch_data = []
                        
                        for row_num, row in enumerate(reader, start=2):
                            try:
                                # Validate and clean data
                                fp_feed_id = self.normalize_feed_id(row['fp_feed_id'].strip())
                                clicks = int(row['clicks'])
                                campaign_id = int(row['campaign_id'])
                                traffic_source_id = int(row['traffic_source_id'])
                                
                                # Skip negative clicks but track them
                                if clicks < 0:
                                    clicks_skipped += 1
                                    logger.warning("Negative clicks detected", 
                                                 row=row_num, campaign_id=campaign_id, 
                                                 clicks=clicks, batch_id=self.batch_id)
                                    continue
                                
                                # Validate date format
                                date_obj = datetime.strptime(row['date'].strip(), '%Y-%m-%d').date()
                                
                                batch_data.append({
                                    'date': date_obj,
                                    'campaign_id': campaign_id,
                                    'campaign_name': row['campaign_name'].strip(),
                                    'fp_feed_id': fp_feed_id,
                                    'traffic_source_id': traffic_source_id,
                                    'clicks': clicks
                                })
                                
                                # Process in batches to avoid memory issues
                                if len(batch_data) >= 1000:
                                    if not dry_run:
                                        await self.insert_clicks_batch(conn, batch_data)
                                    clicks_imported += len(batch_data)
                                    batch_data = []
                                
                            except ValueError as e:
                                clicks_errors.append(f"Row {row_num}: {str(e)}")
                                if len(clicks_errors) > 100:  # Limit error collection
                                    break
                            except Exception as e:
                                clicks_errors.append(f"Row {row_num}: Unexpected error - {str(e)}")
                        
                        # Process remaining batch
                        if batch_data:
                            if not dry_run:
                                await self.insert_clicks_batch(conn, batch_data)
                            clicks_imported += len(batch_data)
                    
                    # Import feeds data
                    feeds_imported = 0
                    feeds_errors = []
                    
                    logger.info("Processing feeds file", file=feeds_file)
                    
                    if not os.path.exists(feeds_file):
                        raise ProcessingError(f"Feeds file not found: {feeds_file}")
                    
                    with open(feeds_file, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        
                        # Validate headers
                        required_headers = ['date', 'fp_feed_id', 'total_searches', 'monetized_searches', 'paid_clicks', 'feed_revenue']
                        if not all(header in reader.fieldnames for header in required_headers):
                            raise ProcessingError(f"Missing required headers in feeds file. Required: {required_headers}")
                        
                        batch_data = []
                        
                        for row_num, row in enumerate(reader, start=2):
                            try:
                                fp_feed_id = self.normalize_feed_id(row['fp_feed_id'].strip())
                                date_obj = datetime.strptime(row['date'].strip(), '%Y-%m-%d').date()
                                
                                batch_data.append({
                                    'date': date_obj,
                                    'fp_feed_id': fp_feed_id,
                                    'total_searches': int(row['total_searches']),
                                    'monetized_searches': int(row['monetized_searches']),
                                    'paid_clicks': int(row['paid_clicks']),
                                    'feed_revenue': float(row['feed_revenue'])
                                })
                                
                                # Process in batches
                                if len(batch_data) >= 1000:
                                    if not dry_run:
                                        await self.insert_feeds_batch(conn, batch_data)
                                    feeds_imported += len(batch_data)
                                    batch_data = []
                                    
                            except ValueError as e:
                                feeds_errors.append(f"Row {row_num}: {str(e)}")
                                if len(feeds_errors) > 100:
                                    break
                            except Exception as e:
                                feeds_errors.append(f"Row {row_num}: Unexpected error - {str(e)}")
                        
                        # Process remaining batch
                        if batch_data:
                            if not dry_run:
                                await self.insert_feeds_batch(conn, batch_data)
                            feeds_imported += len(batch_data)
            
            execution_time = int((time.time() - start_time) * 1000)
            
            # Log results
            status = "success" if not clicks_errors and not feeds_errors else "warning"
            message = f"Imported {clicks_imported} clicks, {feeds_imported} feeds. Skipped: {clicks_skipped} clicks."
            
            if clicks_errors or feeds_errors:
                message += f" Errors: {len(clicks_errors)} clicks, {len(feeds_errors)} feeds"
                logger.warning("CSV import completed with errors", 
                             batch_id=self.batch_id,
                             clicks_errors=clicks_errors[:10],  # Log first 10 errors
                             feeds_errors=feeds_errors[:10])
            
            if dry_run:
                message = f"DRY RUN: Would import {clicks_imported} clicks, {feeds_imported} feeds"
                
            await self.log_operation("file_import", status, message, 
                                    clicks_imported + feeds_imported, execution_time)
            
            logger.info("CSV import completed", 
                       batch_id=self.batch_id,
                       clicks_imported=clicks_imported,
                       feeds_imported=feeds_imported,
                       clicks_skipped=clicks_skipped,
                       execution_time_ms=execution_time,
                       dry_run=dry_run)
            
            if clicks_errors or feeds_errors:
                raise ProcessingError(f"Import completed with {len(clicks_errors) + len(feeds_errors)} errors")
                
        except Exception as e:
            execution_time = int((time.time() - start_time) * 1000)
            await self.log_operation("file_import", "error", str(e), 0, execution_time)
            logger.error("CSV import failed", batch_id=self.batch_id, error=str(e))
            raise ProcessingError(f"Failed to import CSV data: {e}")

    async def insert_clicks_batch(self, conn: asyncpg.Connection, batch_data: List[Dict]):
        """Insert clicks data batch with conflict resolution."""
        await conn.executemany("""
            INSERT INTO campaign_clicks 
            (date, campaign_id, campaign_name, fp_feed_id, traffic_source_id, clicks)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (date, campaign_id, fp_feed_id) 
            DO UPDATE SET 
                clicks = EXCLUDED.clicks,
                campaign_name = EXCLUDED.campaign_name,
                traffic_source_id = EXCLUDED.traffic_source_id,
                updated_at = CURRENT_TIMESTAMP
        """, [(
            row['date'], row['campaign_id'], row['campaign_name'],
            row['fp_feed_id'], row['traffic_source_id'], row['clicks']
        ) for row in batch_data])

    async def insert_feeds_batch(self, conn: asyncpg.Connection, batch_data: List[Dict]):
        """Insert feeds data batch with conflict resolution (keep latest)."""
        await conn.executemany("""
            INSERT INTO feed_provider_data 
            (date, fp_feed_id, total_searches, monetized_searches, paid_clicks, feed_revenue)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (date, fp_feed_id) 
            DO UPDATE SET 
                total_searches = EXCLUDED.total_searches,
                monetized_searches = EXCLUDED.monetized_searches,
                paid_clicks = EXCLUDED.paid_clicks,
                feed_revenue = EXCLUDED.feed_revenue,
                updated_at = CURRENT_TIMESTAMP
        """, [(
            row['date'], row['fp_feed_id'], row['total_searches'],
            row['monetized_searches'], row['paid_clicks'], row['feed_revenue']
        ) for row in batch_data])

    async def distribute_revenue(self, dry_run: bool = False):
        """Main distribution logic with comprehensive error handling."""
        start_time = time.time()
        
        logger.info("Starting revenue distribution", batch_id=self.batch_id, dry_run=dry_run)
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    if not dry_run:
                        # Clear existing distributed stats for this batch
                        await conn.execute("DELETE FROM distributed_stats WHERE batch_id = $1", self.batch_id)
                    
                    # Get all feed data with their corresponding clicks
                    query = """
                        SELECT f.date, f.fp_feed_id, f.total_searches, f.monetized_searches, 
                               f.paid_clicks, f.feed_revenue,
                               c.campaign_id, c.campaign_name, c.traffic_source_id, c.clicks
                        FROM feed_provider_data f
                        LEFT JOIN campaign_clicks c ON f.fp_feed_id = c.fp_feed_id AND f.date = c.date
                        WHERE c.clicks > 0  -- Only positive clicks
                        ORDER BY f.date, f.fp_feed_id, c.campaign_id
                    """
                    
                    results = await conn.fetch(query)
                    
                    # Group by (date, fp_feed_id)
                    feed_groups = defaultdict(list)
                    for row in results:
                        key = (row['date'], row['fp_feed_id'])
                        feed_groups[key].append(dict(row))
                    
                    logger.info("Processing feed groups", 
                               batch_id=self.batch_id,
                               total_groups=len(feed_groups))
                    
                    records_created = 0
                    distribution_errors = []
                    
                    # Process each feed group
                    for (date, fp_feed_id), campaigns in feed_groups.items():
                        try:
                            if not campaigns:
                                continue
                            
                            # Get feed totals (same for all campaigns in group)
                            feed_data = campaigns[0]
                            total_searches = feed_data['total_searches']
                            monetized_searches = feed_data['monetized_searches']
                            paid_clicks = feed_data['paid_clicks']
                            feed_revenue = Decimal(str(feed_data['feed_revenue']))
                            
                            # Calculate click weights
                            campaign_clicks = [row['clicks'] for row in campaigns]
                            total_clicks = sum(campaign_clicks)
                            
                            if total_clicks == 0:
                                logger.warning("Zero total clicks for feed", 
                                             batch_id=self.batch_id,
                                             feed_id=fp_feed_id, 
                                             date=date)
                                continue
                            
                            # Calculate weights (click share)
                            weights = [clicks / total_clicks for clicks in campaign_clicks]
                            
                            # Distribute integer metrics using largest remainder method
                            distributed_searches = self.largest_remainder_method(total_searches, weights)
                            distributed_monetized = self.largest_remainder_method(monetized_searches, weights)
                            distributed_paid_clicks = self.largest_remainder_method(paid_clicks, weights)
                            
                            # Verify integer distribution
                            if sum(distributed_searches) != total_searches:
                                raise ProcessingError(f"Searches distribution error: {sum(distributed_searches)} != {total_searches}")
                            if sum(distributed_monetized) != monetized_searches:
                                raise ProcessingError(f"Monetized distribution error: {sum(distributed_monetized)} != {monetized_searches}")
                            if sum(distributed_paid_clicks) != paid_clicks:
                                raise ProcessingError(f"Paid clicks distribution error: {sum(distributed_paid_clicks)} != {paid_clicks}")
                            
                            # Prepare batch insert data
                            batch_insert_data = []
                            
                            # Distribute revenue proportionally (can be fractional)
                            for i, campaign in enumerate(campaigns):
                                campaign_id = campaign['campaign_id']
                                campaign_name = campaign['campaign_name']
                                traffic_source_id = campaign['traffic_source_id']
                                
                                # Proportional revenue distribution
                                campaign_feed_revenue = (feed_revenue * Decimal(str(weights[i]))).quantize(
                                    Decimal('0.01'), rounding=ROUND_HALF_UP)
                                
                                # Publisher gets 75% of feed revenue
                                pub_revenue = (campaign_feed_revenue * Decimal('0.75')).quantize(
                                    Decimal('0.01'), rounding=ROUND_HALF_UP)
                                
                                batch_insert_data.append((
                                    date, campaign_id, campaign_name, fp_feed_id, traffic_source_id,
                                    distributed_searches[i], distributed_monetized[i], 
                                    distributed_paid_clicks[i], campaign_feed_revenue, pub_revenue, 
                                    True, self.batch_id
                                ))
                            
                            # Insert batch
                            if not dry_run:
                                await conn.executemany("""
                                    INSERT INTO distributed_stats 
                                    (date, campaign_id, campaign_name, fp_feed_id, traffic_source_id,
                                     total_searches, monetized_searches, paid_clicks, 
                                     feed_revenue, pub_revenue, is_feed_data, batch_id)
                                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                                    ON CONFLICT (date, campaign_id, fp_feed_id)
                                    DO UPDATE SET
                                        total_searches = EXCLUDED.total_searches,
                                        monetized_searches = EXCLUDED.monetized_searches,
                                        paid_clicks = EXCLUDED.paid_clicks,
                                        feed_revenue = EXCLUDED.feed_revenue,
                                        pub_revenue = EXCLUDED.pub_revenue,
                                        batch_id = EXCLUDED.batch_id,
                                        processed_at = CURRENT_TIMESTAMP
                                """, batch_insert_data)
                            
                            records_created += len(batch_insert_data)
                            
                        except Exception as e:
                            error_msg = f"Feed {fp_feed_id} on {date}: {str(e)}"
                            distribution_errors.append(error_msg)
                            logger.error("Feed distribution failed", 
                                       batch_id=self.batch_id,
                                       feed_id=fp_feed_id, 
                                       date=date, 
                                       error=str(e))
                    
                    execution_time = int((time.time() - start_time) * 1000)
                    
                    # Verify totals after distribution
                    if not dry_run:
                        verification_results = await self.verify_distribution_totals(conn)
                    else:
                        verification_results = {"status": "skipped", "message": "Dry run mode"}
                    
                    # Log results
                    status = "success" if not distribution_errors else "warning"
                    message = f"Distributed revenue to {records_created} records"
                    
                    if distribution_errors:
                        message += f" with {len(distribution_errors)} errors"
                        
                    if dry_run:
                        message = f"DRY RUN: Would distribute to {records_created} records"
                    
                    await self.log_operation("revenue_distribution", status, message, 
                                           records_created, execution_time)
                    
                    logger.info("Revenue distribution completed", 
                               batch_id=self.batch_id,
                               records_created=records_created,
                               errors=len(distribution_errors),
                               execution_time_ms=execution_time,
                               verification=verification_results,
                               dry_run=dry_run)
                    
                    if distribution_errors and len(distribution_errors) > len(feed_groups) * 0.1:
                        raise ProcessingError(f"Too many distribution errors: {len(distribution_errors)}")
                        
        except Exception as e:
            execution_time = int((time.time() - start_time) * 1000)
            await self.log_operation("revenue_distribution", "error", str(e), 0, execution_time)
            logger.error("Revenue distribution failed", batch_id=self.batch_id, error=str(e))
            raise ProcessingError(f"Failed to distribute revenue: {e}")

    async def verify_distribution_totals(self, conn: asyncpg.Connection) -> Dict:
        """Verify that distribution preserved totals."""
        try:
            # Get original totals
            original_totals = await conn.fetchrow("""
                SELECT SUM(total_searches) as searches, SUM(monetized_searches) as monetized, 
                       SUM(paid_clicks) as clicks, SUM(feed_revenue) as revenue
                FROM feed_provider_data
            """)
            
            # Get distributed totals for this batch
            distributed_totals = await conn.fetchrow("""
                SELECT SUM(total_searches) as searches, SUM(monetized_searches) as monetized, 
                       SUM(paid_clicks) as clicks, SUM(feed_revenue) as revenue
                FROM distributed_stats
                WHERE batch_id = $1
            """, self.batch_id)
            
            # Compare totals (allowing for small rounding differences in revenue)
            searches_match = original_totals['searches'] == distributed_totals['searches']
            monetized_match = original_totals['monetized'] == distributed_totals['monetized']
            clicks_match = original_totals['clicks'] == distributed_totals['clicks']
            revenue_match = abs(float(original_totals['revenue']) - float(distributed_totals['revenue'])) < 0.01
            
            all_match = searches_match and monetized_match and clicks_match and revenue_match
            
            return {
                "status": "passed" if all_match else "failed",
                "original": dict(original_totals),
                "distributed": dict(distributed_totals),
                "matches": {
                    "searches": searches_match,
                    "monetized": monetized_match,
                    "clicks": clicks_match,
                    "revenue": revenue_match
                }
            }
            
        except Exception as e:
            logger.error("Verification failed", batch_id=self.batch_id, error=str(e))
            return {"status": "error", "message": str(e)}

async def main():
    """Main execution function with command line argument support."""
    parser = argparse.ArgumentParser(description="Feed Revenue Distribution Pipeline")
    parser.add_argument('--batch-id', help='Custom batch ID for processing')
    parser.add_argument('--dry-run', action='store_true', help='Simulate processing without making changes')
    parser.add_argument('--clicks-file', default='test_clicks.csv', help='Clicks CSV file path')
    parser.add_argument('--feeds-file', default='test_feeds.csv', help='Feeds CSV file path')
    
    args = parser.parse_args()
    
    print("🚀 Feed Revenue Distribution Pipeline v2.0")
    print("=" * 60)
    
    if args.dry_run:
        print("🔍 DRY RUN MODE - No changes will be made")
    
    # Database connection
    DB_CONNECTION = os.getenv('DATABASE_URL')
    if not DB_CONNECTION:
        print("❌ Error: DATABASE_URL environment variable not set")
        sys.exit(1)
    
    # Check if files exist
    if not os.path.exists(args.clicks_file):
        print(f"❌ Error: {args.clicks_file} not found")
        sys.exit(1)
    
    if not os.path.exists(args.feeds_file):
        print(f"❌ Error: {args.feeds_file} not found")
        sys.exit(1)
    
    distributor = None
    
    try:
        # Initialize distributor
        distributor = RevenueDistributor(DB_CONNECTION, args.batch_id)
        await distributor.create_connection_pool()
        
        # Import CSV data
        await distributor.import_csv_data(args.clicks_file, args.feeds_file, args.dry_run)
        
        # Run distribution
        await distributor.distribute_revenue(args.dry_run)
        
        print("\n🎉 Pipeline completed successfully!")
        
        if not args.dry_run:
            print("\nNext steps:")
            print("1. Start the API server: python api_server.py")
            print("2. Test the API: python test_api.py")
            print(f"3. Check logs: SELECT * FROM processing_logs WHERE batch_id = '{distributor.batch_id}';")
        else:
            print("\n💡 This was a dry run. Use without --dry-run to actually process data.")
        
    except ProcessingError as e:
        print(f"\n❌ Processing Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error("Unexpected error", error=str(e), exc_info=True)
        print(f"\n❌ Unexpected Error: {e}")
        sys.exit(1)
    finally:
        if distributor:
            await distributor.close_connection_pool()

if __name__ == '__main__':
    asyncio.run(main())