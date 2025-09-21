#!/usr/bin/env python3
"""
Feed Revenue Distribution Pipeline
Distributes feed provider revenue to campaigns based on their click share.

Usage:
    python process_data.py

Environment Variables:
    DATABASE_URL: PostgreSQL connection string
    
Requirements:
    pip install psycopg2-binary
"""

import csv
import psycopg2
import psycopg2.extras
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict
from typing import Dict, List, Tuple
import os
from datetime import datetime
import sys
from dotenv import load_dotenv

load_dotenv()

class RevenueDistributor:
    def __init__(self, db_connection_string: str):
        """Initialize with database connection."""
        try:
            self.conn = psycopg2.connect(db_connection_string)
            self.conn.autocommit = False
            print("✅ Database connection established")
        except psycopg2.Error as e:
            raise Exception(f"Failed to connect to database: {e}")

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

    def import_csv_data(self, clicks_file: str, feeds_file: str):
        """Import CSV data into raw tables."""
        cursor = self.conn.cursor()
        
        try:
            print("📥 Importing CSV data...")
            
            # Clear existing data
            cursor.execute("DELETE FROM campaign_clicks")
            cursor.execute("DELETE FROM feed_provider_data")
            
            # Import clicks data
            clicks_imported = 0
            clicks_skipped = 0
            
            print(f"  Processing {clicks_file}...")
            with open(clicks_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Handle case sensitivity and data cleaning
                    fp_feed_id = self.normalize_feed_id(row['fp_feed_id'])
                    clicks = int(row['clicks'])
                    
                    # Skip negative clicks but log them
                    if clicks < 0:
                        print(f"    ⚠️  Skipping negative clicks ({clicks}) for campaign {row['campaign_id']} on {row['date']}")
                        clicks_skipped += 1
                        continue
                    
                    cursor.execute("""
                        INSERT INTO campaign_clicks 
                        (date, campaign_id, campaign_name, fp_feed_id, traffic_source_id, clicks)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date, campaign_id, fp_feed_id) DO UPDATE SET
                        clicks = EXCLUDED.clicks,
                        campaign_name = EXCLUDED.campaign_name,
                        traffic_source_id = EXCLUDED.traffic_source_id
                    """, (row['date'], int(row['campaign_id']), row['campaign_name'], 
                         fp_feed_id, int(row['traffic_source_id']), clicks))
                    clicks_imported += 1
            
            print(f"    ✅ Imported {clicks_imported} click records, skipped {clicks_skipped}")
            
            # Import feeds data
            feeds_imported = 0
            
            print(f"  Processing {feeds_file}...")
            with open(feeds_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    fp_feed_id = self.normalize_feed_id(row['fp_feed_id'])
                    
                    cursor.execute("""
                        INSERT INTO feed_provider_data 
                        (date, fp_feed_id, total_searches, monetized_searches, paid_clicks, feed_revenue)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date, fp_feed_id) DO UPDATE SET
                        total_searches = EXCLUDED.total_searches,
                        monetized_searches = EXCLUDED.monetized_searches,
                        paid_clicks = EXCLUDED.paid_clicks,
                        feed_revenue = EXCLUDED.feed_revenue
                    """, (row['date'], fp_feed_id, int(row['total_searches']),
                         int(row['monetized_searches']), int(row['paid_clicks']), 
                         float(row['feed_revenue'])))
                    feeds_imported += 1
            
            print(f"    ✅ Imported {feeds_imported} feed records")
            
            self.conn.commit()
            print("✅ CSV data imported successfully")
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Failed to import CSV data: {e}")

    def distribute_revenue(self):
        """Main distribution logic."""
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        try:
            print("🔄 Starting revenue distribution...")
            
            # Clear existing distributed stats
            cursor.execute("DELETE FROM distributed_stats")
            
            # Get all feed data with their corresponding clicks
            cursor.execute("""
                SELECT f.date, f.fp_feed_id, f.total_searches, f.monetized_searches, 
                       f.paid_clicks, f.feed_revenue,
                       c.campaign_id, c.campaign_name, c.traffic_source_id, c.clicks
                FROM feed_provider_data f
                LEFT JOIN campaign_clicks c ON f.fp_feed_id = c.fp_feed_id AND f.date = c.date
                WHERE c.clicks > 0  -- Only positive clicks
                ORDER BY f.date, f.fp_feed_id, c.campaign_id
            """)
            
            # Group by (date, fp_feed_id)
            feed_groups = defaultdict(list)
            for row in cursor.fetchall():
                key = (row['date'], row['fp_feed_id'])
                feed_groups[key].append(row)
            
            print(f"  Found {len(feed_groups)} feed groups to process")
            
            # Process each feed group
            records_created = 0
            
            for (date, fp_feed_id), campaigns in feed_groups.items():
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
                    print(f"  ⚠️  Zero total clicks for feed {fp_feed_id} on {date}")
                    continue
                
                # Calculate weights (click share)
                weights = [clicks / total_clicks for clicks in campaign_clicks]
                
                # Distribute integer metrics using largest remainder method
                distributed_searches = self.largest_remainder_method(total_searches, weights)
                distributed_monetized = self.largest_remainder_method(monetized_searches, weights)
                distributed_paid_clicks = self.largest_remainder_method(paid_clicks, weights)
                
                # Verify integer distribution
                assert sum(distributed_searches) == total_searches, "Searches distribution error"
                assert sum(distributed_monetized) == monetized_searches, "Monetized distribution error"
                assert sum(distributed_paid_clicks) == paid_clicks, "Paid clicks distribution error"
                
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
                    
                    # Insert distributed stats
                    cursor.execute("""
                        INSERT INTO distributed_stats 
                        (date, campaign_id, campaign_name, fp_feed_id, traffic_source_id,
                         total_searches, monetized_searches, paid_clicks, 
                         feed_revenue, pub_revenue, is_feed_data)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (date, campaign_id, campaign_name, fp_feed_id, traffic_source_id,
                         distributed_searches[i], distributed_monetized[i], 
                         distributed_paid_clicks[i], campaign_feed_revenue, pub_revenue, True))
                    records_created += 1
            
            self.conn.commit()
            
            print(f"✅ Successfully distributed revenue to {records_created} campaign records")
            
            # Print verification totals
            cursor.execute("""
                SELECT SUM(total_searches) as searches, SUM(monetized_searches) as monetized, 
                       SUM(paid_clicks) as clicks, SUM(feed_revenue) as revenue
                FROM feed_provider_data
            """)
            original_totals = cursor.fetchone()
            
            cursor.execute("""
                SELECT SUM(total_searches) as searches, SUM(monetized_searches) as monetized, 
                       SUM(paid_clicks) as clicks, SUM(feed_revenue) as revenue
                FROM distributed_stats
            """)
            distributed_totals = cursor.fetchone()
            
            print(f"\n📊 Verification:")
            print(f"  Original totals    - Searches: {original_totals['searches']:,}, Revenue: ${original_totals['revenue']:,.2f}")
            print(f"  Distributed totals - Searches: {distributed_totals['searches']:,}, Revenue: ${distributed_totals['revenue']:,.2f}")
            
            # Check for feeds without campaigns
            cursor.execute("""
                SELECT f.fp_feed_id, f.date, f.feed_revenue
                FROM feed_provider_data f
                LEFT JOIN campaign_clicks c ON f.fp_feed_id = c.fp_feed_id AND f.date = c.date
                WHERE c.fp_feed_id IS NULL OR c.clicks <= 0
            """)
            orphaned_feeds = cursor.fetchall()
            
            if orphaned_feeds:
                total_orphaned_revenue = sum(float(feed['feed_revenue']) for feed in orphaned_feeds)
                print(f"  ⚠️  {len(orphaned_feeds)} feeds with no valid campaigns (${total_orphaned_revenue:.2f} revenue)")
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Failed to distribute revenue: {e}")

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            print("🔌 Database connection closed")


def main():
    """Main execution function."""
    print("🚀 Feed Revenue Distribution Pipeline")
    print("=" * 50)
    
    # Database connection (adjust as needed)
    DB_CONNECTION = os.getenv('DATABASE_URL', 
        'postgresql://user:password@localhost:5432/revenue_db')
    
    # CSV file paths
    CLICKS_FILE = 'test_clicks.csv'
    FEEDS_FILE = 'test_feeds.csv'
    
    # Check if files exist
    if not os.path.exists(CLICKS_FILE):
        print(f"❌ Error: {CLICKS_FILE} not found")
        sys.exit(1)
    
    if not os.path.exists(FEEDS_FILE):
        print(f"❌ Error: {FEEDS_FILE} not found")
        sys.exit(1)
    
    distributor = None
    
    try:
        # Initialize distributor
        distributor = RevenueDistributor(DB_CONNECTION)
        
        # Import CSV data
        distributor.import_csv_data(CLICKS_FILE, FEEDS_FILE)
        
        # Run distribution
        distributor.distribute_revenue()
        
        print("\n🎉 Pipeline completed successfully!")
        print("\nNext steps:")
        print("1. Start the API server: python api_server.py")
        print("2. Test the API: python test_api.py")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
    finally:
        if distributor:
            distributor.close()


if __name__ == '__main__':
    main()