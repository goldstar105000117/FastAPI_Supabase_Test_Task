# Feed Revenue Distribution Pipeline

A data pipeline that distributes feed provider revenue to publisher campaigns based on their click share, with a REST API for accessing the results.

## 🚀 Quick Start

### Prerequisites
- Python 3.8+ 
- PostgreSQL 12+
- Your CSV data files: `test_clicks.csv` and `test_feeds.csv`

### Installation & Setup

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Create PostgreSQL database
createdb revenue_db
# OR using psql: psql -c "CREATE DATABASE revenue_db;"

# 3. Initialize database schema
psql revenue_db < schema.sql

# 4. Set database connection (choose one method)

# Method A: Environment variable (recommended)
export DATABASE_URL="postgresql://username:password@localhost:5432/revenue_db"

# Method B: Windows
set DATABASE_URL=postgresql://username:password@localhost:5432/revenue_db

# Method C: Create .env file
echo "DATABASE_URL=postgresql://username:password@localhost:5432/revenue_db" > .env

# 5. Process your CSV data
python process_data.py

# 6. Start the API server
python api_server.py

# 7. Test the API (in another terminal)
python test_api.py
```

### First API Call
```bash
curl "http://localhost:8000/pubstats?ts=66&from=2025-01-15&to=2025-01-20&key=test_key_66"
```

## 📁 Project Structure

```
feed-revenue-pipeline/
├── schema.sql              # Database schema and test data
├── process_data.py         # Data processing script
├── api_server.py          # FastAPI server
├── test_api.py            # API test suite
├── requirements.txt       # Python dependencies
├── README.md             # This file
├── .env.example          # Environment variables template
├── test_clicks.csv       # Your click data (provide this)
└── test_feeds.csv        # Your feed data (provide this)
```

## 🔧 Core Features

### Data Processing Engine
- **Integer Distribution**: Uses largest remainder method for exact totals
- **Case Sensitivity Handling**: Normalizes feed IDs (`SB100` vs `sb100`)
- **Edge Case Management**: Handles negative clicks, zero totals, duplicates
- **Revenue Calculation**: 75% publisher share of proportionally distributed feed revenue

### REST API
- **Authentication**: API key validation with traffic source mapping
- **Filtering**: Only returns records with `pub_revenue > 0`
- **Sorting**: Results sorted by `date DESC, campaign_id ASC`
- **Error Handling**: Comprehensive validation and error responses

## 📊 API Endpoints

### `/pubstats` - Get Publisher Statistics
```http
GET /pubstats?ts=66&from=2025-01-15&to=2025-01-20&key=test_key_66
```

**Parameters:**
- `ts` (int): Traffic source ID
- `from` (string): Start date (YYYY-MM-DD)
- `to` (string): End date (YYYY-MM-DD) 
- `key` (string): API key

**Response Example:**
```json
{
  "status": "success",
  "traffic_source_id": 66,
  "date_range": {
    "from": "2025-01-15",
    "to": "2025-01-20"
  },
  "summary": {
    "record_count": 12,
    "total_revenue": 94.08,
    "total_searches": 15420,
    "unique_campaigns": 3,
    "unique_feeds": 2
  },
  "data": [
    {
      "date": "2025-01-20",
      "campaign_id": 101,
      "campaign_name": "US_Search_Mobile_1", 
      "total_searches": 12052,
      "monetized_searches": 9642,
      "paid_clicks": 294,
      "revenue": 86.47,
      "feed_id": "SB100"
    }
  ]
}
```

### Other Endpoints
- **`GET /health`** - Health check and database status
- **`GET /debug/feeds?key=your_key`** - List available feeds for debugging
- **`GET /`** - API information and example usage
- **`GET /docs`** - Interactive API documentation

## 🔑 API Keys

Test API keys included in schema:
- `test_key_66` → Traffic Source 66
- `test_key_67` → Traffic Source 67

## 🗃️ Database Schema

### Tables Overview
- **`campaign_clicks`** - Raw click data imported from CSV
- **`feed_provider_data`** - Raw feed data imported from CSV  
- **`distributed_stats`** - Calculated revenue distribution results
- **`api_keys`** - API authentication and authorization

### Data Flow
```
test_clicks.csv → campaign_clicks → distributed_stats → API
test_feeds.csv  → feed_provider_data ↗
```

## 🔍 How Revenue Distribution Works

### Integer Distribution Example
```python
# Problem: Distribute 1001 searches between 2 campaigns
# Campaign A: 60% share = 600.6 searches
# Campaign B: 40% share = 400.4 searches
# Simple rounding gives 600 + 400 = 1000 (missing 1!)

# Solution: Largest Remainder Method
# 1. Round down: 600 + 400 = 1000
# 2. Remainder: 1001 - 1000 = 1
# 3. Give remainder to campaign with largest fraction (0.6 > 0.4)
# Final: 601 + 400 = 1001 ✓
```

### Revenue Distribution Example
```python
# Feed revenue: $125.45, Campaign click shares: 62.6% / 37.4%
# Campaign A: $125.45 × 0.626 = $78.55 feed revenue
#            $78.55 × 0.75 = $58.91 publisher revenue
# Campaign B: $125.45 × 0.374 = $46.90 feed revenue  
#            $46.90 × 0.75 = $35.17 publisher revenue
```

## 🔧 Configuration

### Database Connection Options

**Option 1: Environment Variable (Recommended)**
```bash
export DATABASE_URL="postgresql://username:password@localhost:5432/revenue_db"
```

**Option 2: .env File**
```bash
# Create .env file
cat > .env << EOF
DATABASE_URL=postgresql://username:password@localhost:5432/revenue_db
PORT=8000
EOF
```

**Option 3: Direct Code Modification**
Edit the connection string in `process_data.py` and `api_server.py`:
```python
# Replace this line:
DB_CONNECTION = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/revenue_db')

# With your connection:
DB_CONNECTION = 'postgresql://your_user:your_password@localhost:5432/revenue_db'
```

### Common PostgreSQL Connection Strings
```bash
# Local PostgreSQL (default)
postgresql://username:password@localhost:5432/revenue_db

# PostgreSQL with custom port
postgresql://username:password@localhost:5433/revenue_db

# Remote PostgreSQL
postgresql://username:password@hostname:5432/revenue_db

# SSL connection
postgresql://username:password@hostname:5432/revenue_db?sslmode=require
```

## 🚨 Error Handling

The API returns appropriate HTTP status codes:
- **200** - Success
- **400** - Bad Request (invalid dates, parameters)
- **401** - Unauthorized (invalid API key)
- **403** - Forbidden (traffic source mismatch)
- **422** - Validation Error (missing required fields)
- **500** - Internal Server Error

### Common Issues & Solutions

**Database Connection Error**
```bash
# Check if PostgreSQL is running
pg_isready

# Check database exists
psql -l | grep revenue_db

# Test connection
psql revenue_db -c "SELECT 1;"
```

**CSV Import Issues**
```bash
# Check file exists and has data
head -5 test_clicks.csv
head -5 test_feeds.csv

# Check for encoding issues
file test_clicks.csv
```

**API Server Won't Start**
```bash
# Check if port is available
netstat -an | grep 8000

# Try different port
PORT=8001 python api_server.py
```

## 📈 Production Deployment

### Environment Setup
```bash
# Production environment variables
export DATABASE_URL="postgresql://prod_user:secure_password@prod_host:5432/revenue_db?sslmode=require"
export PORT=8000
```

### Security Considerations
1. **Database**: Use strong passwords and SSL connections
2. **API Keys**: Replace test keys with secure production keys
3. **CORS**: Configure appropriate origins in production
4. **Logging**: Enable structured logging for monitoring
5. **Rate Limiting**: Consider adding rate limiting for public APIs

### Process Monitoring
```bash
# Run API server with auto-restart
while true; do python api_server.py; sleep 5; done

# Or use a process manager like supervisor, systemd, or PM2
```

## 🔄 Data Processing Workflow

### Step-by-Step Process
1. **Import CSVs** → Raw tables (`campaign_clicks`, `feed_provider_data`)
2. **Normalize Data** → Handle case sensitivity and clean invalid records
3. **Group by Feed** → Match clicks to feed data by `(date, fp_feed_id)`
4. **Calculate Weights** → Click share percentages for each campaign
5. **Distribute Integers** → Use largest remainder method for exact totals
6. **Calculate Revenue** → Proportional distribution with 75% publisher share
7. **Store Results** → Insert into `distributed_stats` table
8. **Serve via API** → Filter, sort, and return formatted results

### Verification Checks
- All integer totals match exactly (no rounding errors)
- Revenue sums match original feed revenue
- No campaigns have negative values
- All distributed records have valid relationships

## 🤝 Contributing

### Development Workflow
1. Fork the repository
2. Set up local development environment
3. Make changes and add tests
4. Run the test suite: `python test_api.py`
5. Submit a pull request

### Adding New Features
- New API endpoints: Add to `api_server.py`
- New processing logic: Modify `process_data.py` 
- New tests: Update `test_api.py`
- Database changes: Update `schema.sql`

## 📞 Support & Troubleshooting

### Quick Diagnostics
```bash
# 1. Check API server health
curl http://localhost:8000/health

# 2. Verify database connection
python -c "import psycopg2; psycopg2.connect('your_database_url'); print('OK')"

# 3. Check processed data
psql revenue_db -c "SELECT COUNT(*) FROM distributed_stats;"

# 4. Run full test suite
python test_api.py
```

### Getting Help
1. **Check logs** in the terminal where you ran the scripts
2. **Verify setup** using the diagnostics above
3. **Test with curl** to isolate API vs client issues
4. **Check database** directly with psql for data verification

---

**Tech Stack**: Python 3.8+, FastAPI, PostgreSQL, psycopg2  
**License**: MIT  
**Version**: 1.0.0