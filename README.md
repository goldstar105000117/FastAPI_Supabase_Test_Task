# Publisher Statistics API

🚀 **Publisher Statistics API v2.0** is a secure FastAPI service for retrieving distributed feed revenue data. It provides endpoints for publisher statistics, health checks, and system metrics, with built-in API key authentication, rate limiting, and detailed logging.

---

## Table of Contents

- [Features](#features)  
- [Tech Stack](#tech-stack)  
- [Requirements](#requirements)  
- [Setup & Installation](#setup--installation)  
- [Environment Variables](#environment-variables)  
- [Running the API](#running-the-api)  
- [API Endpoints](#api-endpoints)  
- [Testing](#testing)  
- [Logging & Monitoring](#logging--monitoring)  
- [License](#license)  

---

## Features

- Retrieve publisher statistics for specified traffic sources and date ranges  
- API key authentication with traffic source validation  
- Input validation for dates and query parameters  
- Health check endpoint with database connectivity and table existence check  
- Metrics endpoint for system monitoring and API usage  
- Detailed structured JSON logging with request tracking  
- Rate limiting and secure API key handling  

---

## Tech Stack

- Python 3.11+  
- FastAPI  
- asyncpg (PostgreSQL driver)  
- Structlog for structured logging  
- Uvicorn for ASGI server  
- Requests (for testing)  

---

## Requirements

Install Python packages via `requirements.txt`:

```bash
pip install -r requirements.txt
````

* PostgreSQL database with access credentials
* Python 3.11 or later

---

## Setup & Installation

1. Clone the repository:

```bash
git clone https://github.com/your-repo/publisher-stats-api.git
cd publisher-stats-api
```

2. Copy `.env.example` to `.env` and update the values:

```bash
cp .env.example .env
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Ensure PostgreSQL is running and the database is accessible via `DATABASE_URL`.

---

## Environment Variables

The project requires a `.env` file with the following variables:

```ini
# Database Configuration
DATABASE_URL=postgresql://username:password@localhost:5432/revenue_db

# API Server Configuration
PORT=8000
LOG_LEVEL=info

# Security Configuration
CORS_ORIGINS=http://localhost:3000,http://localhost:8080
```

* `DATABASE_URL`: PostgreSQL connection string
* `PORT`: Port for FastAPI server (default 8000)
* `LOG_LEVEL`: Logging level (`info`, `debug`, `error`)
* `CORS_ORIGINS`: Allowed origins for cross-origin requests

---

## Running the API

Start the API with Uvicorn:

```bash
python api.py
```

Server info printed at startup:

* Root endpoint: `http://localhost:8000/`
* API docs: `http://localhost:8000/docs`
* Health check: `http://localhost:8000/health`
* Metrics: `http://localhost:8000/metrics`

---

## API Endpoints

### **Root**

* `GET /`
* Returns basic service info and available endpoints.

### **Publisher Statistics**

* `GET /pubstats`
* Query parameters:

  * `ts` (int) - Traffic source ID
  * `from` (YYYY-MM-DD) - Start date
  * `to` (YYYY-MM-DD) - End date
  * `key` (string) - API key
* Returns publisher revenue statistics with summary. Only records with `pub_revenue > 0` are returned, sorted by `date DESC` and `campaign_id ASC`.

### **Health Check**

* `GET /health`
* Returns server health status and database connectivity info, including table existence and record counts.

### **Metrics**

* `GET /metrics`
* Returns system metrics and API usage stats for the last hour.

---

## Testing

The project includes **automated API tests** in `test_api.py` using the `requests` library.

### Run Tests

```bash
python test_api.py --url http://localhost:8000
```

Optional: wait for server readiness before tests:

```bash
python test_api.py --url http://localhost:8000 --wait
```

**Test Categories:**

* Health check
* Root endpoint
* Valid API requests
* Authentication & authorization errors
* Input validation errors
* Edge cases (future/old date ranges)

Tests provide detailed console output with sample records and error details.

---

## Logging & Monitoring

* Structured JSON logging via `structlog`
* Logs request start/end, errors, and execution times
* Request ID added to each log for traceability
* Health and metrics endpoints allow system monitoring

---

## Development Notes

* All database queries use asyncpg with connection pooling
* API key validation includes hashing with SHA256 for security
* Middleware adds request timing and logging headers
* Exception handling ensures sensitive data is not exposed
* Maximum date range is limited to 365 days per query