import requests
import json
import time
from datetime import datetime

class APITester:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.session = requests.Session()
        
    def print_header(self, title):
        """Print a formatted test header."""
        print(f"\n{'='*60}")
        print(f"🧪 {title}")
        print('='*60)
        
    def print_result(self, test_name, response, expected_status=200):
        """Print formatted test results."""
        status_icon = "✅" if response.status_code == expected_status else "❌"
        print(f"\n{status_icon} {test_name}")
        print(f"   Status: {response.status_code}")
        
        try:
            data = response.json()
            if response.status_code == expected_status:
                if 'data' in data and isinstance(data['data'], list):
                    print(f"   Records: {len(data['data'])}")
                    if data['data']:
                        print(f"   Sample: {data['data'][0]}")
                else:
                    print(f"   Response: {json.dumps(data, indent=2)}")
            else:
                print(f"   Error: {data.get('detail', 'Unknown error')}")
        except json.JSONDecodeError:
            print(f"   Raw response: {response.text}")
    
    def test_health_check(self):
        """Test the health check endpoint."""
        self.print_header("Health Check")
        
        try:
            response = self.session.get(f"{self.base_url}/health")
            self.print_result("Health Check", response)
            
            if response.status_code == 200:
                health_data = response.json()
                if health_data.get('status') == 'healthy':
                    db_info = health_data.get('database', {})
                    print(f"   Database: Connected ✅")
                    print(f"   Tables: {db_info.get('tables_found', [])}")
                    print(f"   Records: {db_info.get('distributed_stats_records', 0)}")
                else:
                    print(f"   Database: Unhealthy ❌")
        except requests.RequestException as e:
            print(f"❌ Connection failed: {e}")
    
    def test_root_endpoint(self):
        """Test the root endpoint."""
        self.print_header("Root Endpoint")
        
        response = self.session.get(f"{self.base_url}/")
        self.print_result("Root Endpoint", response)
    
    def test_valid_requests(self):
        """Test valid API requests."""
        self.print_header("Valid API Requests")
        
        test_cases = [
            {
                "name": "Traffic Source 66 - Valid Range",
                "params": {
                    "ts": 66,
                    "from": "2025-01-15",
                    "to": "2025-01-20",
                    "key": "test_key_66"
                }
            },
            {
                "name": "Traffic Source 67 - Valid Range", 
                "params": {
                    "ts": 67,
                    "from": "2025-01-15",
                    "to": "2025-01-20",
                    "key": "test_key_67"
                }
            },
            {
                "name": "Single Day Range",
                "params": {
                    "ts": 66,
                    "from": "2025-01-15",
                    "to": "2025-01-15",
                    "key": "test_key_66"
                }
            },
            {
                "name": "Wider Date Range",
                "params": {
                    "ts": 66,
                    "from": "2025-01-01",
                    "to": "2025-01-31",
                    "key": "test_key_66"
                }
            }
        ]
        
        for test_case in test_cases:
            response = self.session.get(f"{self.base_url}/pubstats", params=test_case["params"])
            self.print_result(test_case["name"], response)
    
    def test_authentication_errors(self):
        """Test authentication and authorization errors."""
        self.print_header("Authentication & Authorization Tests")
        
        test_cases = [
            {
                "name": "Invalid API Key",
                "params": {
                    "ts": 66,
                    "from": "2025-01-15",
                    "to": "2025-01-20",
                    "key": "invalid_key_123"
                },
                "expected_status": 401
            },
            {
                "name": "Missing API Key",
                "params": {
                    "ts": 66,
                    "from": "2025-01-15", 
                    "to": "2025-01-20"
                },
                "expected_status": 422  # FastAPI validation error
            },
            {
                "name": "Traffic Source Mismatch",
                "params": {
                    "ts": 67,  # Wrong TS for this key
                    "from": "2025-01-15",
                    "to": "2025-01-20",
                    "key": "test_key_66"  # This key is for TS 66
                },
                "expected_status": 403
            }
        ]
        
        for test_case in test_cases:
            response = self.session.get(f"{self.base_url}/pubstats", params=test_case["params"])
            self.print_result(test_case["name"], response, test_case["expected_status"])
    
    def test_validation_errors(self):
        """Test input validation errors."""
        self.print_header("Input Validation Tests")
        
        test_cases = [
            {
                "name": "Invalid Date Format",
                "params": {
                    "ts": 66,
                    "from": "2025/01/15",  # Wrong format
                    "to": "2025-01-20",
                    "key": "test_key_66"
                },
                "expected_status": 400
            },
            {
                "name": "Invalid Date Range (from > to)",
                "params": {
                    "ts": 66,
                    "from": "2025-01-20",
                    "to": "2025-01-15",  # End before start
                    "key": "test_key_66"
                },
                "expected_status": 400
            },
            {
                "name": "Missing Required Parameters",
                "params": {
                    "key": "test_key_66"
                    # Missing ts, from, to
                },
                "expected_status": 422
            },
            {
                "name": "Invalid Traffic Source Type",
                "params": {
                    "ts": "invalid",  # Should be integer
                    "from": "2025-01-15",
                    "to": "2025-01-20",
                    "key": "test_key_66"
                },
                "expected_status": 422
            }
        ]
        
        for test_case in test_cases:
            response = self.session.get(f"{self.base_url}/pubstats", params=test_case["params"])
            self.print_result(test_case["name"], response, test_case["expected_status"])
    
    def test_edge_cases(self):
        """Test edge cases and boundary conditions."""
        self.print_header("Edge Cases")
        
        test_cases = [
            {
                "name": "Future Date Range",
                "params": {
                    "ts": 66,
                    "from": "2026-01-01",
                    "to": "2026-01-31",
                    "key": "test_key_66"
                }
            },
            {
                "name": "Very Old Date Range",
                "params": {
                    "ts": 66,
                    "from": "2020-01-01",
                    "to": "2020-01-31",
                    "key": "test_key_66"
                }
            }
        ]
        
        for test_case in test_cases:
            response = self.session.get(f"{self.base_url}/pubstats", params=test_case["params"])
            self.print_result(test_case["name"], response)
    
    def run_all_tests(self):
        """Run all test suites."""
        print("🧪 Publisher Statistics API - Test Suite")
        print(f"📡 Testing against: {self.base_url}")
        print(f"🕐 Started at: {datetime.now().isoformat()}")
        
        try:
            self.test_health_check()
            self.test_root_endpoint()
            self.test_valid_requests()
            self.test_authentication_errors()
            self.test_validation_errors()
            self.test_edge_cases()
            
            print(f"\n{'='*60}")
            print("🎉 All tests completed!")
            print("💡 Check the results above for any failures")
            print(f"🕐 Finished at: {datetime.now().isoformat()}")
            
        except KeyboardInterrupt:
            print("\n\n⏹️  Tests interrupted by user")
        except Exception as e:
            print(f"\n\n❌ Test suite failed: {e}")

def main():
    """Main test execution."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test the Publisher Statistics API")
    parser.add_argument(
        "--url", 
        default="http://localhost:8000", 
        help="Base URL of the API (default: http://localhost:8000)"
    )
    parser.add_argument(
        "--wait", 
        action="store_true", 
        help="Wait for server to be ready before testing"
    )
    
    args = parser.parse_args()
    
    if args.wait:
        print("⏳ Waiting for server to be ready...")
        max_attempts = 30
        for attempt in range(max_attempts):
            try:
                response = requests.get(f"{args.url}/health", timeout=5)
                if response.status_code == 200:
                    print("✅ Server is ready!")
                    break
            except requests.RequestException:
                pass
            
            if attempt == max_attempts - 1:
                print("❌ Server did not become ready in time")
                return
            
            time.sleep(2)
    
    # Run tests
    tester = APITester(args.url)
    tester.run_all_tests()

if __name__ == "__main__":
    main()