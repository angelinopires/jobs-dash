#!/usr/bin/env python3
"""
Test runner script for nan prevention tests.
Run this script to ensure no "nan" values appear in the UI.

IMPORTANT: This script must be run with the virtual environment activated:
  source venv/bin/activate
  python3 run_nan_tests.py
"""

import os
import sys
import unittest


def check_venv() -> bool:
    """Check if virtual environment is activated."""
    venv_path = os.environ.get("VIRTUAL_ENV")
    if not venv_path:
        print("⚠️  WARNING: Virtual environment not detected!")
        print("Please activate the virtual environment first:")
        print("  source venv/bin/activate")
        print("  python3 run_nan_tests.py")
        return False

    print(f"✅ Virtual environment active: {venv_path}")
    return True


def run_nan_prevention_tests() -> bool:
    """Run all nan prevention tests and display results."""
    print("🧪 Running NaN Prevention Test Suite")
    print("=" * 50)

    # Check virtual environment
    if not check_venv():
        print("\n❌ Please activate virtual environment before running tests")
        return False

    # Add current directory to Python path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, current_dir)

    # Test files to run
    test_files = ["tests.test_display_functions", "tests.test_scraper_formatting", "tests.test_nan_prevention"]

    total_tests = 0
    passed_tests = 0
    failed_tests = 0

    for test_file in test_files:
        print(f"\n📋 Running {test_file}...")
        print("-" * 30)

        try:
            # Load and run the test module
            loader = unittest.TestLoader()
            suite = loader.loadTestsFromName(test_file)
            runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
            result = runner.run(suite)

            # Track results
            total_tests += result.testsRun
            passed_tests += result.testsRun - len(result.failures) - len(result.errors)
            failed_tests += len(result.failures) + len(result.errors)

            if result.failures:
                print(f"❌ {len(result.failures)} test(s) failed")
                for test, error in result.failures:
                    print(f"   FAIL: {test}")
                    print(f"   {error}")

            if result.errors:
                print(f"💥 {len(result.errors)} test(s) had errors")
                for test, error in result.errors:
                    print(f"   ERROR: {test}")
                    print(f"   {error}")

        except Exception as e:
            print(f"❌ Failed to run {test_file}: {e}")
            failed_tests += 1

    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    print(f"Total Tests: {total_tests}")
    print(f"✅ Passed: {passed_tests}")
    print(f"❌ Failed: {failed_tests}")

    if failed_tests == 0:
        print("\n🎉 ALL TESTS PASSED! No nan values should appear in the UI.")
        return True
    else:
        print(f"\n⚠️  {failed_tests} tests failed. Please fix these issues to prevent nan values in the UI.")
        return False


def run_quick_nan_check() -> bool:
    """Run a quick check for common nan issues."""
    print("\n🔍 Quick NaN Check")
    print("-" * 20)

    try:
        # Import functions
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from dashboard import clean_company_info, clean_display_value
        from scrapers.optimized_indeed_scraper import get_indeed_scraper

        scraper = get_indeed_scraper()

        # Test common problematic values
        problematic_values = ["nan", "none", "null", None, "", "   "]

        print("Testing display functions with problematic values...")
        for value in problematic_values:
            result1 = clean_display_value(str(value))
            result2 = clean_company_info(f"Industry: {value} | Size: {value}")

            if "nan" in str(result1).lower() or "nan" in str(result2).lower():
                print(f"❌ Found 'nan' in results for value: {value}")
                return False

        # Test scraper formatting
        print("Testing scraper formatting functions...")
        test_row = {"company_industry": "nan", "company_num_employees": "none", "company_revenue": "null"}
        result = scraper._format_company_info(test_row)

        if "nan" in result.lower():
            print(f"❌ Found 'nan' in scraper result: {result}")
            return False

        print("✅ Quick check passed - no obvious nan issues found")
        return True

    except Exception as e:
        print(f"❌ Quick check failed: {e}")
        return False


if __name__ == "__main__":
    print("NaN Prevention Test Suite for Jobs Dashboard")
    print("This ensures no 'nan' values appear in the user interface")
    print()

    # Run quick check first
    quick_passed = run_quick_nan_check()

    if not quick_passed:
        print("\n⚠️  Quick check failed. Running full test suite...")

    # Run full test suite
    all_passed = run_nan_prevention_tests()

    # Exit with appropriate code
    if all_passed and quick_passed:
        print("\n✅ SUCCESS: All nan prevention tests passed!")
        sys.exit(0)
    else:
        print("\n❌ FAILURE: Some tests failed. Please review and fix.")
        sys.exit(1)
