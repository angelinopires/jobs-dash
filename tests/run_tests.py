#!/usr/bin/env python3
"""
Fast test runner for the job scraping dashboard.

Runs all unit tests efficiently with suppressed warnings.
"""

import os
import sys
import unittest
import warnings

# Suppress all warnings for faster test execution
warnings.filterwarnings("ignore")

# Suppress Streamlit warnings
os.environ["STREAMLIT_SERVER_HEADLESS"] = "true"
os.environ["STREAMLIT_SERVER_RUN_ON_SAVE"] = "false"

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


def run_all_tests():
    """Run all unit tests efficiently and return results."""
    print("🚀 Running all tests...")

    # Discover and run all tests
    loader = unittest.TestLoader()
    start_dir = os.path.dirname(os.path.abspath(__file__))
    suite = loader.discover(start_dir, pattern="test_*.py")

    # Run tests with minimal output for speed
    runner = unittest.TextTestRunner(verbosity=1, buffer=True)
    result = runner.run(suite)

    # Print summary
    print("\n" + "=" * 50)
    print("TEST SUMMARY")
    print("=" * 50)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    if result.failures:
        print("\n❌ FAILURES:")
        for test, traceback in result.failures:
            print(f"  - {test}")

    if result.errors:
        print("\n❌ ERRORS:")
        for test, traceback in result.errors:
            print(f"  - {test}")

    if result.wasSuccessful():
        print("\n✅ ALL TESTS PASSED!")
    else:
        print("\n❌ SOME TESTS FAILED!")

    # Return success status
    return result.wasSuccessful()


def run_specific_test(test_module):
    """Run a specific test module."""
    print(f"🧪 Running specific test: {test_module}")
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromName(test_module)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Run specific test
        test_name = sys.argv[1]
        success = run_specific_test(test_name)
    else:
        # Run all tests
        success = run_all_tests()

    # Exit with appropriate code
    sys.exit(0 if success else 1)
