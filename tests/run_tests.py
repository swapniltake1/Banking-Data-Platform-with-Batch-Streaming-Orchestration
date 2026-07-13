#!/usr/bin/env python
"""
Test Runner Script for Banking Transaction Pipeline

Usage:
    python run_tests.py                    # Run all tests
    python run_tests.py --unit             # Run only unit tests
    python run_tests.py --integration      # Run only integration tests
    python run_tests.py --coverage         # Run with coverage report
    python run_tests.py --file bronze      # Run specific test file
    python run_tests.py --parallel         # Run tests in parallel
"""

import sys
import os
import argparse
import subprocess


def run_command(command):
    """Execute a shell command and return the result."""
    print(f"\nExecuting: {' '.join(command)}")
    print("=" * 80)
    result = subprocess.run(command, capture_output=False)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description='Run banking pipeline tests')
    parser.add_argument('--unit', action='store_true', help='Run only unit tests')
    parser.add_argument('--integration', action='store_true', help='Run only integration tests')
    parser.add_argument('--coverage', action='store_true', help='Run with coverage report')
    parser.add_argument('--html-report', action='store_true', help='Generate HTML test report')
    parser.add_argument('--parallel', action='store_true', help='Run tests in parallel')
    parser.add_argument('--file', type=str, help='Run specific test file (e.g., bronze, silver, gold)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--markers', action='store_true', help='List available test markers')
    
    args = parser.parse_args()
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    # Base pytest command
    cmd = ['pytest']
    
    # List markers
    if args.markers:
        cmd.append('--markers')
        return run_command(cmd)
    
    # Add test selection
    if args.file:
        test_file = f"test_{args.file}_layer.py" if args.file in ['bronze', 'silver', 'gold'] else f"test_{args.file}.py"
        cmd.append(test_file)
    else:
        cmd.append('.')
    
    # Add markers
    if args.unit:
        cmd.extend(['-m', 'unit'])
    elif args.integration:
        cmd.extend(['-m', 'integration'])
    
    # Add coverage
    if args.coverage:
        cmd.extend(['--cov=../src', '--cov-report=html', '--cov-report=term'])
    
    # Add HTML report
    if args.html_report:
        cmd.extend(['--html=test_report.html', '--self-contained-html'])
    
    # Add parallel execution
    if args.parallel:
        cmd.extend(['-n', 'auto'])
    
    # Add verbose
    if args.verbose:
        cmd.append('-vv')
    else:
        cmd.append('-v')
    
    # Run the tests
    return_code = run_command(cmd)
    
    # Print summary
    print("\n" + "=" * 80)
    if return_code == 0:
        print("✓ All tests passed!")
        if args.coverage:
            print("\nCoverage report generated: htmlcov/index.html")
        if args.html_report:
            print("Test report generated: test_report.html")
    else:
        print("✗ Some tests failed!")
        sys.exit(return_code)
    
    print("=" * 80 + "\n")
    return return_code


if __name__ == '__main__':
    sys.exit(main())