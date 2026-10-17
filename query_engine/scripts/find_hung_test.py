#!/usr/bin/env python3
import subprocess
import sys

log_file = sys.argv[1] if len(sys.argv) > 1 else "target/cargo_test_stress.log"

try:
    out = subprocess.check_output(["cargo", "test", "--lib", "--", "--list"]).decode()
    all_tests = {
        line[:-6].strip() for line in out.splitlines() if line.endswith(": test")
    }
except Exception as e:
    print(f"Error querying test list: {e}")
    sys.exit(0)

try:
    with open(log_file) as f:
        completed = {
            l.split()[1]
            for l in f
            if l.startswith("test ") and ("... ok" in l or "... FAILED" in l)
        }
except Exception as e:
    completed = set()

hung = sorted(list(all_tests - completed))
if hung and len(hung) < len(all_tests):
    print(f"Found {len(hung)} hanging / deadlocked test case(s):")
    for t in hung:
        print(f"  ❌ {t}")
else:
    print("Timed out during compilation or runner initialization before tests could finish.")
