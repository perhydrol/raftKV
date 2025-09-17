#!/usr/bin/env python3
# run_3b_tests_50x.py

import subprocess
import time
from pathlib import Path

# 定义要运行的测试列表
TESTS_3B = [
    "TestBasicAgree3B",
    "TestRPCBytes3B",
    "TestFollowerFailure3B",
    "TestLeaderFailure3B",
    "TestFailAgree3B",
    "TestFailNoAgree3B",
    "TestConcurrentStarts3B",
    "TestRejoin3B",
    "TestBackup3B",
    "TestCount3B",
]

LOG_FILE = Path("./log.log")

def run_test(test_name: str, round_num: int) -> bool:
    """运行单个测试，失败时记录日志。不打印stdout除非失败。"""
    cmd = ["go", "test", "-run", f"^{test_name}$", "-v"]
    
    try:
        # 不显示 stdout/stderr 除非失败
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60  # 防止卡死
        )

        if result.returncode != 0:
            # 测试失败，记录日志
            failure_info = (
                f"\n{'='*80}\n"
                f"❌ FAILED: {test_name} | Round #{round_num}\n"
                f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Command: {' '.join(cmd)}\n"
                f"{'-'*80}\n"
                f"STDOUT:\n{result.stdout}\n"
                f"{'-'*80}\n"
                f"STDERR:\n{result.stderr}\n"
                f"{'='*80}\n"
            )
            with LOG_FILE.open("a", encoding="utf-8") as f:
                f.write(failure_info)
            
            print(f"\033[1;31m❌ {test_name} failed in round #{round_num} — log saved.\033[0m")
            return False
        else:
            # 成功：终端只显示绿色通过，不输出任何测试内容
            print(f"\033[1;32m✅ {test_name} passed (Round #{round_num})\033[0m")
            return True

    except subprocess.TimeoutExpired:
        error_msg = f"❌ {test_name} TIMEOUT in round #{round_num}"
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(f"\n{error_msg}\n{'-'*80}\n")
        print(f"\033[1;31m{error_msg} — log saved.\033[0m")
        return False
    except Exception as e:
        error_msg = f"💥 {test_name} CRASHED in round #{round_num}: {e}"
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(f"\n{error_msg}\n")
        print(f"\033[1;31m{error_msg} — log saved.\033[0m")
        return False


def main():
    LOG_FILE.write_text("")  # 清空日志文件
    print(f"\033[1;33m🚀 Starting 50 rounds of 3B tests. Failures logged to '{LOG_FILE}'.\033[0m\n")

    total_failed = 0
    total_runs = 0

    for round_num in range(1, 51):
        print(f"\n\033[1;36m🔁 ROUND {round_num}/50\033[0m")
        for test in TESTS_3B:
            total_runs += 1
            if not run_test(test, round_num):
                total_failed += 1

    print(f"\n\033[1;34m📊 SUMMARY: {total_runs} tests executed, {total_failed} failed.\033[0m")
    if total_failed > 0:
        print(f"\033[1;31m⚠️  Check './log.log' for details.\033[0m")
    else:
        print(f"\033[1;32m🎉 All tests passed in all 50 rounds!\033[0m")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\033[1;33m🛑 Execution interrupted by user.\033[0m")
