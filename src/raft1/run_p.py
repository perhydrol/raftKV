#!/usr/bin/env python3
# run_3b_tests_50x_parallel_linux_pass_check.py

import subprocess
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

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
LOG_LOCK = mp.Lock()


def run_test(test_name: str, round_num: int) -> dict:
    """子进程运行单个测试，并检查是否包含显式 PASS 字样"""
    cmd = ["go", "test", "-run", f"^{test_name}$", "-v"]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
        )

        # 👇 核心逻辑变更：必须 returncode == 0 且 stdout 含 "PASS"
        if result.returncode == 0 and "PASS" in result.stdout:
            return {
                "success": True,
                "test_name": test_name,
                "round_num": round_num,
                "stdout": None,  # 成功时不传回内容（节省内存）
                "stderr": None,
                "error": None
            }
        else:
            # 即使 returncode 是 0，只要没看到 PASS，就判定失败
            reason = "NO 'PASS' IN OUTPUT" if result.returncode == 0 else "NONZERO RETURN CODE"
            return {
                "success": False,
                "test_name": test_name,
                "round_num": round_num,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "error": reason
            }

    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "test_name": test_name,
            "round_num": round_num,
            "stdout": "",
            "stderr": "",
            "error": "TIMEOUT"
        }
    except Exception as e:
        return {
            "success": False,
            "test_name": test_name,
            "round_num": round_num,
            "stdout": "",
            "stderr": "",
            "error": f"CRASH: {str(e)}"
        }


def write_log(failure_data: dict):
    """主进程安全写入失败日志（带锁）"""
    with LOG_LOCK:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            test = failure_data
            ts = time.strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"\n{'='*80}\n")
            f.write(f"❌ FAILED: {test['test_name']} | Round #{test['round_num']}\n")
            f.write(f"Time: {ts}\n")
            f.write(f"Reason: {test['error']}\n")
            f.write("-" * 80 + "\n")

            if test["error"] == "TIMEOUT":
                f.write("⏰ TEST TIMED OUT (60s)\n")
            elif test["error"]:
                f.write(f"💥 {test['error']}\n")
            else:
                if test["stdout"]:
                    f.write("📄 STDOUT:\n")
                    f.write(test["stdout"] + "\n")
                    f.write("-" * 80 + "\n")
                if test["stderr"]:
                    f.write("❗ STDERR:\n")
                    f.write(test["stderr"] + "\n")
            f.write("="*80 + "\n")


def run_round(round_num: int) -> tuple[int, int]:
    """在 Linux 下，并行执行一轮测试"""
    print(f"\n\033[1;36m🔁 ROUND {round_num}/50 — Running {len(TESTS_3B)} tests in parallel...\033[0m")

    success_count = 0
    failed_count = 0

    ctx = mp.get_context("fork")
    with ProcessPoolExecutor(max_workers=len(TESTS_3B), mp_context=ctx) as executor:
        futures = [
            executor.submit(run_test, test, round_num)
            for test in TESTS_3B
        ]

        for future in as_completed(futures):
            result = future.result()
            if result["success"]:
                success_count += 1
                print(f"\033[1;32m✅ {result['test_name']} passed (Round #{round_num})\033[0m")
            else:
                failed_count += 1
                print(f"\033[1;31m❌ {result['test_name']} failed — logging... (Round #{round_num})\033[0m")
                write_log(result)

    return success_count, failed_count


def main():
    LOG_FILE.write_text("")
    print(f"\033[1;33m🚀 Starting 50 rounds of PARALLEL 3B tests (Linux + PASS check). Failures → '{LOG_FILE}'\033[0m\n")

    total_ok = 0
    total_failed = 0

    for r in range(1, 51):
        ok, fail = run_round(r)
        total_ok += ok
        total_failed += fail

    print(f"\n\033[1;34m📊 FINAL: {total_ok + total_failed} tests run, {total_failed} failed.\033[0m")
    if total_failed:
        print(f"\033[1;31m⚠️  Inspect './log.log' — some may have passed exit code but missed 'PASS' output.\033[0m")
    else:
        print(f"\033[1;32m🎉 ALL TESTS PASSED — strict 'PASS' validation + 50 rounds on Linux! 🚀\033[0m")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\033[1;33m🛑 Stopped by user.\033[0m")
