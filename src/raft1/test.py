import subprocess
import os
import re
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import sys

# 正则匹配 Go 日志时间格式
TIMESTAMP_PATTERN = re.compile(r'^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)')

def parse_timestamp(line):
    match = TIMESTAMP_PATTERN.match(line)
    if not match:
        return None
    timestamp_str = match.group(1)
    parts = timestamp_str.split('.')
    base_time = parts[0]
    microsec_str = (parts[1] + '000000')[:6]
    timestamp_with_us = f"{base_time}.{microsec_str}"
    try:
        return datetime.strptime(timestamp_with_us, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return None

def sort_log_lines(log_lines):
    with_timestamp = []
    without_timestamp = []
    for line in log_lines:
        ts = parse_timestamp(line)
        if ts is not None:
            with_timestamp.append((ts, line))
        else:
            without_timestamp.append(line)
    sorted_with_ts = sorted(with_timestamp, key=lambda x: x[0])
    sorted_lines = [item[1] for item in sorted_with_ts]
    sorted_lines.extend(without_timestamp)
    return sorted_lines

def run_single_test_n_times(test_name, times=100, success_indicator="PASS", log_dir="./test_logs"):
    """
    在独立进程中运行单个测试 N 次。
    返回 (test_name, success: bool)
    """
    # 确保每个进程内都能创建日志目录
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"log_{test_name}.log")
    
    command = f"go test -race -run ^{test_name}$"
    
    for i in range(1, times + 1):
        try:
            result = subprocess.run(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=180
            )
            full_output = result.stdout + result.stderr
            lines = full_output.splitlines()

            if any(line.strip() == success_indicator for line in lines):
                continue  # 成功，继续下一轮
            else:
                # 失败：写日志并返回
                sorted_lines = sort_log_lines(lines)
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.write(f"Test {test_name} failed on attempt {i}/{times}\n")
                    f.write(f"Command: {command}\n")
                    f.write("="*80 + "\n")
                    f.write("\n".join(sorted_lines))
                    f.write("\n" + "="*80 + "\n")
                return (test_name, False)

        except subprocess.TimeoutExpired:
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(f"Test {test_name} timed out on attempt {i}/{times} (60s)\n")
                f.write(f"Command: {command}\n")
            return (test_name, False)

        except Exception as e:
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(f"Test {test_name} crashed on attempt {i}/{times}: {e}\n")
                import traceback
                f.write(traceback.format_exc())
            return (test_name, False)

    return (test_name, True)

# =============================
#           主程序入口
# =============================
if __name__ == "__main__":
    TESTS = [
        "TestPersist13C",
        "TestPersist23C",
        "TestPersist33C",
        "TestFigure83C",
        "TestUnreliableAgree3C",
        "TestFigure8Unreliable3C",
        "TestReliableChurn3C",
        "TestUnreliableChurn3C",
    ]

    TOTAL_TIMES = 2
    LOG_DIR = "./test_logs"

    print(f"🚀 Starting {len(TESTS)} tests in parallel, each running {TOTAL_TIMES} times.")
    print(f"📂 Logs will be saved to: {os.path.abspath(LOG_DIR)}\n")

    failed_tests = []

    # 使用 ProcessPoolExecutor 并行运行所有测试
    # max_workers = len(TESTS) 表示每个测试一个进程（完全并行）
    with ProcessPoolExecutor(max_workers=len(TESTS)) as executor:
        # 提交所有任务
        future_to_test = {
            executor.submit(run_single_test_n_times, test, TOTAL_TIMES, "PASS", LOG_DIR): test
            for test in TESTS
        }

        # 等待完成并收集结果
        for future in as_completed(future_to_test):
            test_name, success = future.result()
            if success:
                print(f"✅ {test_name} passed all {TOTAL_TIMES} runs.")
            else:
                print(f"❌ {test_name} failed. See log: {os.path.join(LOG_DIR, f'log_{test_name}.log')}")
                failed_tests.append(test_name)

    # 最终结果汇总
    print("\n" + "="*60)
    if failed_tests:
        print(f"🛑 {len(failed_tests)} test(s) FAILED:")
        for t in failed_tests:
            print(f"   - {t}")
        sys.exit(1)
    else:
        print(f"🎉 All {len(TESTS)} tests passed {TOTAL_TIMES} times each!")
        sys.exit(0)
