import subprocess
import os
import re
from datetime import datetime

# 正则匹配 Go 日志时间格式，例如：
# [2025-09-02 10:10:41.514067761 +0800 CST m=+21.682640196]
TIMESTAMP_PATTERN = re.compile(
    r'^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)'
)

def parse_timestamp(line):
    """
    从日志行提取时间戳字符串，并转换为 datetime 对象用于排序。
    只取到微秒（Python datetime 不支持纳秒，但可截断到6位）
    """
    match = TIMESTAMP_PATTERN.match(line)
    if not match:
        # 无法匹配时间戳的行，返回 None，排序时将放在最后
        return None
    timestamp_str = match.group(1)  # 如 "2025-09-02 10:10:41.514067"
    # 截取到微秒（6位小数），补零或截断
    parts = timestamp_str.split('.')
    base_time = parts[0]
    microsec_str = (parts[1] + '000000')[:6]  # 补足6位，取前6位
    timestamp_with_us = f"{base_time}.{microsec_str}"
    try:
        return datetime.strptime(timestamp_with_us, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return None  # 解析失败也返回 None

def sort_log_lines(log_lines):
    """
    将日志行按时间戳排序。
    无法解析时间戳的行（如 panic、stack trace）放在最后。
    """
    with_timestamp = []
    without_timestamp = []

    for line in log_lines:
        ts = parse_timestamp(line)
        if ts is not None:
            with_timestamp.append((ts, line))
        else:
            without_timestamp.append(line)

    # 按时间戳排序
    sorted_with_ts = sorted(with_timestamp, key=lambda x: x[0])
    # 提取排序后的日志行
    sorted_lines = [item[1] for item in sorted_with_ts]
    # 无时间戳的放在最后
    sorted_lines.extend(without_timestamp)
    return sorted_lines

def run_go_test_n_times(command, times=100, fail_keyword="Fatal", log_file="./log.log"):
    for i in range(1, times + 1):
        print(f"[{i}/{times}] Running: {command}")
        
        try:
            result = subprocess.run(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=60
            )

            full_output = result.stdout + result.stderr
            lines = full_output.splitlines()

            # 检查是否包含失败关键字
            if fail_keyword in full_output:
                print(f"❌ Test failed on attempt {i}. Found keyword: '{fail_keyword}'")
                
                # 排序日志
                sorted_lines = sort_log_lines(lines)
                
                # 写入日志文件
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.write(f"Test run #{i} failed. Command: {command}\n")
                    f.write(f"Sorted by timestamp (ascending):\n")
                    f.write("="*80 + "\n")
                    f.write("\n".join(sorted_lines))
                    f.write("\n" + "="*80 + "\n")
                    f.write("End of sorted log.\n")
                
                abs_path = os.path.abspath(log_file)
                print(f"📝 Sorted log written to {abs_path}")
                return False  # 失败退出
            
            print(f"✅ Test {i} passed. Output discarded.")
        
        except subprocess.TimeoutExpired:
            error_msg = f"❌ Test {i} timed out after 60 seconds."
            print(error_msg)
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(error_msg + "\n")
                f.write("Command was:\n")
                f.write(command + "\n")
            print(f"📝 Timeout logged to {os.path.abspath(log_file)}")
            return False
        
        except Exception as e:
            error_msg = f"❌ Unexpected error during test {i}: {e}"
            print(error_msg)
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(error_msg + "\n")
                import traceback
                f.write(traceback.format_exc())
            print(f"📝 Error details written to {os.path.abspath(log_file)}")
            return False

    print(f"🎉 All {times} tests passed!")
    return True

# =============================
#           主程序入口
# =============================
if __name__ == "__main__":
    RUN_COMMAND = "go test -run 3A"
    TOTAL_TIMES = 100
    LOG_PATH = "./log.log"

    run_go_test_n_times(
        command=RUN_COMMAND,
        times=TOTAL_TIMES,
        fail_keyword="Fatal",
        log_file=LOG_PATH
    )
