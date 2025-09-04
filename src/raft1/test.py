import subprocess
import os
import re
from datetime import datetime

# 正则匹配 Go 日志时间格式
TIMESTAMP_PATTERN = re.compile(
    r'^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)'
)

def parse_timestamp(line):
    """解析时间戳字符串为 datetime 对象（精度到微秒）"""
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
    """按时间戳排序日志行，无法解析的放最后"""
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

def run_go_test_n_times(command, times=100, success_indicator="PASS", log_file="./log.log"):
    """
    执行命令 n 次，仅当输出中存在独立行 "PASS" 视为成功。
    """
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

            # ✅ 新逻辑：检查是否有某一行完全等于 "PASS"
            if any(line.strip() == success_indicator for line in lines):
                print(f"✅ Test {i} passed (found '{success_indicator}'). Output discarded.")
                continue  # 成功，丢弃日志，继续下一次
            else:
                print(f"❌ Test failed on attempt {i}. No '{success_indicator}' found.")
                
                # 排序日志
                sorted_lines = sort_log_lines(lines)
                
                # 写入日志文件
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.write(f"Test run #{i} failed. Command: {command}\n")
                    f.write(f"Expected a line containing exactly: '{success_indicator}'\n")
                    f.write(f"Actual output did not contain it.\n")
                    f.write("="*80 + "\n")
                    f.write("\n".join(sorted_lines))
                    f.write("\n" + "="*80 + "\n")
                    f.write("End of sorted log.\n")
                
                abs_path = os.path.abspath(log_file)
                print(f"📝 Sorted log written to {abs_path}")
                return False  # 失败即退出

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
    RUN_COMMAND = "go test -race -run 3A"
    TOTAL_TIMES = 100
    LOG_PATH = "./log.log"

    run_go_test_n_times(
        command=RUN_COMMAND,
        times=TOTAL_TIMES,
        success_indicator="PASS",   # 必须完全匹配此行（大小写敏感）
        log_file=LOG_PATH
    )
