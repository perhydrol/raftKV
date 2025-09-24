#!/bin/bash

counter=1
while true; do
    echo "=== 运行第 $counter 次测试 ==="
    ./raft.test -test.run TestFigure8Unreliable3C > log2.log
    exit_code=$?
    
    if [ $exit_code -ne 0 ]; then
        echo "❌ 测试失败！退出码: $exit_code"
        echo "失败发生在第 $counter 次运行"
        break
    fi
    
    echo "✅ 第 $counter 次运行成功"
    ((counter++))
done

echo "总共运行了 $((counter-1)) 次测试"

