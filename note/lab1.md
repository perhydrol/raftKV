## Worker

考虑到实验简单的将10秒作为超时时间，Worker 无需像真正的 MapReduce 那样时不时发送心跳包，以维护 Master 节点和 Worker 节点工作状态的同步。