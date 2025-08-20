6.5840 - 2025年春季  
6.5840 实验3：Raft  

协作政策 // 提交实验 // 配置Go环境 // 指导 // Piazza  
简介  

这是构建容错键值存储系统系列实验中的第一个实验。在本实验中，你将实现Raft——一种复制状态机协议。在下一个实验中，你将在Raft之上构建一个键值服务。随后，你将通过“分片”技术将服务分布到多个复制状态机上以提高性能。  

复制服务通过在多个副本服务器上存储其状态（即数据）的完整副本来实现容错。即使部分服务器发生故障（崩溃或网络不稳定），复制也能确保服务持续运行。挑战在于故障可能导致副本持有不一致的数据。  

Raft将客户端请求组织成一个称为日志的序列，并确保所有副本服务器看到相同的日志。每个副本按日志顺序执行客户端请求，将其应用到服务的本地状态副本。由于所有活跃副本看到的日志内容相同，它们以相同顺序执行相同请求，从而保持服务状态一致。如果服务器故障后恢复，Raft会负责更新其日志。只要多数服务器存活且能相互通信，Raft就能持续运行。若无法形成多数派，Raft会暂停进展，但一旦多数派恢复通信，它将从中断处继续。  

在本实验中，你将把Raft实现为一个Go对象类型及其关联方法，作为更大服务中的一个模块。一组Raft实例通过RPC相互通信以维护复制的日志。你的Raft接口将支持无限序列的编号命令（即日志条目）。条目通过索引编号，特定索引的日志条目最终会被提交。此时，Raft应将该条目发送给上层服务执行。  

请遵循扩展Raft论文中的设计，特别注意图2。你将实现论文中的大部分内容，包括保存持久化状态并在节点故障重启后读取它。但无需实现集群成员变更（第6节）。  

本实验分为四个部分提交，每部分需在对应截止日期前完成。  
准备工作  

执行`git pull`获取最新的实验代码。  

若已完成实验1，你已拥有实验源代码。否则，可参考实验1的说明通过git获取代码。  

我们提供了框架代码`src/raft/raft.go`和测试用例`src/raft/raft_test.go`，测试将用于评估你的实现。  

评分时我们会关闭`-race`标志运行测试，但开发时请使用`-race`确保代码无竞态条件。  

运行以下命令开始（别忘了先`git pull`）：  
```bash
$ cd ~/6.5840
$ git pull
...
$ cd src/raft1
$ go test
Test (3A): initial election (reliable network)...
Fatal: expected one leader, got none
--- FAIL: TestInitialElection3A (4.90s)
Test (3A): election after network failure (reliable network)...
Fatal: expected one leader, got none
--- FAIL: TestReElection3A (5.05s)
...
```  

代码实现  
在`raft/raft.go`中添加代码实现Raft。文件中包含框架代码及RPC收发示例。  

你的实现需支持以下接口（详见`raft.go`注释）：  
```go
// 创建Raft服务器实例:
rf := Make(peers, me, persister, applyCh)

// 开始对新日志条目达成一致:
rf.Start(command interface{}) (index, term, isleader)

// 查询当前任期和领导状态:
rf.GetState() (term, isLeader)

// 每个新提交的日志条目需通过ApplyMsg通知上层服务:
type ApplyMsg
```  

服务调用`Make(peers,me,…)`创建Raft节点，`peers`是包含所有节点网络标识的数组，`me`是当前节点在数组中的索引。`Start(command)`请求Raft将命令追加到复制日志中，需立即返回无需等待。实现需通过`applyCh`通道向上层发送每个新提交的日志条目。  

Raft节点应使用`labrpc`包（位于`src/labrpc`）交换RPC。测试会通过该包模拟网络延迟、乱序和丢包。请确保代码与原始`labrpc`兼容，且仅通过RPC交互（禁止共享变量或文件）。  

后续实验基于此，请预留充足时间编写健壮代码。  
### 3A部分：领导者选举（中等）  
实现Raft领导者选举和心跳（不含日志条目的`AppendEntries` RPC）。目标是：  
1. 选出一个领导者  
2. 无故障时保持领导者  
3. 旧领导者故障或通信中断时选出新领导者  

运行`go test -run 3A`测试代码。  

关键提示：  
- 通过测试器运行代码（`go test -run 3A`）  
- 按论文图2实现选举相关状态和规则  
- 在`Raft`结构体中添加选举状态，定义日志条目结构  
- 填充`RequestVoteArgs`和`RequestVoteReply`结构体  
- 在`Make()`中创建后台goroutine定期触发选举（当长时间未收到消息时）  
- 实现`RequestVote()` RPC处理程序使服务器相互投票  
- 定义`AppendEntries` RPC结构（心跳），领导者定期发送  
- 心跳频率需≤10次/秒  
- 旧领导者故障后5秒内需选出新领导者（多数派存活时）  
- 选举超时建议大于300ms（因心跳限制）但不超过5秒  
- 使用`time.Sleep()`而非`time.Timer/Ticker`  
- 调试困难时重读图2，选举逻辑分散在多个部分  
- 实现`GetState()`  
- 在循环中检查`rf.killed()`避免已终止实例输出混乱信息  
- Go RPC仅传输大写字母开头的字段名  
- 调试是本实验最大挑战，参考指导页的调试技巧  
- 测试失败时会生成带事件标记的时间线可视化文件，可通过`tester.Annotate()`添加注释  

提交前确保通过3A测试，输出示例如下：  
```bash
$ go test -run 3A
Test (3A): initial election (reliable network)...
  ... Passed --   3.6  3   106    0
Test (3A): election after network failure (reliable network)...
  ... Passed --   7.6  3   304    0
Test (3A): multiple elections (reliable network)...
  ... Passed --   8.4  7   954    0
PASS
ok      6.5840/raft1    19.834s
```  
每行"Passed"后的数字分别为：测试时间（秒）、Raft节点数、RPC发送数、RPC总字节数、已提交日志条目数。所有测试总时间超过600秒或单测试超过120秒将导致评分失败。  

### 3B部分：日志（困难）  
实现领导者和跟随者追加日志条目的代码，通过`go test -run 3B`测试。  

关键提示：  
- 日志建议0-indexed（首条目term=0），使首次`AppendEntries`的`PrevLogIndex=0`有效  
- 先通过`TestBasicAgree3B()`：实现`Start()`，按图2收发日志条目，通过`applyCh`发送已提交条目  
- 实现选举限制（论文5.4.1节）  
- 循环中需添加延迟（如`time.Sleep(10ms)`或条件变量）避免CPU占用过高  
- 编写清晰代码便于后续实验  
- 测试失败时通过`raft_test.go`追溯测试逻辑  

运行缓慢可能导致后续实验测试失败，可用`time`命令检查耗时：  
```bash
$ time go test -run 3B
...
ok      6.5840/raft1    48.353s
go test -run 3B  1.37s user 0.74s system 4% cpu 48.865 total
```  
若3B测试实际时间超过1分钟或CPU时间超过5秒，需优化RPC超时或循环逻辑。  

### 3C部分：持久化（困难）  
Raft服务器重启后应从持久化状态恢复。论文图2标明了需持久化的状态。  

实现提示：  
- 使用`Persister`对象（非磁盘）保存/恢复状态  
- 完成`persist()`和`readPersist()`，用`labgob`编码状态（注意字段名大写）  
- 在状态变更处调用`persist()`  
- 需优化`nextIndex`回退逻辑（参考论文第7-8页）：  
  - 拒绝消息包含冲突条目的任期（XTerm）、该任期的首索引（XIndex）和日志长度（XLen）  
  - 领导者逻辑：  
    - 无XTerm时：`nextIndex = XIndex`  
    - 有XTerm时：`nextIndex = 该任期最后条目索引 + 1`  
    - 跟随者日志过短时：`nextIndex = XLen`  

3C测试比3A/3B更严格，需确保之前部分完全正确。通过示例如下：  
```bash
$ go test -run 3C
...
ok      6.5840/raft1    126.054s
```  
提交前建议多次运行测试确保稳定性：  
```bash
$ for i in {0..10}; do go test; done
```  

### 3D部分：日志压缩（困难）  
长期运行的服务需定期生成快照并丢弃旧日志，以减小持久化数据量并加速重启。当跟随者落后太多时，领导者需发送快照+后续日志（论文第7节）。  

实现要求：  
- 提供`Snapshot(index int, snapshot []byte)`供服务调用  
- 测试器会定期调用`Snapshot()`（实验4中键值服务将存储完整键值表快照）  
- 快照包含≤index的所有日志条目，需丢弃旧日志  
- 实现`InstallSnapshot` RPC使领导者向落后跟随者发送快照  
- 快照通过`applyCh`发送给服务（`ApplyMsg`结构已包含所需字段）  
- 服务状态只能前进不能回退  
- 崩溃重启时从持久化的Raft状态和快照恢复  
- 使用`persister.Save()`的第二个参数保存快照（无快照时传nil）  

实现提示：  
- 先修改代码支持从索引X开始存储日志（初始X=0通过3B/3C测试）  
- 实现`Snapshot(index)`丢弃旧日志并设X=index  
- 常见问题：跟随者追赶领导者耗时过长  
- `InstallSnapshot` RPC需一次性发送完整快照（不实现分片机制）  
- 确保旧日志可被GC回收（无残留指针）  
- 无`-race`时全套测试应在6分钟（实际时间）/1分钟（CPU时间）内完成  

通过3D测试示例如下：  
```bash
$ go test -run 3D
...
ok      6.5840/raft1    195.006s
```  
最终代码需通过所有3A-3D测试。