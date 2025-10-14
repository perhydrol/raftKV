6.5840 - 2025年春季
6.5840 实验四：容错键值存储服务

协作政策 // 提交实验 // Go环境配置 // 指南 // Piazza论坛
引言

在本实验中，您将使用实验三构建的Raft库创建一个容错的键值存储服务。对客户端而言，该服务与实验二中的服务器类似。但不同的是，本服务由一组通过Raft协议维护相同数据库的服务器组成。只要多数服务器存活且能相互通信，即使出现其他故障或网络分区，您的键值服务也应持续处理客户端请求。完成实验四后，您将实现Raft交互图中所示的所有组件（客户端、服务和Raft）。

客户端将通过Clerk与您的键值服务交互，这与实验二相同。Clerk实现的Put和Get方法语义与实验二一致：Put操作具有最多一次语义，且Put/Get操作必须形成可线性化的历史记录。

对单服务器而言，提供可线性化相对容易。但在服务复制的场景下会更具挑战——所有服务器必须为并发请求选择相同的执行顺序，必须避免使用过时状态响应客户端，且必须在故障恢复后保留所有已确认的客户端更新。

本实验包含三个部分。在A部分，您将使用Raft实现一个与具体请求无关的复制状态机包rsm。在B部分，您将使用rsm实现不带快照功能的复制键值服务。在C部分，您将使用实验3D实现的快照功能，使Raft能够丢弃旧日志条目。请分别在截止日期前提交每个部分。

建议您复习扩展版Raft论文，特别是第7节（第8节除外）。如需更广视角，可参阅Chubby、Paxos Made Live、Spanner、Zookeeper、Harp、Viewstamped Replication和Bolosky等论文。

请尽早开始。
环境准备

我们在src/kvraft1中提供了框架代码和测试。框架代码使用src/kvraft1/rsm包来实现服务器复制。服务器必须实现rsm中定义的StateMachine接口，才能通过rsm进行自我复制。您的主要工作是实现rsm以提供与服务器无关的复制功能。同时需要修改kvraft1/client.go和kvraft1/server.go来实现服务器特定功能。这种分离设计让您可以在后续实验中复用rsm。您可能会复用实验二的代码（例如通过复制或导入"src/kvsrv1"包来复用服务器代码），但这不是强制要求。

执行以下命令开始实验。别忘了先运行git pull获取最新代码：

$ cd ~/6.5840
$ git pull
..

A部分：复制状态机（RSM）（中等/困难）

$ cd src/kvraft1/rsm
$ go test -v
=== RUN   TestBasic
测试RSM基础功能（可靠网络）...
..
    config.go:147: one: 耗时过长

在使用Raft进行复制的典型客户端/服务器场景中，服务通过两种方式与Raft交互：服务领导者通过调用raft.Start()提交客户端操作，所有服务副本通过Raft的applyCh接收已提交的操作并执行。在领导者节点上，这两个活动会产生交互：某些服务器协程处理客户端请求时已调用raft.Start()，每个协程都在等待其操作被提交并获取执行结果；而随着已提交操作出现在applyCh上，每个操作都需要被服务执行，且执行结果需要传递给最初调用raft.Start()的协程以便返回给客户端。

rsm包封装了上述交互过程。它作为服务（如键值数据库）与Raft之间的中间层。您需要在rsm/rsm.go中实现一个读取applyCh的"reader"协程，以及一个rsm.Submit()函数——该函数会为客户端操作调用raft.Start()，然后等待reader协程返回该操作的执行结果。

使用rsm的服务在rsm reader协程中表现为一个提供DoOp()方法的StateMachine对象。reader协程应将每个已提交操作传递给DoOp()，并将DoOp()的返回值交还给对应的rsm.Submit()调用。DoOp()的参数和返回值类型为any，实际类型应分别与服务传递给rsm.Submit()的参数和返回值类型相同。

服务应将每个客户端操作传递给rsm.Submit()。为帮助reader协程匹配applyCh消息与等待的rsm.Submit()调用，Submit()应将每个客户端操作封装在Op结构体中并附加唯一标识符。Submit()应等待操作提交并执行后，返回执行结果（即DoOp()返回值）。如果raft.Start()显示当前节点不是Raft领导者，Submit()应返回rpc.ErrWrongLeader错误。Submit()还应检测并处理调用raft.Start()后领导权变更导致操作丢失（未提交）的情况。

在A部分，rsm测试程序充当服务，提交被解释为对单个整数状态进行递增的操作。在B部分，您将把rsm用作键值服务的一部分，该服务需实现StateMachine（及DoOp()）并调用rsm.Submit()。

如果一切正常，客户端请求的事件序列如下：

1. 客户端向服务领导者发送请求
2. 服务领导者调用rsm.Submit()提交请求
3. rsm.Submit()调用raft.Start()提交请求后等待
4. Raft提交请求并通过所有节点的applyCh发送
5. 每个节点的rsm reader协程从applyCh读取请求并传递给服务的DoOp()
6. 在领导者节点，rsm reader协程将DoOp()返回值交还给最初提交请求的Submit()协程，Submit()返回该值

服务器之间不应直接通信，只能通过Raft进行交互。

请实现rsm.go中的Submit()方法和reader协程。当通过rsm 4A测试时即完成本任务：

$ cd src/kvraft1/rsm
$ go test -v -run 4A
=== RUN   TestBasic4A
测试RSM基础功能（可靠网络）...
  ... 通过 --   1.2  3    48    0
--- PASS: TestBasic4A (1.21s)
=== RUN   TestLeaderFailure4A
  ... 通过 --  9223372036.9  3    31    0
--- PASS: TestLeaderFailure4A (1.50s)
PASS
ok      6.5840/kvraft1/rsm      2.887s

注意事项：
- 无需修改Raft ApplyMsg或Raft RPC（如AppendEntries）的字段，但允许修改
- 需处理rsm领导者调用Start()提交请求后，在请求提交到日志前失去领导权的情况。解决方案包括通过检测Raft任期变更或Start()返回索引处出现不同请求来发现领导权丢失，并从Submit()返回rpc.ErrWrongLeader
- 测试程序在关闭节点时会调用Raft的rf.Kill()。Raft应关闭applyCh以便rsm感知关闭并退出所有循环

B部分：无快照的键值服务（中等）

$ cd src/kvraft1
$ go test -v -run TestBasic4B
=== RUN   TestBasic4B
测试：单客户端（4B基础）（可靠网络）...
    kvtest.go:62: 错误类型不符
$

现在您将使用rsm包复制键值服务器。每个服务器（"kvserver"）都关联一个rsm/Raft节点。Clerk向Raft领导者对应的kvserver发送Put()和Get() RPC。kvserver代码将Put/Get操作提交给rsm，rsm通过Raft复制该操作并在每个节点调用服务的DoOp，将操作应用到节点的键值数据库，目的是让服务器维护相同的键值数据库副本。

Clerk有时不知道哪个kvserver是Raft领导者。如果Clerk向错误的kvserver发送RPC，或无法联系到kvserver，应尝试向其他kvserver重新发送。如果键值服务将操作提交到Raft日志（即对键值状态机应用了操作），领导者通过响应RPC向Clerk报告结果。如果操作提交失败（例如领导者变更），服务器报告错误，Clerk应重试其他服务器。

您的kvserver之间不应直接通信，只能通过Raft交互。
首要任务是实现无丢包无服务器故障场景下的解决方案。

可自由将实验二的客户端代码（kvsrv1/client.go）复制到kvraft1/client.go。需要添加选择kvserver发送RPC的逻辑。

还需在server.go实现Put()和Get() RPC处理程序。这些处理程序应通过rsm.Submit()将请求提交给Raft。当rsm包从applyCh读取命令时，会调用您将在server.go实现的DoOp方法。

当稳定通过测试套件的第一个测试（go test -v -run TestBasic4B）时，即完成本任务。

- 如果kvserver不处于多数派中，不应完成Get() RPC（避免返回过期数据）。简单解决方案是将每个Get()（及Put()）都通过Submit()存入Raft日志，无需实现第8节所述的只读操作优化
- 建议从一开始就添加锁机制，避免死锁的需求会影响整体代码设计。使用go test -race检查代码是否存在竞态条件

现在需要修改解决方案以应对网络和服务器故障。面临的问题是Clerk可能需多次发送RPC直到找到正确响应的kvserver。如果领导者在提交日志条目后立即故障，Clerk可能收不到回复，从而向新领导者重发请求。每个Clerk.Put()调用应对特定版本号仅执行一次。

添加故障处理代码。您的Clerk可采用类似实验二的重试策略，包括当重试Put RPC的响应丢失时返回ErrMaybe。当代码稳定通过所有4B测试（go test -v -run 4B）时即告完成。

- 注意rsm领导者可能失去领导权并从Submit()返回rpc.ErrWrongLeader。此时应让Clerk向其他服务器重发请求直到找到新领导者
- 可能需要修改Clerk使其记住最后成功RPC的领导者服务器，并优先向该服务器发送后续RPC。这可避免每次RPC都寻找领导者，有助于快速通过某些测试

现在代码应能通过实验4B测试：

$ cd kvraft1
$ go test -run 4B
测试：单客户端（4B基础）...
  ... 通过 --   3.2  5  1041  183
测试：单客户端（4B速度）...
  ... 通过 --  15.9  3  3169    0
测试：多客户端（4B多客户端）...
  ... 通过 --   3.9  5  3247  871
测试：不可靠网络，多客户端（4B不可靠网络，多客户端）...
  ... 通过 --   5.3  5  1035  167
测试：不可靠网络，单客户端（4B多数派进展）...
  ... 通过 --   2.9  5   155    3
测试：少数派无进展（4B）...
  ... 通过 --   1.6  5   102    3
测试：恢复后完成（4B）...
  ... 通过 --   1.3  5    67    4
测试：网络分区，单客户端（4B分区，单客户端）...
  ... 通过 --   6.2  5   958  155
测试：网络分区，多客户端（4B分区，多客户端）...
  ... 通过 --   6.8  5  3096  855
测试：重启，单客户端（4B重启，单客户端）...
  ... 通过 --   6.7  5   311   13
测试：重启，多客户端（4B重启，多客户端）...
  ... 通过 --   7.5  5  1223   95
测试：不可靠网络，重启，多客户端（4B不可靠网络，重启，多客户端）...
  ... 通过 --   8.4  5   804   33
测试：重启，分区，多客户端（4B重启，分区，多客户端）...
  ... 通过 --  10.1  5  1308  105
测试：不可靠网络，重启，分区，多客户端（4B不可靠网络，重启，分区，多客户端）...
  ... 通过 --  11.9  5  1040   33
测试：不可靠网络，重启，分区，随机密钥，多客户端（4B不可靠网络，重启，分区，随机密钥，多客户端）...
  ... 通过 --  12.1  7  2801   93
PASS
ok      6.5840/kvraft1  103.797s

每行"通过"后的数字分别表示实际时间（秒）、节点数、发送的RPC数（含客户端RPC）、执行的键值操作数（Clerk Get/Put调用）。

C部分：带快照的键值服务（中等）

目前您的键值服务器未调用Raft库的Snapshot()方法，因此重启服务器必须重放完整持久化Raft日志才能恢复状态。现在需要修改kvserver和rsm，使其与Raft协作使用实验3D的快照功能来节省日志空间并减少重启时间。

测试程序向StartKVServer()传递maxraftstate参数，该参数会传递给rsm。maxraftstate表示持久化Raft状态的最大允许字节数（含日志但不含快照）。您需要比较maxraftstate与rf.PersistBytes()。当rsm检测到Raft状态大小接近该阈值时，应通过调用Raft的Snapshot保存快照。rsm可通过调用StateMachine接口的Snapshot方法获取kvserver快照。如果maxraftstate为-1则无需快照。该限制适用于Raft作为第一个参数传递给persister.Save()的GOB编码字节数。

可在tester1/persister.go找到persister对象的源代码。

修改rsm使其检测持久化Raft状态是否过大，然后向Raft提交快照。当rsm服务器重启时，应通过persister.ReadSnapshot()读取快照，如果快照长度大于零，则将快照传递给StateMachine的Restore()方法。通过TestSnapshot4C测试即完成本任务：

$ cd kvraft1/rsm
$ go test -run TestSnapshot4C
=== RUN   TestSnapshot4C
  ... 通过 --  9223372036.9  3   230    0
--- PASS: TestSnapshot4C (3.88s)
PASS
ok      6.5840/kvraft1/rsm      3.882s

注意事项：
- 考虑rsm应在何时快照状态，以及除服务器状态外快照还应包含哪些内容。Raft使用Save()将每个快照及对应Raft状态存入persister对象，可通过ReadSnapshot()读取最新存储的快照
- 快照中存储的结构体字段需首字母大写

实现kvraft1/server.go中rsm调用的Snapshot()和Restore()方法。修改rsm以处理包含快照的applyCh消息。

- 此任务可能暴露Raft和rsm库中的潜在错误。如果修改Raft实现，请确保其仍能通过所有实验三测试
- 实验四测试的合理耗时约为400秒实际时间和700秒CPU时间

您的代码应通过4C测试（如下例所示）以及4A+B测试（且Raft仍需通过实验三测试）：

$ go test -run 4C
测试：快照，单客户端（4C快照RPC）...
测试：InstallSnapshot RPC（4C）...
  ... 通过 --   4.5  3   241   64
测试：快照，单客户端（4C快照大小合理）...
  ... 通过 --  11.4  3  2526  800
测试：快照，单客户端（4C速度）...
  ... 通过 --  14.2  3  3149    0
测试：重启，快照，单客户端（4C重启，快照，单客户端）...
  ... 通过 --   6.8  5   305   13
测试：重启，快照，多客户端（4C重启，快照，多客户端）...
  ... 通过 --   9.0  5  5583  795
测试：不可靠网络，快照，多客户端（4C不可靠网络，快照，多客户端）...
  ... 通过 --   4.7  5   977  155
测试：不可靠网络，重启，快照，多客户端（4C不可靠网络，重启，快照，多客户端）...
  ... 通过 --   8.6  5   847   33
测试：不可靠网络，重启，分区，快照，多客户端（4C不可靠网络，重启，分区，快照，多客户端）...
  ... 通过 --  11.5  5   841   33
测试：不可靠网络，重启，分区，快照，随机密钥，多客户端（4C不可靠网络，重启，分区，快照，随机密钥，多客户端）...
  ... 通过 --  12.8  7  2903   93
PASS
ok      6.5840/kvraft1  83.543s