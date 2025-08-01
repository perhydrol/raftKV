pdos.csail.mit.edu  
6.5840 实验2：键值存储服务器  
12-15分钟  
## 简介  

在本实验中，你将构建一个单机键值存储服务器，确保即使在网络故障的情况下，每个Put操作最多执行一次，并且所有操作都是线性一致的。你将使用这个KV服务器来实现一个锁。后续实验会通过复制类似这样的服务器来处理服务器崩溃问题。  

### KV服务器  

每个客户端通过Clerk与键值服务器交互，Clerk向服务器发送RPC请求。客户端可以向服务器发送两种不同的RPC：Put(key, value, version)和Get(key)。服务器维护一个内存中的映射表，记录每个键对应的（值，版本号）元组。键和值都是字符串。版本号记录了键被写入的次数。Put(key, value, version)仅在Put的版本号与服务器中该键的版本号匹配时，才会安装或替换映射表中该键的值。如果版本号匹配，服务器还会递增该键的版本号。如果版本号不匹配，服务器应返回rpc.ErrVersion。客户端可以通过调用版本号为0的Put来创建新键（服务器存储的版本号将为1）。如果Put的版本号大于0且键不存在，服务器应返回rpc.ErrNoKey。  

Get(key)获取键的当前值及其关联的版本号。如果键在服务器中不存在，服务器应返回rpc.ErrNoKey。  

为每个键维护版本号有助于通过Put实现锁，并在网络不可靠且客户端重传时确保Put操作的至多一次语义。  

当你完成本实验并通过所有测试后，从调用Clerk.Get和Clerk.Put的客户端角度来看，你将拥有一个线性一致的键值服务。也就是说，如果客户端操作不是并发的，每个客户端的Clerk.Get和Clerk.Put将观察到由前序操作序列所隐含的状态修改。对于并发操作，返回值与最终状态将与这些操作以某种顺序一次执行一个时的结果相同。如果操作在时间上重叠，则它们是并发的：例如，如果客户端X调用Clerk.Put()，客户端Y调用Clerk.Put()，然后客户端X的调用返回。一个操作必须观察到在该操作开始之前已完成的所有操作的效果。更多背景信息请参阅线性一致性的FAQ。  

线性一致性对应用程序很方便，因为它类似于单服务器一次处理一个请求的行为。例如，如果一个客户端从服务器收到更新请求的成功响应，随后其他客户端发起的读取操作将保证看到该更新的效果。对于单服务器来说，提供线性一致性相对容易。  

## 开始实验  

我们在src/kvsrv1中提供了框架代码和测试。kvsrv1/client.go实现了Clerk，客户端用它管理与服务器的RPC交互；Clerk提供了Put和Get方法。kvsrv1/server.go包含服务器代码，包括实现RPC请求服务器端的Put和Get处理程序。你需要修改client.go和server.go。RPC请求、回复和错误值定义在kvsrv1/rpc包的kvsrv1/rpc/rpc.go文件中，你应该查看该文件，但无需修改rpc.go。  

要开始运行，请执行以下命令。别忘了git pull以获取最新代码。  

```bash
$ cd ~/6.5840  
$ git pull  
...  
$ cd src/kvsrv1  
$ go test -v  
=== RUN   TestReliablePut  
One client and reliable Put (reliable network)...  
    kvsrv_test.go:25: Put err ErrNoKey  
...  
$  
```

#### 可靠网络下的键值服务器（简单）  

你的第一个任务是实现一个在没有消息丢失的情况下工作的解决方案。你需要在client.go的Clerk Put/Get方法中添加RPC发送代码，并在server.go中实现Put和Get的RPC处理程序。  

当你通过测试套件中的Reliable测试时，即完成此任务：  

```bash
$ go test -v -run Reliable  
=== RUN   TestReliablePut  
One client and reliable Put (reliable network)...  
  ... Passed --   0.0  1     5    0  
--- PASS: TestReliablePut (0.00s)  
=== RUN   TestPutConcurrentReliable  
Test: many clients racing to put values to the same key (reliable network)...  
info: linearizability check timed out, assuming history is ok  
  ... Passed --   3.1  1 90171 90171  
--- PASS: TestPutConcurrentReliable (3.07s)  
=== RUN   TestMemPutManyClientsReliable  
Test: memory use many put clients (reliable network)...  
  ... Passed --   9.2  1 100000    0  
--- PASS: TestMemPutManyClientsReliable (16.59s)  
PASS  
ok  	6.5840/kvsrv1	19.681s  
```

每个Passed后的数字分别是实际时间（秒）、常数1、发送的RPC数量（包括客户端RPC）和执行的键值操作数量（Clerk Get和Put调用）。  

- 使用go test -race检查代码是否存在竞态条件。  

#### 使用键值Clerk实现锁（中等）  

在许多分布式应用中，运行在不同机器上的客户端使用键值服务器来协调它们的活动。例如，ZooKeeper和Etcd允许客户端通过分布式锁进行协调，类似于Go程序中的线程通过锁（如sync.Mutex）协调。Zookeeper和Etcd通过条件Put实现这种锁。  

在本练习中，你的任务是在客户端Clerk.Put和Clerk.Get调用的基础上实现一个锁。锁支持两种方法：Acquire和Release。锁的规范是：同一时间只有一个客户端能成功获取锁；其他客户端必须等待第一个客户端通过Release释放锁。  

我们在src/kvsrv1/lock/中提供了框架代码和测试。你需要修改src/kvsrv1/lock/lock.go。你的Acquire和Release代码可以通过调用lk.ck.Put()和lk.ck.Get()与你的键值服务器通信。  

如果客户端在持有锁时崩溃，锁将永远不会被释放。在本实验之外的更复杂设计中，客户端会为锁附加租约。当租约到期时，锁服务器会代表客户端释放锁。本实验中客户端不会崩溃，你可以忽略此问题。  

实现Acquire和Release。当你的代码通过lock子目录中的测试套件的Reliable测试时，即完成此练习：  

```bash
$ cd lock  
$ go test -v -run Reliable  
=== RUN   TestOneClientReliable  
Test: 1 lock clients (reliable network)...  
  ... Passed --   2.0  1   974    0  
--- PASS: TestOneClientReliable (2.01s)  
=== RUN   TestManyClientsReliable  
Test: 10 lock clients (reliable network)...  
  ... Passed --   2.1  1 83194    0  
--- PASS: TestManyClientsReliable (2.11s)  
PASS  
ok  	6.5840/kvsrv1/lock	4.120s  
```

如果你尚未实现锁，第一个测试会通过。  

此练习需要编写的代码不多，但比前一个练习需要更多独立思考。  

- 你需要为每个锁客户端生成唯一标识符；调用kvtest.RandValue(8)生成随机字符串。  
- 锁服务应使用特定键存储“锁状态”（你需要明确锁状态的具体内容）。使用的键通过src/kvsrv1/lock/lock.go中MakeLock的参数l传递。  

#### 消息丢失的键值服务器（中等）  

本练习的主要挑战是网络可能会重新排序、延迟或丢弃RPC请求和/或回复。为了从丢弃的请求/回复中恢复，Clerk必须不断重试每个RPC，直到收到服务器的回复。  

如果网络丢弃了RPC请求消息，客户端重新发送请求将解决问题：服务器将接收并仅执行重新发送的请求。  

然而，网络也可能丢弃RPC回复消息。客户端不知道是哪条消息被丢弃；客户端仅观察到未收到回复。如果是回复被丢弃，客户端重新发送RPC请求，服务器将收到两份请求副本。这对Get来说没问题，因为Get不会修改服务器状态。对于Put RPC，如果版本号相同，重新发送也是安全的，因为服务器根据版本号条件执行Put；如果服务器已接收并执行了Put RPC，它将对该RPC的重传副本返回rpc.ErrVersion，而不是再次执行Put。  

一个棘手的情况是，如果服务器在对Clerk重试的RPC回复中返回rpc.ErrVersion。此时，Clerk无法知道其Put是否已被服务器执行：第一个RPC可能已被服务器执行，但网络可能丢弃了服务器的成功回复，导致服务器仅对重传的RPC返回rpc.ErrVersion。或者，可能是另一个Clerk在该Clerk的第一个RPC到达服务器之前更新了键，因此服务器未执行该Clerk的任何RPC，并对两者都回复rpc.ErrVersion。因此，如果Clerk对重传的Put RPC收到rpc.ErrVersion，Clerk.Put必须向应用程序返回rpc.ErrMaybe而非rpc.ErrVersion，因为请求可能已被执行。然后由应用程序处理这种情况。如果服务器对初始（非重传）Put RPC返回rpc.ErrVersion，则Clerk应向应用程序返回rpc.ErrVersion，因为该RPC肯定未被服务器执行。  

如果Put操作能保证恰好一次（即没有rpc.ErrMaybe错误），对应用开发者会更方便，但如果不为每个Clerk在服务器维护状态，这很难保证。在本实验的最后一个练习中，你将使用你的Clerk实现一个锁，探索如何用至多一次的Clerk.Put编程。  

现在你应该修改kvsrv1/client.go，以应对RPC请求和回复丢失的情况。客户端ck.clnt.Call()返回true表示客户端收到了服务器的RPC回复；返回false表示未收到回复（更准确地说，Call()会等待回复消息一段时间，超时未收到则返回false）。你的Clerk应不断重新发送RPC，直到收到回复。记住上述关于rpc.ErrMaybe的讨论。你的解决方案不应要求对服务器进行任何修改。  

添加代码使Clerk在未收到回复时重试。如果你的代码通过kvsrv1/中的所有测试，即完成此任务：  

```bash
$ go test -v  
=== RUN   TestReliablePut  
One client and reliable Put (reliable network)...  
  ... Passed --   0.0  1     5    0  
--- PASS: TestReliablePut (0.00s)  
=== RUN   TestPutConcurrentReliable  
Test: many clients racing to put values to the same key (reliable network)...  
info: linearizability check timed out, assuming history is ok  
  ... Passed --   3.1  1 106647 106647  
--- PASS: TestPutConcurrentReliable (3.09s)  
=== RUN   TestMemPutManyClientsReliable  
Test: memory use many put clients (reliable network)...  
  ... Passed --   8.0  1 100000    0  
--- PASS: TestMemPutManyClientsReliable (14.61s)  
=== RUN   TestUnreliableNet  
One client (unreliable network)...  
  ... Passed --   7.6  1   251  208  
--- PASS: TestUnreliableNet (7.60s)  
PASS  
ok  	6.5840/kvsrv1	25.319s  
```

- 客户端重试前应稍作等待；可以使用Go的time包调用time.Sleep(100 * time.Millisecond)。  

#### 使用键值Clerk和不可靠网络实现锁（简单）  

修改你的锁实现，使其在网络不可靠时与修改后的键值客户端正确工作。当你的代码通过kvsrv1/lock/的所有测试（包括不可靠网络测试）时，即完成此练习：  

```bash
$ cd lock  
$ go test -v  
=== RUN   TestOneClientReliable  
Test: 1 lock clients (reliable network)...  
  ... Passed --   2.0  1   968    0  
--- PASS: TestOneClientReliable (2.01s)  
=== RUN   TestManyClientsReliable  
Test: 10 lock clients (reliable network)...
```
