# 简要说明

## 注意

在发出请求之前需要进行persist stat，如果状态由变化的话

## 逻辑处理架构

raft维护raft的节点相关状态
raft handler 处理所有事件（外部请求，超时等等），修改raft 状态，返回/发出对应请求
raft handler own raft
raft handler 通过一个chan接受外部事件，同时实现对raft状态修改的串行化
事件处理流程是raft handler 的main函数,raft算法基本都通过这个事件触发的处理函数体现

**重要**
事件处理的流程的框架
根据role和event,派发相应的处理流程
处理流程的通用框架
validate event,修改状态，状态pesister,发送消息给其他节点/回复
可以根据这个框架来写单测,即输入事件，role，状态，检查状态变化，发送的消息

## 状态变化checklist

|election 超时|heartbeat timeout|append|append apply|vote|vote apply|

## 单元测试

### 选举

#### candidates

[x]超时开始选举
[x]选举失败，没有达到半数，继续下一轮
[x]选举失败，其他节点成功
[x]选举成功，发送心跳
[x]选举失败，term 大于当前，更新term

#### following and leader

[x] followe/leader 接受选举
[x] followe/leader 拒绝选举 已经投票/term  
[x] followe/leader 拒绝选举 logs不够新

### append

[x]正常append leader,follower
[x]append 截断 follower
[x]失败，term不符合当前
[x]拒绝，prex不符合 leader,follower

[x]leader 超时心跳发送
[x] append client 请求成功

[x]append reply 成功leader 更新commit,match_index,next_index
[x] 只对当前term的entry进行commit，之前term的entry，即使数量达到要求，也不进行commit

## 定时器

## election timeout 定时器

使用一个线程，sleep到[election_timeout_check] 时间点，发送超时事件给main,这个事件包含timeout check 时间点的数值
handler开始后需要初始化这个定时线程
timeout check时间点的设定：接受到外部有效信息（vote,append)后，说明当前有leader或者candidate存活，更新这个时间点，设置为now+rand(150-300ms)
当接收到timeout 事件，检查事件记录的timeout 时间点的数值是否等于当前的timeout check
如果是，说明没有接收到其他节点发来的有效信息，需要进行vote
如果不是，说明不需要vote，启动一个新的定时线程，重复sleep 到timeout check时间点

## leader heartbeat 定时器

heartbeat 时间，论文没找到，[election_timeout] 最小为150，network的delay最多是27,heartbeat使用20吧,这样在[election_timeout]之前可以发送3次

使用一个线程,每隔20ms 发送[heartbeat_timeout]事件，这个线程一直存在和role无关，follower和candidate忽略heartbeat timeout事件

如果是leader，检查所有follower 上次发送消息的时间，如果超过20ms，发送empty append

## todo

[] append 失败后马上append reply

test_reliable_churn_2c

## 乱序问题

因为有term机制保证，所以只需要考虑同一个term内的vote和append乱序问题

vote是幂等的，重复vote不会有什么其他副作用，follower重复回复就好了

append乱序，仔细看论文figure2，当prev entry match时，比较后续entry，如果发现冲突，用append 请求的entry覆盖掉冲突，并丢弃后续entry
而相同的entry会被保留下来，所以，对于append 乱序其实不会造成任何影响

append reply乱序，可以用append请求的prev entry index对比leader当前记录的next index,如果不match则说明是乱序的reply，直接舍去

## election timer

### 优化append失败，回退多个entry

参考 <https://pdos.csail.mit.edu/6.824/notes/l-raft2.txt>

> rejection from S1 includes:
> XTerm:  term in the conflicting entry (if any)
>XIndex: index of first entry with that term (if any)
>XLen:   log length
>Case 1 (leader doesn't have XTerm):
>nextIndex = XIndex
>Case 2 (leader has XTerm):
>nextIndex = leader's last entry for XTerm
>Case 3 (follower's log is too short):
>nextIndex = XLen

append reply 返回冲突entry的term和冲突term的first entry index（可能不存在）,log len

对于leader，接受到冲突entry的index和term后
term存在，
使用冲突term的最后一个entry作为next index
term不存在,next index=first entry index
follower log太小，没有冲突的entry，next index=log len

### 2d snapshot

两个部分

### compact log

由测试代码触发
config->node snapshot->send snapshot event->raft hanldle snapshot
丢弃指定index 范围的entry,更新snapshot

### install snapshot

leader在处理append时，发现entry已经被归入snapshot，发起install rpc
leader send rpc
follower node server,send snapshot event to raft,wait res(use a ch)
raft send msg to reply
reply handle(in config) call node cond_install_snapshot
node send event to raft
raft do install,return res to node by ch
node recv res, set res to node server,return true

#### log 抽象改造

因为snapshot的存在，log不能再从0开始，而是变为一段连续的，可以被从开头截断的序列，这里需要进行一定程度的封装，而不是作为一个vec来使用
实现变为vec+offset，offset是snapshot的lastindex+1,get_index操作变为vec.get(index-offset)

### lab 3

raft实现了多点顺序写入，但是如果要将其作为kv，实现 exactly 写入语义 还有其他问题解决

1. 如何获知写入成功
2. 写入失败，如何获知
3. 写入成功，但是apply到statemachine丢失
正常情况下，raft会把commit的entry apply到state machine，但是不保证所有commit的entry一定apply
解决方法是重试加去重
对于client，一直重试直到成功返回
对于server，进行去重，client为每个请求增加一个唯一id,state machine记录所有已经写入的id，并进行去重
记录所有id开销比较大，这里使用另外一种方法，只允许id以**严格**自增方式写入state machine，即[n，n+1,n+2...],其他不符合的id，state machine会拒绝，并返回当前id,state machine记录所有client的最新id即可
对于小于当前写入state machine 的id的请求，可以写入raft log，但是在apply 到state machiene时会被拒绝，同时返回当前的最大id，client进行id更新后再重试

4. 如何读到最新数据
leader返回读请求之前，需要和多数节点完成一次心跳,从而保证在读取这个时间点之后，仍然是leader，实现线性一致性
当leader选举成功时，并不清楚当前哪些log已经达到多数节点可以commit，所以需要进行一次空的append，append commit成功后，之前所有log都可以安全commit

架构
client - (rpc）- > node ->kv server--(append)->raft-(apply)->kv server--(channel)->node->client

client:客户端相关，提供put get接口，持有多个node 连接
node:服务端代码，实现了服务端相关接口，本身实现很简单，通过channel向对应的node发送相关事件
kv server：state machine +raft,实现核心的存储功能，将请求append到raft上，当raft完成commit，将请求reply回state machine，完成数据的写入

### snaphsot

包括两个部分，
snapshot 生成 server检查raft占用大小，超过指定大小后，要求raft执行compact，将log合并为一个snapshot，合并方法由kv server提供
install snapshot，leader发现append 的prev不存在，通过install snapshot请求将snapshot发送给follower（已经实现），follower apply snapshot后，
kv server需要将statemachine替换为snapshot的数据
snapshot数据对raft不可见，对kv server来说就是statemachine的所有数据
