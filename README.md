# rfbp使用手册  
## 使用场景  
需要对数据进行处理，由于数据量巨大需要使用特殊工具（如Spark，Mapreduce）来实现分布式处理的情况。
## 工具特点

 - 简单：普遍情况下，无需对逻辑代码进行任何修改即可实现分布式计算化。此外，也无需任何特殊的环境配置，只要有可以通过ip互联的机器即可运行。
 - 高效：相比于传统的分片计算，rfbp实现了动态分配任务，从而避免了出现任务在进程间分布不均的问题。
 - 可扩展：在任务执行过程中，可以任意添加和退出工作进程，且不影响执行流程。
 - 可恢复：当任务异常终止时，重新启动时可以从上次退出位置继续执行，而不需要从头开始。
 - 易调试：可以在分布式执行和单进程执行间进行无缝切换，从而方便代码调试。
### 入门使用方式
#### 无后处理的情况
在需要对数据进行for循环时，可以直接使用for-loop完成任务。
代码示例：
```python
# main.py
from rfbp.rfbp_tools import rfbp
data = [i for i in range(100)] # 将处理的data，如图像地址。可以是list或iterable

# 逻辑代码主体：像单进程一样处理数据
# (ip, port)：表示server进程的所在地址，缺省时默认为本机端口55553，此时只能在本机的进程互连
# ckpt: server使用该filepath来保存任务进度，并使用它来从上次执行位置恢复
# serial: 表示是否是单进程串行执行，可以设置为True来方便进行代码的逻辑测试，方便调试。缺省值为False
for i in rfbp(data, ip='127.0.0.1', port=65530, ckpt='ckpt', serial=False):
	print(i)
```
ip可以设置为任意一个节点的ip，之后可以在任意节点中执行```python main.py```来开启任意数量的工作进程处理数据。

在执行过程中，rfbp会在(ip, port)上开启一个server，来给工作进程分发任务并通过ckpt维护任务进度。
可以通过```python3 rfbp/cat.py ckpt.ckpt```来观察任务执行情况。

PS: 当程序正常执行时，第一个在指定的ip上启动的进程会输出 **server start** 字样，其它进程会输出 **server already started** 字样。如果没有看到**server start**且程序长时间停滞，可以更换port试一试。

#### 有后处理的情况
当我们需要对分布式计算完成的数据进行处理时（比如整合计算得到的图像来生成视频时），rfbp也提供了简单的接口。
示例代码：
```python
# main.py
from rfbp.rfbp_tools import rfbp
data = [i for i in range(100)] # 将处理的data，如图像地址。可以是list或iterable

data = rfbp(data, ip='127.0.0.1', port=65530, ckpt='ckpt', serial=False) # 显示指定迭代器
for i in data:
	print(i)
	data.produce(i)  # 该函数会将输入值保存到server
result = data.product()  # 从server取回所有的输出值
print(result)
```
```data.product()```只会在一个进程处返回有效值（即产生了server的那个进程），其它进程则会返回None。这种设计可以保证只有一次后处理，且可以实现连续的for-loop处理。

```product```方法返回的列表默认是无序的，可以通过在data出显式指定次序来实现有序性（即```data = enumerate(data)```。

PS: 为了保证程序正确性和执行效率，当使用了ckpt时，对于此前已执行过的数据，```product```方法将不会返回值（因为for-loop本身也不会重复处理该数据）。

PPS：所有通过```produce```发送的数据都需要是可序列化的。

#### 在Server端执行任务
因为client可能是跑在不同的机器上的，不能直接执行后处理逻辑，因此rfbp提供了接口使得Client可以在Server端执行程序。
这里以client读取并处理数据，并集中保存到Server所在节点的文件系统中为例子。
示例代码：

```python
# main.py
import time
from rfbp.rfbp_tools import rfbp
import json

data = [i for i in range(100)]


# 在Server端执行的任务
def save(i):
    path = f"{i}.json"
    print(f"server saving to {path}")
    json.dump(i, open(path, "w"))
    return "suc"


data = rfbp(data, ip='127.0.0.1', port=65533, serial=False)
for i in data:
    time.sleep(1)
    ret = data.execute(save, i)
    print(ret)
```
执行结束后，会看到只有在ip参数所对应的节点的文件系统中会保存 [0-100].json 这100个文件。
此外，如果输入数据都保存在Server节点中，也可以通过该方式来从Server端读取需要的数据。

#### 函数映射
除了显式的for-loop，rfbp也提供了整合式的mapping方案来实现数据处理。
示例代码：
```python
# main.py
from rfbp.rfbp_tools import mapping

def func(i):
	return i + 1

data = [i for i in range(100)]
data = mapping(data, func, ip='127.0.0.1', port=65530)
data = mapping(data, func, ip='127.0.0.1', port=65531)
print(data)
```
此时输出值为```[2,3,..,102]```。

### 进阶使用方式

#### 任务的拓扑序执行
在执行任务时，有时任务间会存在相互依赖（比如分段生成视频时，每一段中的帧互相依赖），此时可以通过输入依赖图参数来实现拓扑序执行。
这里以第[0-30], [30-60]两个片段为例子.
示例代码：
```python
# main.py
from rfbp.rfbp_tools import rfbp

data = [i for i in range(60)]
# 设置依赖图
dependencies = [(i, i+1) for i in range(30-1)]  # [0-30]间的依赖图，每个tuple表示tuple[1]执行的前驱是tuple[0]。每个任务可以有多个前驱任务。
dependencies += [(i, i+1) for i in range(30, 60-1)]  # [30-60]间的依赖图

data = rfbp(data, dependencies=dependencies, ip='127.0.0.1', port=65532)
for i in data:
    data.produce(i)
print(data.product())
```
输出结果中可以看到任务会是类似```[0,30, 1, 31,..., 29, 59]```这样交替执行的序列，从而在保证拓扑序的前提下也最大程度提高并行度。

#### Server-Client 显式配置
TODO:
