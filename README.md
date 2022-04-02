## 项目二之b+tree
### 分析
#### b_plus_tree

这个是主要提供接口的类，特点如下：

1、只支持单一key

2、支持insert & remove

3、动态的增长或者减小

4、实现提供 range 扫描的 index 的迭代器

#### 节点类型

首先节点分为两种页面：叶子节点（leaf page）、非叶子节点（internal page）

#### 非叶子节点

header + kv对

其中，header大小为 —— 24byte ，包含：

`PageType(4) | LSN (4) | CurrentSize (4) | MaxSize (4) | ParentPageId (4) | PageId (4)` 

其中LSN是Log sequence number (在项目四中会用到)

k0是不存储数据的，对于v，存储的是孩子节点的page-id

所以，总体来说，就是*K(i) <= K < K(i+1)，* 其中K是V[i]

这么看，v[0] 的所有数据都是小于K[1]的，v1又大于等于k1，画出来就是这样

```cpp
 (k[0]为空)      5(k[1])             10(k[2]) 
        v[0]/             v[1]|               v[2]\
       1,2,3,4            5,6,9               10,11,15
```

所以，对于internal类型的page，可以存储的kv对数量就是

`(PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType))`

#### 叶子节点

叶子节点也是由两部分组成，header和kv对

其中，header大小为28byte，分为：

`PageType(4) | LSN (4) | CurrentSize (4) | MaxSize (4) | ParentPageId (4) | PageId (4) | NextPageId (4)`

k是k，v则是RID，RID也在include/common/rid.h中实现了，RID是page-id + slot-id

RID是一个64位的字节数据

前32位是page_id，后32位是slot_id

所以对于叶子节点，可以存储的kv数量就是

`(PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType)`


### 删除
函数调用关系如下：

![b+树的删除](./assets/b%2Btree_delete.png)
（上图还有一个CoalesceOrRedistribute内的判断根节点没画出，但总体不影响）

对于删除，有三种重要的函数，如下：

#### 1、CoalesceOrRedistribute

`CoalesceOrRedistribute`函数是，删除之后，如果当前节点不满足最小值限制，那么就调用这个函数，

如果当前是根节点，那么就直接调ajustroot即可

如果当前不是根节点，那么就检查左右节点有没有可用的，如果有可用的，那么就直接调用redistrubute，重新分配，如果没有可用的，那么就调用coalsece进行合并

#### 2、Coalesce

`Coalesce`是进行合并的

如果当前节点是叶子节点，那么就与左/右节点进行合并，然后删除父节点对应的middle kv

如果当前节点是internal节点，那么也是进行合并，但是要把右侧节点对应的kv值也进行合并，作为middle key，并且把父节点的middle kv 删除

最后因为父节点删除了一个kv，所以还是需要检查父节点的kv值是否小于minsize，并且决定是否重新调用`CoalesceOrRedistribute`

#### 3、Redistribute

`Redistribute`是进行重新分配的，不涉及删除任何kv与page

如果当前节点是叶子节点，那么就把左侧/右侧的最后一个/第一个kv移动过来，同时也要更新父节点的middle key

如果当前节点是内部节点，那么就把父节点对应的kv移动到自己首部/尾部，然后把左/右的节点的首/尾移动到父节点的对应的位置

删除这部分测试很简单，极其不完备，我也有一部分不太理解，比如在insert的叶子节点为2，内部节点为3的情况下，删除1之后，父节点就只剩下一个指针了，此时父节点是否应该找兄弟节点借kv？