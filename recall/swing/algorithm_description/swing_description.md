# Swing
## Swing算法定义
Swing算法是阿里提出的一种召回算法，Swing算法认为`user - item - user`结构比itemCF单边`item - item`的结构更稳定，形成`user - item - user`的局部图结构关系（秋千）；**对于共同点击商品i，j的用户，如果用户之间共同点击的商品越少则商品i，j就越相似**。商品i，j的Swing得分计算如下：

![swing1](../../../assets/img/swing1.png "Swing初始计算公式")

上式中，Ui表示点击商品i的所有用户；Iu表示用户u点击的所有商品；α是一个平滑因子。
## Swing实例
下图中，A、B、C表示用户；h、t、r、p表示商品；如果用户和商品之间存在交互关系则连接一条边。假设需要计算h、p商品的相似度，共同点击商品h、p的用户集合为：{A,B,C}；

![swing2](../../../assets/img/swing2.png)

![swing3](../../../assets/img/swing3.png)
## Swing权重
在上述Swing计算公式中，没有考虑用户权重信息；可以考虑加入用户权重信息，比如可以对较活跃的用户做一定的惩罚；下面公式中，wu表示用户u的权重，其跟用户点击的商品数量成反比；还可以考虑加入商品权重信息；

![swing4](../../../assets/img/swing4.png)

## Swing计算步骤
1. 首先统计物品侧的共现矩阵，筛选出物品共现次数>=n（自定义阈值）的物品对；
2. 统计每个物品被浏览的用户序列；
3. 对1中的物品对（itemi，itemj）中itemi和itemj分别对应的用户序列做交集（假设交集元素个数为n），并将该交集做两两组合的展开，可以得到四维矩阵如下，本次展开可以得到四维矩阵中的`n(n-1)/2`个点。每个点对应上面所述的一个四边形；

`(itemidI, itemidJ, pairAccess(i), pairAccess(j))`

4. 根据2，构造用户共现矩阵，用户间共现得分作为score；
5. 将3和4中的表格做join，得到3中四维矩阵中每个点（四边形）对应的score；
6. `similar=1/(alpha+score)`，alpha是平滑因子，自己定义；
7. 将(itemi, itemj)名称互换后和前面的表union起来；
8. 对(itemi, itemj)做group by，将similar得分求sum，作为(itemi,itemj)的similar得分；
9. 对每个itemi作为分区，选取和itemi的similar降序排名topn的itemj。这就是itemi按照swing协同过滤思想所得的n个相似item。