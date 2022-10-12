# personal-rank
## 阐述
对用户A进行个性推荐，从用户A结点开始在`用户-物品`二分图random walk，以α的概率从A的出边相等概率选择一条游走过去，到达该顶点后，有α的概率继续从顶点的出边相等概率选择一条继续游走到下一个结点或者(1 - α)的概率回到起点A，多次迭代。直至各顶点对于用户A的重要度收敛。
## 原始公式
![personal-rank](../../../assets/img/personal-rank原始公式.jpg)
## 转换矩阵式
![personal-rank](../../../assets/img/personal-rank矩阵式.jpg)
