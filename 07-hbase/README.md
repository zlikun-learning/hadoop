#### 准备两个表，测试数据迁移过程
```
# 准备两个表，初始化数据，测试表与表间数据迁移
> create 't1', 'info'
> create 't2', 'info'

> put 't1', 't0001', 'info:name', 'A'
> put 't1', 't0001', 'info:age', '18'
> put 't1', 't0002', 'info:name', 'B'
> put 't1', 't0002', 'info:age', '21'
```