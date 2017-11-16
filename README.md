# Feature_Go
基于spark的自动特征生成框架
## 配置文件
支持toml风格的配置文件。

```
[Group]
    db="wkshop"
    table="user_click_order"
    column=["userid"]
[Range]
    column="data_dt"
    end="2017-11-01"
    factor=1.0
[[Calculator]]
    dimension="category"
    [[Calculator.task]]
        column="click"
        name="category_click_count"
        stat="sum"
    [[Calculator.task]]
           column="click"
           name="category_click_rate"
           stat="rate"
    [[Calculator.task]]
           column="click"
           name="category_click_per"
           stat="percentile"
```

## 功能描述
将特征生成的过程进行抽象和分解成下面的过程：
	hive->对象聚合->维度算子->维度聚合->衰减累加->度量算子->统计算子

主要包含三个算子的开发。

- 维度算子用来处理维度相关的特征工程，比如连续特征离散化。
- 度量算子进行原始字段向输出特征转换，计算衰减累加后的结果
- 统计算子主要有以下五个实现：
	+ 累加值
	+ 平均值
	+ 分布
	+ 拼接
	+ 分位点

## 效果

