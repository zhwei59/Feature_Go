# Feature_Go
基于spark的自动特征生成框架
三个算子
维度算子

一次的处理链处理一条数据流：

select need_columns from table 
	uid collect_list()
hive->对象聚合->维度算子->维度聚合->衰减累加->度量算子->统计算子
		group by uid =>uid list<a1,a2,a3> 初步group  step1
		RowData(dimonsion,columns,pojo)
						=>uid map<category list(a1,a2,a3)> 施加维度算子，处理维度 
							=>uid map<category value> 衰减累加
								=>uid v1,v2,v3 度量算子
先扫描一遍 遍历出所有的column
