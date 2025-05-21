# db_experiment
### 1. 生成采样向量
`gen_sample_vec.py` 脚本对训练查询生成采样向量，并将结果保存在 `vec.txt` 文件中。

### 2. 聚类和分类
`classifier.py` 脚本对采样向量进行聚类，并将不同类别的最优样本保存到 `cluster_sum.txt` 文件中。训练分类器，对测试查询进行分类。

### 3. 生成 bitmap SQL 语句
`gen_bitmap_sql.py` 脚本生成用于得到 bitmap 的SQL语句。

### 4. 生成bitmap
执行步骤3得到的SQL语句，`gen_bitmap.cpp`将结果生成bitmap，作为mscn的输入。