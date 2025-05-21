"""

得到分类结果之后，给每一个训练/测试查询生成单表采样的sql语句, 之后用postgres执行, 执行结果用于生成bitmap

用于生成整合的bitmap，每个查询根据分到类别的不同，选择不同的样本

"""

import csv
import re
from datetime import datetime
import time

def timestamp_to_string(timestamp_str):
    unix_timestamp = int(timestamp_str)
    dt = datetime.fromtimestamp(unix_timestamp)
    timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return timestamp_str

def gen_sample_sql_union(file_name, sample_vec, cluster_labels, table_name_index):
    
    joins = []
    predicates = []
    tables = []
    samples = []
    label = []
    num = []
    quit = []
    query_cnt = 0

    # Load queries
    with open("test/test_sample_union.sql", 'w') as sql_file:
        with open(file_name + ".csv", 'r') as f:
            data_raw = list(list(rec) for rec in csv.reader(f, delimiter='#'))
            for row in data_raw:
                current_label = cluster_labels[query_cnt]
                current_sample = sample_vec[current_label]
                query_cnt += 1
                
                if int(row[3]) < 1: # 跳过基数为0的查询
                    exit(1)
                label.append(row[3])
                
                table_list = row[0].split(',')
                num.append(len(table_list)) # 记录每个查询的表数
                tables.append(table_list)
                
                join_list = row[1].split(',')
                joins.append(join_list)
                
                predicate_list = row[2].split(',')
                predicates.append(predicate_list)
                
                for table in table_list:
                    parts = table.split()
                    
                    table_index = table_name_index[table]
                    sample_index = current_sample[table_index]
                    
                    sql_file.write("SELECT " + parts[1] + ".sid FROM " + parts[0] + "_" + str(sample_index) + " " + parts[1])

                    is_first = True
                    for j in range(0, len(predicate_list), 3):
                        if(predicate_list[0] == ''):
                            break
                        if(parts[1] + "." in predicate_list[j]):
                            if is_first:
                                sql_file.write(" WHERE ")
                                if("Date" in predicate_list[j]):
                                    sql_file.write(predicate_list[j] + predicate_list[j+1] + "'" + timestamp_to_string(predicate_list[j+2]) + "'" + "::timestamp")

                                else:
                                    sql_file.write(predicate_list[j] + predicate_list[j+1] + predicate_list[j+2])
                                is_first = False
                            else:
                                sql_file.write(" AND ")
                                if("Date" in predicate_list[j]):
                                    sql_file.write(predicate_list[j] + predicate_list[j+1] + "'" + timestamp_to_string(predicate_list[j+2]) + "'" + "::timestamp")

                                else:
                                    sql_file.write(predicate_list[j] + predicate_list[j+1] + predicate_list[j+2])
                    sql_file.write(";\n")
                sql_file.write("\n")
                
            
    return joins, predicates, tables, samples, label

def main():
    sample_groups = [
        [0, 7, 0, 0, 87, 6, 0, 0],
        [46, 75, 21, 99, 18, 6, 42, 0],
        [0, 5, 0, 27, 74, 37, 2, 0],
        [99, 75, 0, 25, 22, 6, 0, 0],
        [99, 15, 0, 4, 24, 76, 0, 0],
        [0, 57, 0, 21, 55, 12, 0, 0],
        [0, 5, 0, 27, 39, 0, 0, 0],
        [1, 2, 0, 1, 0, 0, 0, 0]
    ]
    tables = ["users u", "posts p", "postLinks pl", "postHistory ph", "comments c", "votes v", "badges b", "tags t"]
    # table_index = [0, 1, 2, 3, 4, 5, 6, 7]
    # table_name_index = dict(zip(table_name, table_index))
    table_name_index = {name: idx for idx, name in enumerate(tables)}
     
    # 读取分类标签
    with open("predictions.txt", "r") as f:
        cluster_labels = [int(line.strip()) for line in f]
        
    # 生成样本SQL
    gen_sample_sql_union(
        file_name="test",
        sample_vec=sample_groups,
        cluster_labels=cluster_labels,
        table_name_index=table_name_index
    )
    
if __name__ == "__main__":
    main()
