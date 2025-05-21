"""
    
给所有训练查询生成用于聚类的采样向量, 维度为8*100=800

"""

import csv
import re
from datetime import datetime
import time

def timestamp_to_string(timestamp_str):
    """时间戳转换为字符串"""
    unix_timestamp = int(timestamp_str)
    dt = datetime.fromtimestamp(unix_timestamp)
    timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return timestamp_str

def check_table(file_name):
    """检查每个查询中有哪些表, 用于生成采样向量"""
    tables = []
    
    table_name = ["users u", "posts p", "postLinks pl", "postHistory ph", "comments c", "votes v", "badges b", "tags t"]
    table_alias = ["u", "p", "pl", "ph", "c", "v", "b", "t"]
    table_have = [0, 0, 0, 0, 0, 0, 0, 0]
    table_alias_dic = dict(zip(table_name, table_alias))
    table_have_dic = dict(zip(table_name, table_have))
    
    with open(file_name + "_table.txt", 'w') as output_file:
        with open(file_name + ".csv", 'r') as f:
            data_raw = list(list(rec) for rec in csv.reader(f, delimiter='#'))
            for row in data_raw:
                table_list = row[0].split(',')
                tables.append(table_list)
                
                for table in table_list:
                    table_have_dic[table] = 1
                
                for table in table_name:
                    if table_have_dic[table] == 0:
                        output_file.write("0" + "\n")
                    if table_have_dic[table] == 1:
                        output_file.write("1" + "\n")
                    table_have_dic[table] = 0
                output_file.write("\n")
    return tables


def gen_sql(file_name):
    """生成100次采样的sql语句, 之后用postgres执行, 结果存入sample_vec.txt"""
    
    joins = []
    predicates = []
    tables = []
    
    table_name = ["users u", "posts p", "postLinks pl", "postHistory ph", "comments c", "votes v", "badges b", "tags t"]
    table_alias = ["u", "p", "pl", "ph", "c", "v", "b", "t"]
    table_have = [0, 0, 0, 0, 0, 0, 0, 0]
    table_alias_dic = dict(zip(table_name, table_alias))
    table_have_dic = dict(zip(table_name, table_have))
    
    with open(file_name + "_sample_vec.sql", 'w') as output_file:
        with open(file_name + ".csv", 'r') as f:
            data_raw = list(list(rec) for rec in csv.reader(f, delimiter='#'))
            for row in data_raw:
                table_list = row[0].split(',')
                tables.append(table_list)
                
                join_list = row[1].split(',')
                joins.append(join_list)
                
                predicate_list = row[2].split(',')
                predicates.append(predicate_list)
                
                for table in table_list:
                    table_have_dic[table] = 1
                
                for table in table_name:
                    if table_have_dic[table] == 1:  # 此查询中有这个表
                        is_first = True
                        parts = table.split()
                        
                        outputstr = ""
                        
                        for j in range(0, len(predicate_list), 3):
                            if(predicate_list[0] == ''):
                                break
                            if(parts[1] + "." in predicate_list[j]):
                                if is_first:
                                    outputstr += " WHERE "
                                    if("Date" in predicate_list[j]):
                                        outputstr += predicate_list[j] + predicate_list[j+1] + "'" + timestamp_to_string(predicate_list[j+2]) + "'" + "::timestamp"
                                    else:
                                        outputstr += predicate_list[j] + predicate_list[j+1] + predicate_list[j+2]
                                    is_first = False
                                else:
                                    outputstr += " AND "
                                    if("Date" in predicate_list[j]):
                                        outputstr += predicate_list[j] + predicate_list[j+1] + "'" + timestamp_to_string(predicate_list[j+2]) + "'" + "::timestamp"
                                    else:
                                        outputstr += predicate_list[j] + predicate_list[j+1] + predicate_list[j+2]
                        outputstr += ");"
                        
                        for cnt in range(0, 100):
                            output_file.write("SELECT EXISTS ( SELECT 1 FROM " + parts[0] + "_"+ str(cnt) + " " + parts[1])
                            output_file.write(outputstr + "\n")
                        
                    table_have_dic[table] = 0
                output_file.write("\n")
    return joins, predicates, tables

def gen_sample_vec(file_name):
    """基于sql语句的执行结果生成采样向量, 存到vec.txt, 作为后续聚类的输入"""
    cnt = 0
    with open("vec.txt", 'w') as output_file:
        with open(file_name + ".txt", 'r') as f:
            with open("train_table.txt", "r") as table_file:
                num = 0
                for line in table_file:
                    line = line.strip()
                    if line == "0":
                        # 查询中没有这个表就全标记1
                        num += 1
                        output_file.write("1" + ",1" * 99)
                        if num < 8:
                            output_file.write(",")
                    elif line == "1":
                        num += 1
                        # 查询中有这个表，读入100个样本上的采样结果
                        for _ in range(100):
                            data_line = f.readline().strip()
                            cnt += 1
                            if data_line == "t":
                                output_file.write("1")
                            elif data_line == "f":
                                output_file.write("0")
                            else:
                                raise ValueError("Unexpected value in file: " + data_line)
                            if _ < 99:
                                output_file.write(",")
                        if num < 8:
                            output_file.write(",")
                    elif line == "":
                        # 一个查询结束
                        num = 0
                        output_file.write("\n")
                    else:
                        raise ValueError("Unexpected value in table_file: " + line)
    print(cnt)
    return
    

def main():
    check_table("train")
    gen_sql("train")
    # gen_sample_vec("sample_vec")
    
if __name__ == "__main__":
    main()
