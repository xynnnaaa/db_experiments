import csv
import random
import json
import os

def load_table_info(table_info_file):
    """加载表的结构信息"""
    with open(table_info_file, 'r') as f:
        return json.load(f)

def load_col_min_max(col_min_max_file):
    """加载列的最小值和最大值"""
    col_min_max = {}
    with open(col_min_max_file, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            col_full_name = row[0]  # 格式：table.column
            min_val = int(row[1])
            max_val = int(row[2])
            col_min_max[col_full_name] = (min_val, max_val)
    return col_min_max

def generate_predicates(table, table_info, col_min_max):
    """为单个表生成随机谓词条件"""
    predicates = []
    non_key_cols = table_info.get(table, {}).get('non_key_columns', [])
    
    # 设置每个表的最大谓词数量为3
    max_predicates = 3
    if len(non_key_cols) < max_predicates:
        max_predicates = len(non_key_cols)
    num_predicates = random.randint(0, max_predicates)
    if num_predicates == 0:
        return predicates
    
    # 随机选择列
    selected_cols = random.sample(non_key_cols, num_predicates)
    
    for col in selected_cols:
        col_key = col
        if col_key not in col_min_max:
            print(f"warning: column {col_key} not found in min/max values.")
            continue
        
        min_val, max_val = col_min_max[col_key]
        is_date_col = 'date' in col.lower()
        
        # 随机选择操作符
        if is_date_col:
            op = random.choice(['<', '>', 'BETWEEN'])
        else:
            op = random.choice(['=', '<', '>'])
        
        # 随机选择谓词值
        if op == 'BETWEEN':
            val1 = random.randint(min_val, max_val)
            val2 = random.randint(min_val, max_val)
            if val1 > val2:
                val1, val2 = val2, val1
            predicate = f"{col},>,{val1},{col},<,{val2}"
        else:
            value = random.randint(min_val, max_val)
            predicate = f"{col},{op},{value}"
        
        predicates.append(predicate)
    
    return predicates

def process_template(row, tables, table_info, col_min_max, outfile, queries_per_template):
    """处理单个连接模板"""
    for _ in range(queries_per_template):
        outfile.write("#".join(row))
        outfile.write("#")
        is_first = True
        
        # 为每个表生成谓词
        for table in tables:
            predicates = generate_predicates(table, table_info, col_min_max)
            if len(predicates) > 0:
                if is_first:
                    is_first = False
                else:
                    outfile.write(",")
                outfile.write(",".join(predicates))
                
        outfile.write("#\n")

def generate_train_workload(table_info_file, col_min_max_file, join_template_file, output_file, queries_per_template):
    """生成训练工作负载"""
    table_info = load_table_info(table_info_file)
    col_min_max = load_col_min_max(col_min_max_file)
    
    with open(join_template_file, 'r') as infile, \
         open(output_file, 'w', newline='') as outfile:
        
        data_raw = list(list(rec) for rec in csv.reader(infile, delimiter='#'))
        for row in data_raw:    # 每一个模板
            table_list = row[0].split(',')
            process_template(row, table_list, table_info, col_min_max, outfile, queries_per_template)

def main():
    TABLE_INFO_FILE = 'tables.json'
    COL_MIN_MAX_FILE = 'column_min_max_vals.csv'
    JOIN_TEMPLATE_FILE = 'join_template.csv'
    OUTPUT_FILE = 'train.csv'
    QUERIES_PER_TEMPLATE = 200
    
    generate_train_workload(TABLE_INFO_FILE, COL_MIN_MAX_FILE, JOIN_TEMPLATE_FILE, OUTPUT_FILE, QUERIES_PER_TEMPLATE)

if __name__ == "__main__":
    main()