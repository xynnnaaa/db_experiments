import re
import random


def get_new_sample(file_path, total_num, output_sql_file):
    """
    从包含符合查询条件的sid的文本文件中按比例随机选择部分sid, 并生成相应的 SQL 语句。
    
    :param file_path: 包含符合查询条件tuple的sid的文本文件路径。
    :param total_num: 表的基数
    :param output_sql_file: 输出 SQL 语句的文件路径。
    """
    # 使用 set 存储满足条件的sid
    unique_numbers = set()

    # 读取文件并填充唯一数字集合
    with open(file_path, 'r') as file:
        for line in file:
            try:
                number = int(line.strip())
                unique_numbers.add(number)
            except ValueError:
                print(f"Warning: '{line.strip()}' is not a valid integer.")

    # 计算需要选取的数字数量
    cnt = (len(unique_numbers) * 1000) // total_num
    cnt_left = 1000 - cnt

    if cnt < 0:
        cnt = 0
    if cnt_left < 0:
        cnt_left = 0

    # 从 unique_numbers 中随机选择 cnt 个数
    selected_from_unique = random.sample(unique_numbers, min(cnt, len(unique_numbers)))

    # 创建一个集合，包含从 1 到 total_num 的所有数字，然后从中移除 unique_numbers 中的数字
    all_numbers = set(range(1, total_num + 1))
    remaining_numbers = all_numbers - unique_numbers

    # 从 remaining_numbers 中随机选择 cnt_left 个数
    selected_from_remaining = random.sample(remaining_numbers, min(cnt_left, len(remaining_numbers)))

    # 合并两个列表并排序
    selected_sids = sorted(selected_from_unique + selected_from_remaining)

    # 生成 SQL 语句并写入 .sql 文件
    with open(output_sql_file, 'w') as sql_file:
        for sid in selected_sids:
            sql_statement = f"INSERT INTO badges_sample1 SELECT * FROM badges WHERE sid = {sid};\n"
            sql_file.write(sql_statement)

    print("SQL statements have been written to", output_sql_file)
    return cnt, cnt_left

file_path = 'badges.txt'
output_sql_file = 'badges_insert.sql'
total_num = 79851
cnt, cnt_left = get_new_sample(file_path, total_num, output_sql_file)
print(cnt, cnt_left)