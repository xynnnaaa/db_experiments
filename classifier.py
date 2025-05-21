"""

用训练查询的采样向量进行聚类, 训练分类器, 对测试查询进行分类

"""

import numpy as np
import csv
import os
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.decomposition import PCA
from sklearn.decomposition import FactorAnalysis
from sklearn.neural_network import MLPClassifier
from util import *


def perform_kmeans_clustering(n_clusters):
    """对采样向量聚类"""
    
    table_name = ["users", "posts", "postLinks", "postHistory", "comments", "votes", "badges", "tags"]
    
    vector_file = 'vec.txt'
    
    vectors = np.genfromtxt(vector_file, delimiter=',')
    
    # PCA降维
    pca = PCA(n_components=0.95)  # 保留 95% 的方差
    vectors_reduced = pca.fit_transform(vectors)
    
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    
    cluster_labels = kmeans.fit_predict(vectors_reduced)
    
    # cnt
    unique_labels, counts = np.unique(cluster_labels, return_counts=True)
    cluster_vectors = {label: [] for label in unique_labels}
    for label, vec in zip(cluster_labels, vectors):
        cluster_vectors[label].append(vec)
        
        
    output_file_name = "cluster_labels.txt"
    with open(output_file_name, 'w') as file:
        for prediction in cluster_labels:
            file.write(str(prediction) + '\n')

                        
    # 计算每个聚类的分段和，并找到每段的最大值
    segment_size = 100
    num_segments = 8  
    output_file_name = "cluster_sums.txt"
    with open(output_file_name, 'w') as file:
        for label, count in zip(unique_labels, counts):
            file.write(f"Cluster {label}: {count} vectors\n")
        file.write('\n')
        
        for label, vecs in cluster_vectors.items():
            file.write(f"Cluster {label} segment sums:\n")
            cluster_array = np.array(vecs)
            segment_sums = np.zeros((num_segments, segment_size))
            for i in range(num_segments):
                start = i * segment_size
                end = start + segment_size
                segment_sums[i] = np.sum(cluster_array[:, start:end], axis=0)
            
            
            for i, segment_sum in enumerate(segment_sums):
                max_index = np.argmax(segment_sum)
                max_value = segment_sum[max_index]
                file.write(f"{table_name[i]} ({i * segment_size + 1}-{i * segment_size + 100}): Max value at index {max_index}: {max_value}\n")
            file.write("\n")
    
    return cluster_labels



def load_data(file_name):
    joins = []
    predicates = []
    tables = []

    # Load queries
    with open(file_name + ".csv", 'r') as f:
        data_raw = list(list(rec) for rec in csv.reader(f, delimiter='#'))
        for row in data_raw:
            tables.append(row[0].split(','))
            joins.append(row[1].split(','))
            predicates.append(row[2].split(','))

    # Split predicates
    predicates = [list(chunks(d, 3)) for d in predicates]

    return joins, predicates, tables

def load_and_encode_data(train_file, test_file):
    file_name_column_min_max_vals = "column_min_max_vals.csv"
    
    joins, predicates, tables= load_data(train_file)
    joins_test, predicates_test, tables_test = load_data(test_file)

    # Get column name dict
    column_names = get_all_column_names(predicates)
    column2vec, idx2column = get_set_encoding(column_names)

    # Get table name dict
    table_names = get_all_table_names(tables)
    table2vec, idx2table = get_set_encoding(table_names)

    # Get operator name dict
    operators = get_all_operators(predicates)
    op2vec, idx2op = get_set_encoding(operators)

    # Get join name dict
    join_set = get_all_joins(joins)
    join2vec, idx2join = get_set_encoding(join_set)

    # Get min and max values for each column
    with open(file_name_column_min_max_vals, 'r') as f:
        data_raw = list(list(rec) for rec in csv.reader(f, delimiter=','))
        column_min_max_vals = {}
        for i, row in enumerate(data_raw):
            if i == 0:
                continue
            column_min_max_vals[row[0]] = [float(row[1]), float(row[2])]
    
    # Get feature encoding      
    predicates_enc, joins_enc = encode_data(predicates, joins, column_min_max_vals, column2vec, op2vec, join2vec)
    predicates_enc_test, joins_enc_test = encode_data(predicates_test, joins_test, column_min_max_vals, column2vec, op2vec, join2vec)
    
    len_per_predicate = len(predicates_enc[0][0])
    len_per_join = len(joins_enc[0][0])
    
    # add padding
    max_predicates = max(max([len(p) for p in predicates_enc]), max([len(p) for p in predicates_enc_test]))
    max_joins = max(max([len(j) for j in joins_enc]), max([len(j) for j in joins_enc_test]))
    
    print(f"Max predicates: {max_predicates}")
    print(f"Max joins: {max_joins}")
    print(f"len_per_predicate: {len_per_predicate}")
    print(f"len_per_join: {len_per_join}")
    
    # max_joins = max([len(j) for j in joins_enc])
    for i in range(len(predicates_enc)):
        for j in range(max_predicates - len(predicates_enc[i])):
            predicates_enc[i].append(np.zeros(len_per_predicate))
        # for j in range(max_joins - len(joins_enc[i])):
        #     joins_enc[i].append(np.zeros(len(joins_enc[0][0])))
        predicates_enc[i] = np.hstack(predicates_enc[i])
        
    for i in range(len(predicates_enc_test)):
        for j in range(max_predicates - len(predicates_enc_test[i])):
            predicates_enc_test[i].append(np.zeros(len_per_predicate))
        # for j in range(max_joins - len(joins_enc_test[i])):
        #     joins_enc_test[i].append(np.zeros(len(joins_enc_test[0][0]))
        predicates_enc_test[i] = np.hstack(predicates_enc_test[i])
    
    for i in range(len(joins_enc)):
        for j in range(max_joins - len(joins_enc[i])):
            joins_enc[i].append(np.zeros(len_per_join))
        joins_enc[i] = np.hstack(joins_enc[i])
        
    for i in range(len(joins_enc_test)):
        for j in range(max_joins - len(joins_enc_test[i])):
            joins_enc_test[i].append(np.zeros(len_per_join))
        joins_enc_test[i] = np.hstack(joins_enc_test[i])
        
    # Concatenate predicates and joins for training and testing data
    train_features = np.concatenate((joins_enc, predicates_enc), axis=1)
    test_features = np.concatenate((joins_enc_test, predicates_enc_test), axis=1)
    
    # print(f"Train features shape: {train_features.shape}")
    # print(f"Test features shape: {test_features.shape}")
    
    return train_features, test_features

def train_classifier():
    train_file = "train"
    test_file = "test"
    X, X_test = load_and_encode_data(train_file, test_file)
    X = np.array(X)
    
    n_clusters = 8
    y = perform_kmeans_clustering(n_clusters)
    
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.1, random_state=42)
    
    clf = MLPClassifier(hidden_layer_sizes=(512, 256), max_iter=1000, random_state=42)
    
    clf.fit(X_train, y_train)
    
    y_pred = clf.predict(X_val)
    
    accuracy = accuracy_score(y_val, y_pred)
    print(f"rf_clf Accuracy: {accuracy}")
    
    
    X_test = np.array(X_test)
    y_test = clf.predict(X_test)
    
    output_file_name = "predictions.txt"

    with open(output_file_name, 'w') as file:
        for prediction in y_test:
            file.write(str(prediction) + '\n')

    print(f"Predictions have been written to {output_file_name}")
            

def classify_csv(input_csv, predictions_file, output_dir, num_classes=8):
    """
    根据对测试集的分类结果，将测试集中的查询分到不同的文件中
    """

    os.makedirs(output_dir, exist_ok=True)

    with open(input_csv, "r", encoding="utf-8") as csv_file, \
         open(predictions_file, "r", encoding="utf-8") as pred_file:

        output_files = [open(os.path.join(output_dir, f"test_{i}.csv"), "w", encoding="utf-8") for i in range(num_classes)]

        for row, pred_line in zip(csv_file, pred_file):
            category = int(pred_line.strip())

            if 0 <= category < num_classes:
                output_files[category].write(row)
            else:
                print(f"Invalid category: {category}. Skipping this row.")

        for file in output_files:
            file.close()

    print(f"分类完成，文件已生成在 '{output_dir}' 目录中。")
    
    
train_classifier()    

classify_csv("test.csv", "predictions.txt", "test")

    
