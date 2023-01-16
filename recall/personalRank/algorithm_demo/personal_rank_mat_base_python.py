# -*- encoding: utf-8 -*-
"""
@author: Hanjingwei
@date: 2022/10/13 10:26
@brief: personal-rank矩阵式基于python实验
"""
import numpy as np
from scipy.sparse import coo_matrix


class PRMatBasePython(object):
    def __init__(self):
        self.train_file = "../../../data/ml-100k/ua.base"
        self.test_file = "../../../data/ml-100k/ua.test"
        self.personal_rank_save_file = "../../../model/personal_rank/pr_user_item_recommend_dict_mat_by_python.json"
        self.alpha = 0.8
        self.iter_num = 100
        self.topN = 100

    def read_data(self):
        train_data = pd.read_csv(self.train_file, sep='\t', engine='python',
                                 names=['user_id', 'movie_id', 'rate', 'event_timestamp'])
        test_data = pd.read_csv(self.test_file, sep='\t', engine='python',
                                names=['user_id', 'movie_id', 'rate', 'event_timestamp'])
        print('=' * 20, '训练数据集', '=' * 20)
        print(train_data.head(5))
        print('=' * 20, '测试数据集', '=' * 20)
        print(test_data.head(5))
        return train_data, test_data

    def get_personal_rank_graph(self, train_data):
        """
        :param train_data: personal_rank训练数据
        :return: 构造personal_rank"用户-物品"二分图
        """
        personal_rank_graph = dict()
        train_user_set = set()
        for index, row in train_data.iterrows():
            user_id = int(row['user_id'])
            item_id = int(row['movie_id'])
            personal_rank_graph.setdefault(user_id, dict())
            personal_rank_graph[user_id][item_id] = 1
            personal_rank_graph.setdefault(item_id, dict())
            personal_rank_graph[item_id][user_id] = 1
            train_user_set.add(user_id)
        return personal_rank_graph, train_user_set

    def graph_to_mat(self, personal_rank_graph):
        vertex = list(personal_rank_graph.keys())
        address_dict = {}
        total_len = len(vertex)
        for index in range(len(vertex)):
            address_dict[vertex[index]] = index
        row = []
        col = []
        data = []
        for element_i in personal_rank_graph:
            weight = round(1 / len(personal_rank_graph[element_i]), 3)
            row_index = address_dict[element_i]
            for element_j in personal_rank_graph[element_i]:
                col_index = address_dict[element_j]
                row.append(row_index)
                col.append(col_index)
                data.append(weight)
        row = np.array(row)
        col = np.array(col)
        data = np.array(data)
        m = coo_matrix((data, (row, col)), shape=(total_len, total_len))
        return m, vertex, address_dict

    def mat_all_point(self, m, vertex):
        """
            get E-alpha*m.T
        """
        total_len = len(vertex)
        row = []
        col = []
        data = []
        for index in range(total_len):
            row.append(index)
            col.append(index)
            data.append(1)
        row = np.array(row)
        col = np.array(col)
        data = np.array(data)
        eye_t = coo_matrix((data, (row, col)), shape=(total_len, total_len))
        return eye_t.tocsr() - self.alpha * m.tocsr().transpose()

    def personal_rank_mat(self, personal_rank_graph, train_user_set):
        m, vertex, address_dict = self.graph_to_mat(personal_rank_graph)
        mat_all = self.mat_all_point(m, vertex)


