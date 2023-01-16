# -*- encoding: utf-8 -*-
"""
@author: Aramil
@date: 2022/10/4 22:41
@brief: personal-rank基于python实现
"""
import operator
import json

import pandas as pd


class PersonalRankBasePython(object):
    """
        基于python实现personal_rank原始公式
    """
    def __init__(self):
        self.train_file = "../../../data/ml-100k/ua.base"
        self.test_file = "../../../data/ml-100k/ua.test"
        self.personal_rank_save_file = "../../../model/personal_rank/pr_user_item_recommend_dict_origin_by_python.json"
        self.alpha = 0.8
        self.iter_num = 100
        self.topN = 100
        pass

    def read_data(self):
        """
            读取personal_rank数据
        """
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

    def personal_rank(self, personal_rank_graph, train_user_set):
        user_recommend_result = dict()
        cnt = 0
        print(len(list(train_user_set)))
        for user_id in train_user_set:
            rank = {point: 0 for point in personal_rank_graph}
            rank[user_id] = 1
            recommend_result = dict()
            for iter_index in range(self.iter_num):
                tmp_rank = {point: 0 for point in personal_rank_graph}
                for out_point, out_dict in personal_rank_graph.items():
                    for inner_point, value in personal_rank_graph[out_point].items():
                        tmp_rank[inner_point] += round(self.alpha * rank[out_point]/len(out_dict), 4)
                        if inner_point == user_id:
                            tmp_rank[inner_point] += round(1 - self.alpha, 4)
                if tmp_rank == rank:
                    break
                rank = tmp_rank
            right_num = 0
            for zuhe in sorted(rank.items(), key=operator.itemgetter(1), reverse=True):
                point, pr_score = zuhe[0], zuhe[1]
                if point in personal_rank_graph[user_id]:
                    continue
                recommend_result.setdefault(point, 0)
                recommend_result[point] = pr_score
                right_num += 1
                if right_num > self.topN:
                    break
            user_recommend_result.setdefault(user_id, dict())
            user_recommend_result[user_id] = recommend_result
            cnt += 1
            if cnt % 10 == 0:
                print(cnt)
        return user_recommend_result

    def save_user_recommend_result_dict(self, user_recommend_result):
        json.dump(user_recommend_result, open(self.personal_rank_save_file, 'w'))
        print("pr original by python 保存成功！")
        pass

    def run(self):
        train_data, test_data = self.read_data()
        personal_rank_graph, train_user_set = self.get_personal_rank_graph(train_data)
        user_recommend_result = self.personal_rank(personal_rank_graph, train_user_set)
        self.save_user_recommend_result_dict(user_recommend_result)


def main():
    prbp = PersonalRankBasePython()
    prbp.run()


if __name__ == '__main__':
    exit(main())

