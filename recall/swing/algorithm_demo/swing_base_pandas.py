# -*- encoding: utf-8 -*-
"""
@author: Aramil
@date: 2022/8/1 19:48
@brief: 基于movie lens数据集，pandas工具实现Swing算法
"""
import json
import os.path
import time

import pandas as pd
from itertools import combinations


class SwingBasePandas(object):
    """
        基于movie lens数据集，pandas工具实现Swing算法
    """
    def __init__(self):
        self.alpha = 0.5
        self.top_n = 20
        self.recommend_n = 100
        self.train_data_path = "../../../data/ml-100k/ua.base"
        self.test_data_path = "../../../data/ml-100k/ua.test"
        self.item_sim_save_path = "../../../model/swing/swing_item_sim_dict_by_pandas.json"

    def load_data(self):
        """
            加载训练、测试数据集
            :returns train_data、test_data
        """
        train_data = pd.read_csv(self.train_data_path, sep="\t", engine="python",
                                 names=["userid", "movieid", "rate", "event_timestamp"])
        test_data = pd.read_csv(self.test_data_path, sep="\t", engine="python",
                                names=["userid", "movieid", "rate", "event_timestamp"])
        print(15 * "=", "训练集", 15 * "=")
        print(train_data.head(5))
        print(15 * "=", "测试集", 15 * "=")
        print(test_data.head(5))
        return train_data, test_data

    def get_uitems_iusers(self, train_data):
        """
        构建Swing算法结构
        :param train_data: 训练集
        :return: u_items: 用户行为物料合集；i_users: 物料被行为用户合集。
        """
        u_items = dict()
        i_users = dict()
        for index, row in train_data.iterrows():
            u_items.setdefault(row["userid"], set())
            i_users.setdefault(row["movieid"], set())

            u_items[row["userid"]].add(row["movieid"])
            i_users[row["movieid"]].add(row["userid"])
        print("数据集的用户数量为:{}".format(len(u_items)))
        print("数据集的item数量为:{}".format(len(i_users)))
        return u_items, i_users

    def cal_sim(self, u_items, i_users):
        """
        计算相似度
        :param u_items: 用户行为物料合集
        :param i_users: 物料被行为用户合集
        :return: item_sim_dict: 物料相似度集合
        """
        # 物料两两之间的组合
        item_pairs = list(combinations(i_users.keys(), 2))
        print("item pairs length:{}".format(len(item_pairs)))
        item_sim_dict = dict()
        cnt = 0
        for (i, j) in item_pairs:
            cnt += 1
            print(cnt)
            user_pairs = list(combinations(i_users[i] & i_users[j], 2))
            result = 0.0
            for (u, v) in user_pairs:
                result += 1 / (self.alpha + list(u_items[u] & u_items[v]).__len__())
            # 过滤result为0的相似度关系
            if result == 0.0:
                continue
            item_sim_dict.setdefault(str(i), dict())
            item_sim_dict[str(i)][str(j)] = result
        return item_sim_dict

    def save_item_sims(self, item_sim_dict):
        """
        保存item相似度文件
        :param item_sim_dict:
        """
        new_item_sim_dict = dict()
        for item, sim_items in item_sim_dict.items():
            new_item_sim_dict.setdefault(str(item), dict())
            new_item_sim_dict[str(item)] = dict(sorted(sim_items.items(), key=lambda k: k[1], reverse=True)[:self.top_n])

        json.dump(new_item_sim_dict, open(self.item_sim_save_path, "w"))
        print("item sim ({})保存成功!".format(self.top_n))
        return new_item_sim_dict

    def evaluate(self, u_items, item_sim_dict, test_data):
        """
        评价函数
        :param u_items:
        :param item_sim_dict:
        :param test_data:
        """
        u_test_items = self.deal_with_test(test_data)
        pr = self.precision(u_items, u_test_items, item_sim_dict)
        rr = self.recall(u_items, u_test_items, item_sim_dict)
        cr = self.coverage(u_items, item_sim_dict)
        print("确准率为:{}".format(pr))
        print("召回率为:{}".format(rr))
        print("覆盖率为:{}".format(cr))
        pass

    def deal_with_test(self, test_data):
        u_test_items = dict()
        for index, row in test_data.iterrows():
            u_test_items.setdefault(row["userid"], set())
            u_test_items[row["userid"]].add(row["movieid"])
        print("测试集使用用户数量为:{}".format(len(u_test_items)))
        return u_test_items

    def get_recommendation(self, user_items_list, item_sim_dict):
        """
        获取用户的推荐结果topN
        :param user_items_list:
        :param item_sim_dict:
        :return:
        """
        recommendation = dict()
        for item in user_items_list:
            sim_items = item_sim_dict.get(str(item), dict())
            if not sim_items:
                continue
            for sim_item, sim_score in sim_items.items():
                recommendation.setdefault(int(sim_item), 0)
                recommendation[int(sim_item)] += sim_score
        recommendation_top = dict(sorted(recommendation.items(), key=lambda k: k[1], reverse=True)[:self.recommend_n])
        return recommendation_top

    def precision(self, u_items, u_test_items, item_sim_dict):
        """
        评估算法的准确率
        :param u_items:
        :param u_test_items:
        :param item_sim_dict:
        :return:
        """
        hit_cnt = 0
        all_cnt = 0
        for user, item_list in u_items.items():
            test_items = u_test_items.get(user, list())
            if not test_items:
                continue
            rank_results = self.get_recommendation(item_list, item_sim_dict)
            if not rank_results:
                continue
            for item, _ in rank_results.items():
                if item in test_items:
                    hit_cnt += 1
            all_cnt += self.recommend_n
        return "{}%".format(round(hit_cnt / (all_cnt * 1.0) * 100, 4))

    def recall(self, u_items, u_test_items, item_sim_dict):
        """
        评估算法的召回率
        :param u_items:
        :param u_test_items:
        :param item_sim_dict:
        :return:
        """
        hit_cnt = 0
        all_cnt = 0
        for user, item_list in u_items.items():
            test_items = u_test_items.get(user, list())
            if not test_items:
                continue
            rank_results = self.get_recommendation(item_list, item_sim_dict)
            if not rank_results:
                continue
            for item, _ in rank_results.items():
                if item in test_items:
                    hit_cnt += 1
            all_cnt += len(test_items)
        return "{}%".format(round(hit_cnt / (all_cnt * 1.0) * 100, 4))

    def coverage(self, u_items, item_sim_dict):
        """
        评估算法的覆盖率
        :param u_items:
        :param item_sim_dict:
        :return:
        """
        recommend_items = set()
        all_items = set()
        for user, item_list in u_items.items():
            for item in item_list:
                all_items.add(item)
            rank_result = self.get_recommendation(item_list, item_sim_dict)
            if not rank_result:
                continue
            for item in rank_result.keys():
                recommend_items.add(item)
        return "{}%".format(round(len(recommend_items) / (len(all_items) * 1.0) * 100, 4))

    def run(self):
        """
            运行函数
        """
        train_data, test_data = self.load_data()
        u_items, i_users = self.get_uitems_iusers(train_data)
        if not os.path.exists(self.item_sim_save_path):
            item_sim_dict = self.cal_sim(u_items, i_users)
            new_item_sim_dict = self.save_item_sims(item_sim_dict)
        else:
            new_item_sim_dict = json.load(open(self.item_sim_save_path, "r"))

        self.evaluate(u_items, new_item_sim_dict, test_data)


def main():
    """
        主函数
    """
    start_time = time.time()
    sbp = SwingBasePandas()
    sbp.run()
    print("Swing python训练时间:{}".format(time.time() - start_time))


if __name__ == '__main__':
    exit(main())

