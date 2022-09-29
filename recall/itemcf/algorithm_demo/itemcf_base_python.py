# -*- encoding: utf-8 -*-
"""
@author: Aramil
@date: 2022/9/29 15:28
@brief: 基于python计算itemcf
"""
import csv
import math
import json
import pandas as pd
from itertools import combinations
from utils.evaluate import recall_evalute


class ItemCFBasePython(object):
    """
        基于Python计算ItemCF
    """

    def __init__(self):
        self.top_n = 20
        self.recommend_n = 100
        self.train_data_path = "../../../data/ml-100k/ua.base"
        self.test_data_path = "../../../data/ml-100k/ua.test"
        self.item_sim_save_path = "../../../model/itemcf/itemcf_item_sim_dict_by_python.json"

    def load_data(self, file):
        # 读取数据的header
        header_list = ['userid', 'movieid', 'rate', 'event_timestamp']
        # 读取训练数据
        train_data = list()
        with open(file, "r") as train_file:
            train_reader = csv.reader(train_file)
            for _, line in enumerate(train_reader):
                train_line_data = line[0].strip().split("\t")
                # 将训练数据转换成int类型
                train_line_data = [int(x) for x in train_line_data]
                train_line_dict = dict(zip(header_list, train_line_data))
                train_data.append(train_line_dict)
            train_file.close()
        return train_data

    def get_invert_table(self, train_data):
        """
        建立倒排索引表
        :param train_data:
        """
        invert_table = dict()
        u_items = dict()
        for row in train_data:
            user_id = row['userid']
            item_id = row['movieid']
            rate = row['rate']
            invert_table.setdefault(user_id, dict())
            invert_table[user_id].setdefault(item_id, 0)
            invert_table[user_id][item_id] = max(invert_table[user_id][item_id], rate)
            u_items.setdefault(user_id, set())
            u_items[user_id].add(item_id)
        return invert_table, u_items

    def get_cos_calc_factor(self, invert_table):
        item_behavior_count = dict()
        intersect = dict()
        for user_id, item_dict in invert_table.items():
            for item_id, rate in item_dict.items():
                item_behavior_count.setdefault(item_id, 0)
                item_behavior_count[item_id] += rate
            item_list = list(item_dict.keys())
            for item_i, item_j in combinations(item_list, 2):
                if item_i < item_j:
                    item_i, item_j = item_j, item_i
                intersect.setdefault((item_i, item_j), 0)
                intersect[(item_i, item_j)] += min(item_dict[item_i], item_dict[item_j])
        return item_behavior_count, intersect

    def calc_cos_sim(self, item_behavior_count, intersect):
        cos_sim = dict()
        for (item_i, item_j), score in intersect.items():
            count_item_i = item_behavior_count[item_i]
            count_item_j = item_behavior_count[item_j]
            cos_sim.setdefault(item_i, dict())
            cos_sim.setdefault(item_j, dict())
            cos_sim[item_i][item_j] = score / math.sqrt(count_item_i * count_item_j)
            cos_sim[item_i][item_j] = score / math.sqrt(count_item_i * count_item_j)
        # print(cos_sim)
        return cos_sim

    def save_item_cos_sim_dict(self, cos_sim):
        new_cos_sim_dict = dict()
        for item_id, sim_items in cos_sim.items():
            new_cos_sim_dict.setdefault(str(item_id), dict())
            new_cos_sim_dict[str(item_id)] = dict(sorted(sim_items.items(), key=lambda k: k[1], reverse=True)[:self.top_n])
        json.dump(new_cos_sim_dict, open(self.item_sim_save_path, "w"))
        print("item cos sim ({})保存成功!".format(self.top_n))

    def run(self):
        train_data = self.load_data(self.train_data_path)
        invert_table, u_items = self.get_invert_table(train_data)
        item_behavior_count, intersect = self.get_cos_calc_factor(invert_table)
        cos_sim = self.calc_cos_sim(item_behavior_count, intersect)
        self.save_item_cos_sim_dict(cos_sim)
        test_data = pd.read_csv(self.test_data_path, sep="\t", engine="python",
                                names=["userid", "movieid", "rate", "event_timestamp"])
        recall_evalute.evaluate(u_items, cos_sim, test_data, self.recommend_n)


def main():
    ibp = ItemCFBasePython()
    ibp.run()


if __name__ == '__main__':
    exit(main())
