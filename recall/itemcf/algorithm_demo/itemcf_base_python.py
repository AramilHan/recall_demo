# -*- encoding: utf-8 -*-
"""
@author: Aramil
@date: 2022/9/29 15:28
@brief: 基于python计算itemcf
"""
import os
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
        self.item_sim_pearson_save_path = "../../../model/itemcf/itemcf_item_sim_pearson_by_python.json"

    def load_data(self):
        train_data = pd.read_csv(self.train_data_path, sep="\t", engine="python",
                                 names=["userid", "movieid", "rate", "event_timestamp"])
        test_data = pd.read_csv(self.test_data_path, sep="\t", engine="python",
                                names=["userid", "movieid", "rate", "event_timestamp"])
        return train_data, test_data

    def get_invert_table(self, train_data):
        """
        建立倒排索引表
        :param train_data:
        """
        invert_table = dict()
        u_items = dict()
        for index, row in train_data.iterrows():
            user_id = int(row['userid'])
            item_id = int(row['movieid'])
            rate = float(row['rate'])
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
                if item_i == item_j:
                    continue
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
            cos_sim[item_j][item_i] = score / math.sqrt(count_item_i * count_item_j)
        # print(cos_sim)
        return cos_sim

    def save_item_cos_sim_dict(self, cos_sim):
        new_cos_sim_dict = dict()
        for item_id, sim_items in cos_sim.items():
            new_cos_sim_dict.setdefault(str(item_id), dict())
            new_cos_sim_dict[str(item_id)] = dict(sorted(sim_items.items(), key=lambda k: k[1], reverse=True)[:self.top_n])
        json.dump(new_cos_sim_dict, open(self.item_sim_save_path, "w"))
        print("item cos sim ({})保存成功!".format(self.top_n))
        return new_cos_sim_dict

    def get_pearson_calc_factor(self, invert_table):
        item_rate_dict = dict()
        item_rate_avg_dict = dict()
        item_pearson_molecular = dict()
        item_pearson_denominator = dict()
        for user_id, item_dict in invert_table.items():
            for item_id, rate in item_dict.items():
                item_rate_dict.setdefault(item_id, list())
                item_rate_dict[item_id].append(rate)
        for item_id, rate_list in item_rate_dict.items():
            item_total_rate = 0
            for rate in rate_list:
                item_total_rate += rate
            item_rate_avg_dict.setdefault(item_id, 0)
            if not rate_list:
                item_rate_avg_dict[item_id] = item_total_rate / len(rate_list)
        for user_id, item_dict in invert_table.items():
            for item_id, rate in item_dict.items():
                item_pearson_denominator.setdefault(item_id, 0)
                rate_item_avg = item_rate_avg_dict[item_id]
                item_pearson_denominator[item_id] += rate - rate_item_avg
            item_list = list(item_dict.keys())
            for item_i, item_j in combinations(item_list, 2):
                if item_i == item_j:
                    continue
                if item_i < item_j:
                    item_i, item_j = item_j, item_i
                rate_i = item_dict[item_i]
                rate_j = item_dict[item_j]
                rate_avg_i = item_rate_avg_dict[item_i]
                rate_avg_j = item_rate_avg_dict[item_j]
                item_pearson_molecular.setdefault((item_i, item_j), 0)
                item_pearson_molecular[(item_i, item_j)] += (rate_i - rate_avg_i) * (rate_j - rate_avg_j)
        return item_pearson_denominator, item_pearson_molecular

    def calc_pearson_sim(self, item_pearson_denominator, item_pearson_molecular):
        pearson_sim = dict()
        for (item_i, item_j), score in item_pearson_molecular.items():
            count_item_i = item_pearson_denominator[item_i]
            count_item_j = item_pearson_denominator[item_j]
            pearson_sim.setdefault(item_i, dict())
            pearson_sim.setdefault(item_j, dict())
            pearson_sim[item_i][item_j] = score / math.sqrt(count_item_i * count_item_j)
            pearson_sim[item_j][item_i] = score / math.sqrt(count_item_i * count_item_j)
        return pearson_sim

    def save_item_pearson_sim_dict(self, pearson_sim):
        new_pearson_sim_dict = dict()
        for item_id, sim_items in pearson_sim.items():
            new_pearson_sim_dict.setdefault(str(item_id), dict())
            new_pearson_sim_dict[str(item_id)] = dict(
                sorted(sim_items.items(), key=lambda k: k[1], reverse=True)[:self.top_n])
        json.dump(new_pearson_sim_dict, open(self.item_sim_pearson_save_path, "w"))
        print("item pearson sim ({})保存成功!".format(self.top_n))
        return new_pearson_sim_dict

    def sim_base_cos(self, invert_table):
        if not os.path.exists(self.item_sim_save_path):
            item_behavior_count, intersect = self.get_cos_calc_factor(invert_table)
            cos_sim = self.calc_cos_sim(item_behavior_count, intersect)
            new_cos_sim_dict = self.save_item_cos_sim_dict(cos_sim)
        else:
            new_cos_sim_dict = json.load(open(self.item_sim_save_path, "r"))
        return new_cos_sim_dict

    def sim_base_pearson(self, invert_table):
        if not os.path.exists(self.item_sim_pearson_save_path):
            item_pearson_denominator, item_pearson_molecular = self.get_pearson_calc_factor(invert_table)
            pearson_sim = self.calc_pearson_sim(item_pearson_denominator, item_pearson_molecular)
            new_pearson_sim_dict = self.save_item_pearson_sim_dict(pearson_sim)
        else:
            new_pearson_sim_dict = json.load(open(self.item_sim_pearson_save_path, "r"))
        return new_pearson_sim_dict

    def run(self):
        tag = "pearson"
        train_data, test_data = self.load_data()
        invert_table, u_items = self.get_invert_table(train_data)
        if tag == "cos":
            cos_sim = self.sim_base_cos(invert_table)
            recall_evalute.evaluate(u_items, cos_sim, test_data, self.recommend_n)
        if tag == "pearson":
            pearson_sim = self.sim_base_pearson(invert_table)
            recall_evalute.evaluate(u_items, pearson_sim, test_data, self.recommend_n)


def main():
    ibp = ItemCFBasePython()
    ibp.run()


if __name__ == '__main__':
    exit(main())
