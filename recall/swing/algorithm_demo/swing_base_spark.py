# -*- encoding: utf-8 -*-
"""
@author: Aramil
@date: 2022/8/2 14:10
@brief: 基于movie lens数据集，spark实现Swing算法
"""
import json
from itertools import combinations
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.rdd import RDD
from pyspark.sql.types import StructType, StructField, IntegerType

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()


def cal_sim(info, item_pairs_dict):
    item1 = info[0][0]
    item2 = info[1][0]
    pairs = info[0][1] & info[1][1]
    score = 0.0
    alpha = 0.5
    for i1 in pairs:
        for i2 in pairs:
            pair_score = item_pairs_dict.get(i1, {}).get(i2, 0)
            if pair_score == 0:
                continue
            score += 1.0 / (alpha + pair_score)
    return item1, item2, score


def tranfrom_dict_top_n(infos):
    sim_dict = dict()
    for info in infos:
        sim_dict.update(info)
    sim_dict = dict(sorted(sim_dict.items(), key=lambda k: k[1], reverse=True)[:20])
    return sim_dict


class SwingBaseSpark(object):
    def __init__(self):
        self.train_data_path = "../../../data/ml-100k/ua.base"
        self.test_data_path = "../../../data/ml-100k/ua.test"
        self.item_sim_save_path = "../../../model/swing/swing_item_sim_dict_by_spark.json"
        self.schema = StructType([StructField('user_id', IntegerType()),
                                  StructField('movie_id', IntegerType()),
                                  StructField('rate', IntegerType()),
                                  StructField('event_timestamp', IntegerType())])

    def load_data(self):
        train_data = sc.textFile(self.train_data_path)
        train_rdd = train_data.map(lambda x: x.split("\t"))
        train_user_rdd = train_rdd.map(lambda x: (x[0], x[1])).groupByKey().map(lambda x: (x[0], set(x[1])))
        # 生成训练集数据RDD的笛卡尔积，并对rdd进行重组
        item_pairs_dict = (train_user_rdd
                           .cartesian(train_user_rdd)
                           .map(lambda x: (x[0][0], (x[1][0], list((x[0][1] & x[1][1])).__len__())))
                           .filter(lambda x: x[0][0] != x[1][0])
                           .groupByKey()
                           .map(lambda x: (x[0], dict(x[1])))
                           .collectAsMap())

        train_item_rdd = train_rdd.map(lambda x: (x[1], x[0])).groupByKey().map(lambda x: (x[0], set(x[1])))
        item_sim_rdd = (train_item_rdd
                        .cartesian(train_item_rdd)
                        .filter(lambda x: x[0][0] != x[1][0])
                        .map(lambda x: cal_sim(x, item_pairs_dict))
                        .filter(lambda x: x[0] != x[1])
                        .filter(lambda x: x[2] != 0.0))
        item_sim_rdd = (item_sim_rdd
                        .map(lambda x: (x[0], dict({x[1]: x[2]})))
                        .groupByKey()
                        .mapValues(list)
                        .map(lambda x: {x[0]: tranfrom_dict_top_n(x[1])}))
        item_sim_dict = item_sim_rdd.collect()
        print(item_sim_dict)
        json.dump(item_sim_dict, open(self.item_sim_save_path, "w"))

    def run(self):
        self.load_data()


def main():
    sbs = SwingBaseSpark()
    sbs.run()


if __name__ == '__main__':
    exit(main())
