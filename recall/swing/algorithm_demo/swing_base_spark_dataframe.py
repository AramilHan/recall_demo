# -*- encoding: utf-8 -*-
"""
@author: Aramil
@date: 2022/8/5 20:49
@brief: 基于movie lens数据集，spark实现Swing算法
"""
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.rdd import RDD
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import pyspark.sql.functions as F
from pyspark.sql.window import Window

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
ctx = SQLContext(spark)


def calc_pair_matrix_udf(infos):
    pair_matrix = list()
    for i in range(0, len(infos) - 1):
        for j in range(i + 1, len(infos)):
            pair_matrix.append((infos[i], infos[j], 1))
    pair_matrix = tuple(pair_matrix)
    return pair_matrix


def calc_ui_pair_matrix_udf(s1, s2, user_list1, user_list2):
    pair_matrix = list()
    user_pair_list = list(set(user_list1) & set(user_list2))
    for i in range(0, len(user_pair_list) - 1):
        for j in range(i + 1, len(user_pair_list)):
            pair_matrix.append((s1, s2, user_pair_list[i], user_pair_list[j]))
    pair_matrix = tuple(pair_matrix)
    return pair_matrix


class SwingBaseSparkDF(object):
    def __init__(self):
        self.train_data_path = "../../../data/ml-100k/ua.base"
        self.test_data_path = "../../../data/ml-100k/ua.test"
        self.item_sim_save_path = "../../../model/swing/swing_item_sim_dict_by_spark.json"
        self.schema = StructType([StructField('user_id', StringType()),
                                  StructField('movie_id', StringType()),
                                  StructField('rate', StringType()),
                                  StructField('event_timestamp', StringType())])

    def load_data(self):
        # 加载训练数据
        train_data = sc.textFile(self.train_data_path)
        train_rdd = train_data.map(lambda x: x.split("\t"))
        train_df = (ctx
                    .createDataFrame(train_rdd, schema=self.schema)
                    .withColumn('user_id', F.col('user_id').cast(IntegerType()))
                    .withColumn('movie_id', F.col('movie_id').cast(IntegerType()))
                    .withColumn('rate', F.col('rate').cast(FloatType()))
                    .withColumn('event_timestamp', F.col('event_timestamp').cast(IntegerType())))
        # 查看每一列数据类型
        # print(train_df.dtypes)
        train_df.show(n=5)
        # 加载测试数据
        test_data = sc.textFile(self.test_data_path)
        test_rdd = test_data.map(lambda x: x.split("\t"))
        test_df = ctx.createDataFrame(test_rdd, schema=self.schema)
        return train_df, test_df

    def calc_item_pair_matrix(self, train_df):
        # 计算物品共现矩阵
        # 对用户id进行物料序列聚合
        user_item_list_df = (train_df
                             .groupBy('user_id')
                             .agg(F.collect_set('movie_id').alias('movie_set')))
        # 物品共现矩阵
        item_pair_matrix_df = (user_item_list_df
                               .rdd
                               .flatMap(lambda x: calc_pair_matrix_udf(infos=x[1]))
                               .toDF(['item_i', 'item_j', 'item_co_score'])
                               .groupBy('item_i', 'item_j')
                               .agg(F.sum('item_co_score').alias('item_co_score'))
                               .drop('item_co_score'))
        item_pair_matrix_df.show(n=5)
        return item_pair_matrix_df

    def calc_user_pair_matrix(self, train_df):
        # 计算用户共现矩阵
        # 对物品id进行用户序列聚合
        item_user_list_df = (train_df
                             .groupBy('movie_id')
                             .agg(F.collect_set('user_id').alias('user_set')))
        # 用户共现矩阵
        user_pair_matrix_df = (item_user_list_df
                               .rdd
                               .flatMap(lambda x: calc_pair_matrix_udf(x[1]))
                               .toDF(['user_i', 'user_j', 'user_co_score'])
                               .groupBy('user_i', 'user_j')
                               .agg(F.sum('user_co_score').alias('user_co_score')))
        user_pair_matrix_df.show(n=5)
        return item_user_list_df, user_pair_matrix_df

    def calc_user_item_pair_matrix(self, item_pair_matrix_df, item_user_list_df):
        # 计算用户物品共现矩阵
        # 物品共现矩阵join物品用户序列集合
        item_pair_user_list_df = (item_pair_matrix_df
                                  .join(item_user_list_df
                                        .withColumnRenamed('movie_id', 'item_i')
                                        .withColumnRenamed('user_set', 'user_set_i'),
                                        'item_i', 'inner')
                                  .join(item_user_list_df
                                        .withColumnRenamed('movie_id', 'item_j')
                                        .withColumnRenamed('user_set', 'user_set_j'),
                                        'item_j', 'inner')
                                  .select('item_i', 'item_j', 'user_set_i', 'user_set_j'))
        # 用户物品共现矩阵
        user_item_pair_matrix_df = (item_pair_user_list_df
                                    .rdd
                                    .flatMap(lambda x: calc_ui_pair_matrix_udf(x[0], x[1], x[2], x[3]))
                                    .toDF(['item_i', 'item_j', 'user_i', 'user_j']))
        user_item_pair_matrix_df.show(n=5)
        return user_item_pair_matrix_df

    def calc_item_sim_swing(self, user_item_pair_matrix_df, user_pair_matrix_df):
        # 计算物料swing相似度
        # 将用户共现次数join到用户物品共现矩阵
        user_item_pair_matrix_co_df = (user_item_pair_matrix_df
                                       .join(user_pair_matrix_df, ['user_i', 'user_j'], 'inner'))
        # 计算sim
        item_single_sim_df = (user_item_pair_matrix_co_df
                              .withColumn('sim', F.lit(1) / (F.lit(1) + F.col('user_co_score')))
                              .select('item_i', 'item_j', 'sim'))

        item_sim_df = (item_single_sim_df
                       .union(item_single_sim_df
                              .withColumnRenamed('item_j', 'item_i')
                              .withColumnRenamed('item_i', 'item_j'))
                       .groupBy('item_i', 'item_j')
                       .agg(F.bround(F.sum('sim'), 5).alias('sim'))
                       .withColumn('rank', F.row_number().over(Window
                                                               .partitionBy('item_i')
                                                               .orderBy(F.col('sim').desc())))
                       .where('rank<=50')
                       .drop('rank'))

        swing_sim_df = (item_sim_df
                        .groupBy('item_i')
                        .agg(F.concat_ws(',', F.collect_set('item_j')).alias('sim_item_list')))
        return swing_sim_df

    def run(self):
        train_df, test_df = self.load_data()
        item_pair_matrix_df = self.calc_item_pair_matrix(train_df)
        item_user_list_df, user_pair_matrix_df = self.calc_user_pair_matrix(train_df)
        user_item_pair_matrix_df = self.calc_user_item_pair_matrix(item_pair_matrix_df, item_user_list_df)
        swing_sim_df = self.calc_item_sim_swing(user_item_pair_matrix_df, user_pair_matrix_df)
        swing_sim_df.show(n=5)


def main():
    sbsd = SwingBaseSparkDF()
    sbsd.run()


if __name__ == '__main__':
    exit(main())
