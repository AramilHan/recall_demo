# -*- encoding: utf-8 -*-
"""
@author: Aramil
@date: 2022/9/29 16:40
@brief: 抽取离线评估共用函数
"""


def get_recommendation(user_items_list, item_sim_dict, recommend_n):
    """
    获取用户的推荐结果topN
    :param recommend_n:
    :param user_items_list:
    :param item_sim_dict:
    :return:
    """
    recommendation = dict()
    for item in user_items_list:
        sim_items = item_sim_dict.get(str(item), dict())
        if not sim_items:
            sim_items = item_sim_dict.get(int(item), dict())
        if not sim_items:
            continue
        for sim_item, sim_score in sim_items.items():
            recommendation.setdefault(int(sim_item), 0)
            recommendation[int(sim_item)] += sim_score
    recommendation_top = dict(sorted(recommendation.items(), key=lambda k: k[1], reverse=True)[:recommend_n])
    return recommendation_top


def precision(u_items, u_test_items, item_sim_dict, recommend_n):
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
        rank_results = get_recommendation(item_list, item_sim_dict, recommend_n)
        if not rank_results:
            continue
        for item, _ in rank_results.items():
            if item in test_items:
                hit_cnt += 1
        all_cnt += recommend_n
    print(hit_cnt, all_cnt)
    return "{}%".format(round(hit_cnt / (all_cnt * 1.0) * 100, 4))


def recall(u_items, u_test_items, item_sim_dict, recommend_n):
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
        rank_results = get_recommendation(item_list, item_sim_dict, recommend_n)
        if not rank_results:
            continue
        for item, _ in rank_results.items():
            if item in test_items:
                hit_cnt += 1
        all_cnt += len(test_items)
    return "{}%".format(round(hit_cnt / (all_cnt * 1.0) * 100, 4))


def coverage(u_items, item_sim_dict, recommend_n):
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
        rank_result = get_recommendation(item_list, item_sim_dict, recommend_n)
        if not rank_result:
            continue
        for item in rank_result.keys():
            recommend_items.add(item)
    return "{}%".format(round(len(recommend_items) / (len(all_items) * 1.0) * 100, 4))


def evaluate(u_items, item_sim_dict, test_data, recommend_n):
    """
    评价函数
    :param u_items:
    :param item_sim_dict:
    :param test_data:
    """
    u_test_items = deal_with_test(test_data)
    pr = precision(u_items, u_test_items, item_sim_dict, recommend_n)
    rr = recall(u_items, u_test_items, item_sim_dict, recommend_n)
    cr = coverage(u_items, item_sim_dict, recommend_n)
    print("确准率为:{}".format(pr))
    print("召回率为:{}".format(rr))
    print("覆盖率为:{}".format(cr))
    pass


def deal_with_test(test_data):
    u_test_items = dict()
    for index, row in test_data.iterrows():
        u_test_items.setdefault(row["userid"], set())
        u_test_items[row["userid"]].add(row["movieid"])
    print("测试集使用用户数量为:{}".format(len(u_test_items)))
    return u_test_items