#!/usr/bin/env python
# -*- coding: utf-8 -*-
import csv
import pandas as pd
import json
import re
import jieba
from jieba import analyse

annual_report_ind_path = 'data/分行业.json'
zhengjianhui_ind_path = 'data/industry.txt'
shenyin_ind_path = 'data/申银万国行业分类.xlsx'
stopwords = 'data/stopwords.txt'
zjh_res, sy_res, com_ap_res, mappings, zjh_to_sy, kws = {}, {}, '', {}, {}, []

'''
1. 行业直接/间接对应到申万
2. 行业对应到证监会（直接/间接），证监会到申万
'''


def process():
    global zjh_res, sy_res, com_ap_res
    zjh = pd.read_csv(zhengjianhui_ind_path)
    zjh_res, ca, big, mid = {}, '', '', ''
    for index, row in zjh.iterrows():
        code, name, cl, intro = str(row['行业代码']), str(row['类别']), str(row['名称']), str(row['简介'])
        if len(code) == 1:
            ca = cl
            zjh_res[cl] = {'类别': name, '门类': '', '大类': '', '中类': '', '简介': intro}
        elif len(code) == 2:
            big = cl
            zjh_res[cl] = {'类别': name, '门类': ca, '大类': '', '中类': '', '简介': intro}
        elif len(code) == 3:
            mid = cl
            zjh_res[cl] = {'类别': name, '门类': ca, '大类': big, '中类': '', '简介': intro}
        else:
            zjh_res[cl] = {'类别': name, '门类': ca, '大类': big, '中类': mid, '简介': intro}

    sy = pd.read_excel(shenyin_ind_path)
    first, second = '', ''
    for index, row in sy.iterrows():
        if pd.notnull(row['一级行业名称']):
            first = str(row['一级行业名称'])
        if pd.notnull(row['二级行业名称']):
            second = str(row['二级行业名称'])
        if pd.notnull(row['三级行业名称']):
            third = str(row['三级行业名称'])
            sy_res[third] = {'一级': first, '二级': second}

    with open(annual_report_ind_path) as f:
        com_ap_res = json.load(f)

    with open(stopwords) as f:
        global kws
        reader = csv.reader(f)
        for row in reader:
            kws += row
    print()


def concat_lists_without_duplicates(l1, l2):
    if not l1:
        return l2
    if not l2:
        return l1
    for val in l1:
        if val not in l2:
            l2.append(val)


def indirect_search_in_shenwan(segs):
    res = []
    for seg in segs:
        shenwan_direct_res = direct_search_in_shenwan([seg])
        res = concat_lists_without_duplicates(res, shenwan_direct_res)
    # print('indirect: segs{}, res{}'.format(segs, res))
    return res


# 区分1，2级
def direct_search_in_shenwan(inds):
    res = []
    for ind in inds:
        for k, v in sy_res.items():
            if ind in k or ind in v['二级']:
                if v['二级'] not in res:
                    res.append(v['二级'])
            else:
                if ind in v['一级'] and v['一级'] not in res:
                    res.append(v['一级'])
    return res


def get_up_zjh_industry(zjh_name):
    record = zjh_res[zjh_name]
    data = [zjh_name, record['门类'], record['大类'], record['中类']]
    new_data = []
    for d in data:
        if d and len(d) > 0:
            new_data.append(d)
    return new_data


def zhengjianhui_to_shenyin(zjh_names):
    res = []
    global zjh_to_sy
    for zjh_name in zjh_names:
        if zjh_name in zjh_to_sy:
            res += zjh_to_sy[zjh_name]
        else:
            record = get_up_zjh_industry(zjh_name)
            # print('tmp: {}'.format(record))
            jieba.analyse.set_stop_words(stopwords)
            segs = jieba.analyse.extract_tags(record[0])
            for seg in segs:
                res += direct_search_in_shenwan([seg])
                res += indirect_search_in_shenwan([seg])
            for r in res:
                zjh_to_sy.setdefault(zjh_name, []).append(r)
    return res


def direct_search_in_zhengjianhui(ind):
    res = []
    for k, v in zjh_res.items():
        if ind in k or ind in v['简介']:
            res.append(k)
    res = zhengjianhui_to_shenyin(res)
    return res


def indirect_search_in_zhengjianhui(segs):
    res = []
    for seg in segs:
        res += direct_search_in_zhengjianhui(seg)
    return res


def get_valid_segs(segs):
    new_segs = []
    for seg in segs:
        if len(seg) > 1:
            for kw in kws:
                seg = seg.replace(kw, '') if kw in seg else seg
            if seg:
                new_segs.append(seg)
    return new_segs


def match_industry(ind):
    punctuation = re.compile(r'[-.?!,":;()（）|0-9]')
    ind = punctuation.sub("", ind)
    res = direct_search_in_shenwan([ind])
    # print('direct ind: {}, res: {}'.format(ind, res))
    if not res:
        jieba.analyse.set_stop_words(stopwords)
        segs = jieba.analyse.extract_tags(ind, topK=3, withWeight=False)
        segs = get_valid_segs(segs)
        res = indirect_search_in_shenwan(segs)
        # if not res:
        #     res = direct_search_in_zhengjianhui(ind)
    #         if not res:
    #             res += indirect_search_in_zhengjianhui(segs)
    return res


def verify_an_industry(ind):
    if not ind:
        return False
    sws = ['其它', '其他', '主营']
    for stopword in sws:
        if stopword in ind:
            return False
    return True


def match_for_company():
    global mappings
    num_com, num_ind = 0, 0
    for com in com_ap_res:
        revenue, inds, com_name = com['分行业收入'], com['行业分类'], com['公司名称']
        # print('name: {}'.format(com_name))
        for ind in inds:
            if verify_an_industry(ind) is True:
                if ind not in mappings:
                    res = match_industry(ind)
                    if res:
                        mappings[ind] = res
                        # print('comp: {}, ind: {}'.format(com_name, ind))
                    # data = 'ind: {}, res: {}'.format(ind, res) if res else 'No match'
                    # print(data)
                else:
                    # print('ind: {}, res: {}'.format(ind, mappings[ind]))
                    pass


def write_mappings_to_excel():
    df = pd.DataFrame(columns=['年报行业', '匹配行业'])
    for k, v in mappings.items():
        row = pd.Series({'年报行业': k, '匹配行业': v})
        df.loc[len(df)] = row
    df.head()
    df.to_excel('data/匹配结果.xlsx')


def start():
    process()
    match_for_company()
    write_mappings_to_excel()


start()