#-*- coding: utf-8 -*-
#Title : TencentVideo_comment_crawler
#腾讯视频评论爬虫
#Author ： Richard
#Date: 2022/4/25


import re
import random
import urllib.request
import time
import pandas as pd
from sqlalchemy import create_engine
# 配置链接数据库信息
db_config = {
    'host': '127.0.0.1',
    'port': '3306',
    'database': 'filmproject_test',
    'username': 'root',
    'password': '1234'
}
# 数据库链接地址
db_url = 'mysql+pymysql://{username}:{password}@{host}:{port}/{database}?charset=utf8mb4'.format(**db_config)
# 创建数据库引擎
engine = create_engine(db_url)
# 创建数据库链接


#构建用户代理
uapools=["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
         "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36",
         "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0",
        ]
#从用户代理池随机选取一个用户代理
def ua(uapools):
    thisua=random.choice(uapools)
    #print(thisua)
    headers=("User-Agent",thisua)
    opener=urllib.request.build_opener()
    opener.addheaders=[headers]
    #设置为全局变量
    urllib.request.install_opener(opener)

def Launch_countdown():
    print("发射倒计时")
    for i in range(1,11,1):
        print(11-i)
        time.sleep(1)
    print("点火")



#获取源码
def get_content(lastId,comment_id):
    url="https://video.coral.qq.com/varticle/"+comment_id+"/comment/v2?callback=_varticle"+comment_id+"commentv2&orinum=10&oriorder=o&pageflag=1&cursor="+lastId
    request=urllib.request.urlopen(url)
    print(url)
    html=request.read().decode("utf-8","ignore")
    #print(html)
    request.close()
    return html

#从源码中获取评论的评论文本数据
def get_comment_content(html):
    pat1='"content":"(.*?)"'
    rst1 = re.compile(pat1,re.S).findall(html)
    return rst1

#从源码中获取评论的评论ID号
def get_comment_id(html):
    pat2='"id":"(.*?)"'
    rst2 = re.compile(pat2,re.S).findall(html)
    return rst2


#从源码中获取评论的评论时间
def get_comment_time(html):
    pat3='"time":"(.*?)"'
    rst3 = re.compile(pat3,re.S).findall(html)
    return rst3


#从源码中获取评论的评论用户id
def get_comment_userid(html):
    pat4='"userid":"(.*?)"'
    rst4 = re.compile(pat4,re.S).findall(html)
    return rst4


#从源码中获取下一轮刷新页的ID
def get_lastId(html):
    pat='"last":"(.*?)"'
    lastId = re.compile(pat,re.S).findall(html)[0]
    return lastId

def get_comment_num(html_first):
    patt = '"oritotal":(.*?),'
    numm = re.compile(patt,re.S).findall(html_first)[0]
    return numm


def crawler(comment_id):#执行
    ua(uapools)
    #初始页面
    #初始待刷新页面ID
    lastId='0'
    userid=[]
    addTime=[]
    content=[]
    contentid=[]
    page = []

    html_first = get_content(lastId,comment_id)#第一次请求API（第一页）以获得改集评论条数
    # print(html_first)

    num = int(int(get_comment_num(html_first))/10)

    print('该集总共需抓取：',num,'页')


    # data=pd.DataFrame(columns=['contentid','content','addTime','userid','page'])
    time.sleep(5)

    for i in range(1,num):
        try:
            html = get_content(lastId,comment_id)#
            # print(html)
            #获取评论文本数据数据
            contentlist = get_comment_content(html)
            # print(contentlist)
            idlist = get_comment_id(html)
            # print(idlist)
            timelist = get_comment_time(html)
            # print(timelist)
            useridlist = get_comment_userid(html)
            # print(useridlist)


            print("------第"+str(i)+"轮/"+str(num)+"页面评论数据------")

            for j in range(1,len(contentlist)):
                # print("第"+str(j)+"条评论：" +str(commentlist[j]))
                content.append(str(contentlist[j]))
                contentid.append(str(idlist[j]))
                addTime.append(str(timelist[j]))
                userid.append(str(useridlist[j]))
                page.append(i)

            print("Finish %d 抓取" % i)
            #获取下一轮刷新页ID
            lastId=get_lastId(html)
            # print(lastId)

            time.sleep(1)

        except:
            print("Start Sleep")
            time.sleep(5)
            continue

        crawler_data = pd.DataFrame({'content':content,'contentid':contentid,'addTime':addTime,'userid':userid,'page':page})
        # print(crawler_data)



        filename = 'xxxx_s01e01' + '_'+'commment_id_'+comment_id
        crawler_data.to_sql(filename, engine, if_exists='replace')
        print("Finish %d 写入" % i)


if __name__ == "__main__":
    comment_id = str(input("请输入发射密码:"))#输入int转为str
    Launch_countdown()#

    t1 = time.time()

    crawler(comment_id)
    print('------全部评论数据抓取完毕！------')
    t2 = time.time()
    t3 = round(t2-t1,2)
    print('------本次抓取耗时:%s秒------' % t3)
    print("------已经入预定轨道，发射成功！------")