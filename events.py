# coding=utf-8
import nsq
import time,datetime
import torndb
import json

#数据库连接
db = torndb.Connection('127.0.0.1', 'km_mall', 'root', 'root')

cache = {
    'last_time': int(time.time()),
    'id': 0,
    'a1_key': 0,
    'a2_key': 0,
    'a3_key': 0
}
now = datetime.datetime.now()
last_ymd = now.strftime("%Y%m%d")

def mysql_c(ymd, event):
    global last_ymd
    if last_ymd != ymd:
        update_mysql_data(cache['id'])
        cache['id'] = 0
        cache['a1_key'] = 0
        cache['a2_key'] = 0
        cache['a3_key'] = 0
        cache['last_time'] = int(time.time())
        last_ymd = ymd

    if cache['id'] > 0:
        pass
    else:
        sql = "SELECT * FROM `mall_month_info` WHERE `ymd`=%s AND `type` = 'event'" % (ymd)
        item = db.query(sql)
        if len(item) > 0:
            cache['id'] = item[0]['id']
        else:
            cache['id'] = create_mysql_data(ymd)
            print("插入id:%s") % (cache['id'])

    if event == 'km_mall_UserJumpLink':
            cache['a1_key'] = cache['a1_key'] + 1

    elif event == 'km_mall_UserLevelUp':
            cache['a2_key'] = cache['a2_key'] + 1

    elif event == 'km_mall_UserJumpTkl':
            cache['a3_key'] = cache['a3_key'] + 1

    if cache['last_time'] + 60 < int(time.time()):  #超时更新数据
        print (cache)
        if cache['a1_key']==0 and cache['a2_key']==0 and cache['a3_key']==0:
            return
        rs=update_mysql_data(cache['id'])
        print ("update 更新%s条 id: %s, 日期: %s") % (rs, cache['id'], ymd)
        cache['a1_key'] = 0
        cache['a2_key'] = 0
        cache['a3_key'] = 0
        cache['last_time'] = int(time.time())


def create_mysql_data(ymd):
    sql = "INSERT INTO `mall_month_info`(ymd,type,a1,a2,a3,create_time) VALUES ('%s','%s','0','0','0',%s)" % (
        ymd, 'event', int(time.time()))
    rs = db.insert(sql)
    return rs

def update_mysql_data(id):
    sql = "UPDATE `mall_month_info` SET a1=a1+%s, a2= a2+%s, a3= a3+%s WHERE id=%d" % (
    cache['a1_key'], cache['a2_key'], cache['a3_key'], id)
    rs = db.update(sql)
    return rs

def handler(message):
    now = datetime.datetime.now()
    ymd = now.strftime("%Y%m%d")
    data=json.loads(message.body)
    mysql_c(ymd, data['event'])
    return True

##nsq链接 
r = nsq.Reader(message_handler=handler, nsqd_tcp_addresses=['127.0.0.1:4150'], topic='km_mall', channel='stat_events',lookupd_poll_interval=15)
nsq.run()
