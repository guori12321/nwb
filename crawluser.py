#coding=utf-8
import sys 
reload(sys) 
sys.setdefaultencoding('utf-8') 
from weibo import APIClient
import json
import random
import threading
import datetime
import time
import MySQLdb
import Queue
import os
import datetime

def timediff(timestart, timestop):
	t  = (timestop-timestart)
	time_day = t.seconds / (24*60*60)
	s_time = t.seconds
	ms_time = t.microseconds / 1000000
	usedtime = int(s_time + ms_time)
	time_hour = usedtime / 60 / 60
	time_minute = (usedtime - time_hour * 3600 ) / 60
	time_second =  usedtime - time_hour * 3600 - time_minute * 60
	time_micsecond = (t.microseconds - t.microseconds / 1000000) / 1000

	retstr = "%d天 %02d:%02d:%02d"  %(time_day, time_hour, time_minute, time_second)
	return retstr

tokenlist = [
		'2.00Z21hZC01klB_a5eeb2ccbewGBDSE',
		'2.00Z21hZC01klB_a5eeb2ccbewGBDSE',
		'2.00Z21hZCwRUA3D1ac7184899RDEsHD',
		'2.00Z21hZC9ononDb0371b97760MEIX6',
		'2.00Z21hZCc3QVwDfe836ce906V_5HBE',
		'2.00Z21hZCZIFV3Cdad8ac8b48ruTRRC',
		'2.00Z21hZCQUkaKC8d06e925fdcm_SIB',
		'2.00Z21hZCbv2fNCf81de27045IRMMIC',
		'2.00Z21hZCut1A9B6be8328459JVavOD',
		'2.00Z21hZC0RNIfCa4abbb84dctPdI_E',
		]

APP_KEY = '1234567' # app keyjj
APP_SECRET = 'abcdefghijklmn' # app secret
CALLBACK_URL = 'http://www.example.com/callback' # callback url
expires_in =99999999999

crawledUser = open('crawledUser','w') #记录已经抓取的uid
userlist = []
now = 0
month =['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
messageFile = open('messageFile.txt','w')
total = 0
error = 0
mysqlError = open('mysqlError.txt', 'w')
urlError = open('urlError.txt', 'w')
starttime = datetime.datetime.now()
crawlTime = 0

sp = threading.Semaphore(1) #信号量
sp2 = threading.Semaphore(1) 
sp3 = threading.Semaphore(1) 

class ThreadCrawl(threading.Thread):
	def __init__(self, i):
		self.i = i
		threading.Thread.__init__(self)
		print self.getName(), ' start!'
	def run(self): #i表示用第几个token
		print self.getName()
		global now
		global userlist
		global sp, sp2, sp3
		global total, error, starttime
		global crawlTime

		con = MySQLdb.connect(host='localhost', user='root',passwd='131',charset='utf8')
		con.ping(True)

#选择数据库
		con.select_db('nwb');

#获取操作游标
		cur = con.cursor()
		cur.execute("SET NAMES utf8")
		cur.execute("SET CHARACTER_SET_CLIENT=utf8")
		cur.execute("SET CHARACTER_SET_RESULTS=utf8")
		con.commit()
		
		queue.get()

		client = APIClient(app_key=APP_KEY, app_secret=APP_SECRET, redirect_uri=CALLBACK_URL)
		client.set_access_token(tokenlist[self.i], expires_in)

		flag = True
		ti = 0
		while sp.acquire() and now < len(userlist):
			print flag
			if sp3.acquire():
				tmp = os.system( 'clear' )
				nowtime = datetime.datetime.now()
				print "Run Time: %s" % timediff(starttime, nowtime)
				print "%0.4f%s\tnow:\t%d\ttot:\t%d" % ((now + 0.0)/len(userlist), '%', now, len(userlist))
				print self.getName() + '\ttotal:' + str(total)
				print 'crawlTime: ', crawlTime
				if error != 0:
					print 'ERROR!!!', str(error)
			sp3.release()

			if flag:
				tu = userlist[now]
				now += 1 
				ti = tu % 10 #ti是当前用户的id的最后一位
				flag = False
			sp.release()

			sql = 'select min(id) from data%d where uid = %d' % (ti, tu)
			tt = 1
			cur.execute(sql)
			con.commit()
			tt = cur.fetchone()
			tt = tt[0]
			maxId = 0
			minData = '2013-12-12 00:00:00'
			if tt != None:
				maxId = tt
				sql = 'select createdAt from data%d where id = %d' % (ti, maxId)
				cur.execute(sql)
				con.commit()
				k = cur.fetchone()
				minData = str(k[0])
				print minData
				if minData < '2012-11-00 00:00:00':
					flag = True
					print 'YESSSSSS!!!'
					continue

			try:
				messages = client.statuses.user_timeline.get(uid=tu, count=100, max_id = maxId)
				print 'uid ', tu
				print 'max_id ', maxId
				print messages
			except:
				error += 1
				print self.getName(), ' HTTP ERROR!!', error
				continue

			crawlTime += 1
			if len(messages['statuses']) < 100:
				flag = True
				print 'YESEEEEEEEEE!'
				print len(messages['statuses'])

			for message in messages['statuses']:
				total += 1

				c = message['created_at']
				dt = "%s-%02d-%s %s" % (c[-4:], month.index(c[4:7]) +1, c[8:10], c[11:19] )
				if dt < '2012-11-00 00:00:00':
					flag = True
					print 'YESOOOOOO!'
				if 'deleted' in message:
					message['reposts_count'] = -1
				t = "insert ignore into data%d values(%d, %d,'%s', '%s', %d);\n" %(ti, message['id'], tu, message['text'].replace('\\','\\\\').replace('\'','\\\''), dt, message['reposts_count'])
				print t
				cur.execute(t)
				con.commit()
				

			ttime = nowtime - starttime
			tlimit = (ttime.days * 24 + ttime.seconds / 3600 + 1) * 1000 - 50
			while crawlTime > tlimit:
				nowtime = datetime.datetime.now()
				ttime = nowtime - starttime
				tlimit = (ttime.days * 24 + ttime.seconds / 3600 + 1) * 1000 - 50
				print '\rwait %d seconds~~' % (3600 - ttime.seconds%3600), #加一个,则不换行
		queue.task_done()

######以下为主程序######

#建立和数据库系统的连接
conn = MySQLdb.connect(host='localhost', user='root',passwd='131',charset='utf8')
conn.ping(True)

#获取操作游标
cursor = conn.cursor()

#选择数据库
conn.select_db('nwb');

cursor.execute("SET NAMES utf8")
cursor.execute("SET CHARACTER_SET_CLIENT=utf8")
cursor.execute("SET CHARACTER_SET_RESULTS=utf8")

cursor.execute('select * from user order by uid desc')
conn.commit()

for i in cursor.fetchall(): 
	userlist.append(i[0])
#userlist = userlist[820+95+120+50:]
#userlist = userlist[userlist.index(2458773504):]

queue = Queue.Queue()
for i in range(10):
	queue.put(i)

for i in range(10):
	t = ThreadCrawl(i)
	t.setDaemon(True)
	t.start()

queue.join()

conn.close()
crawledUser.close()
cursor.close()
