import os
import datetime
import time

print "dumping~~"
os.system('mysqldump -uroot -p131 nwb > nwb.sql')
print "dump Succeed!"

dt = datetime.datetime.now()
strt = dt.strftime('%Y-%m-%d %H:%M:%S')  
os.system('git add .')
os.system('git commit -m "dumps %s"' % strt)
print "git pushing"
os.system('git push')
print "git Succeed!"
print "#####"
#time.sleep(60*60)

