import pandas as pd
import pymssql
import pymysql

def updatefields(id, brand, propref, propname):
    mycur = mysqlconn.cursor()
    mycur.execute("select rmc_name, owner_name from rmcs r inner join owner o on o.owner_id = r.owner_id inner join lookup_rmc lr on lr.rmc_lookup = r.rmc_num inner join subsidiary s on s.subsidiary_id = r.subsidiary_id where rmc_ref = '" + propref + "' and subsidiary_name = '" + brand + "'")
    prop = mycur.fetchone()
    mscur = conn.cursor()
    #print("update mw_major_works set oldcompref = '" + propref + "', propname = '" + str(prop[0]) + "', owner_name = '" + str(prop[1]) + "' where id = " + str(id))
    mscur.execute("update mw_major_works set oldcompref = '" + propname.replace("'", "''") + "', propname = '" + str(prop[0]).replace("'", "''") + "', owner_name = '" + str(prop[1]).replace("'", "''") + "' where id = " + str(id))
    #if not prop:
    #    print (str(id) + "-" + brand + "-" + propref)

conn = pymssql.connect(server="HODD1SRSQL1\HODD1DB3", user="xxx", password="xxx", database="RMGReporting", charset='utf8')
conn.autocommit(True)

mwdf = pd.read_sql("select id, brand, propref, propname from mw_major_works where oldcompref is null", conn)

mysqlconn = pymysql.connect(host="hodd1srmysql2", port=3306, user="xxx", passwd="xxx", db="intranet", charset='utf8')
mysqlconn.autocommit(True)

mwdf.apply(lambda row: updatefields(row['id'], row['brand'], row['propref'], row['propname']), axis=1)

