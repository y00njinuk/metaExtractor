import cx_Oracle as ora
import pandas as pd 
pd.set_option('display.max_row', 500)
pd.set_option('display.max_columns', 500)

'''
if len(sys.argv) < 5:
	print("Usage : makeMetaData.py User Password IP Port SID")
	return
'''

# Connect Information 
user = 'ewpbigdata'
password = 'Ewpbigdata2019!'
ip = 'localhost'
port = '1521'
sid = 'orcl'
schema = 'EWPBIGDATA'

# step 1) Make URI
URI = "%s/%s@%s:%s/%s" % (user, password, ip, port, sid)

# step 2) Connect and make cursor object
## step 2.1) Check database connect 
conn = ora.connect(URI)
## step 2.2) Check schema connect (Skip)
## step 2.3) Make cursor object 
cursor = conn.cursor()

# step 3) TB_META_0001
## step 3.1) Check to existed connect info
sysname = 'O001'
acntname = 'NEWKMS'

# step 4) TB_META_0002
## step 4.1) Look up table of schema 
cursor.execute( f"SELECT TABLE_NAME FROM ALL_TABLES " 
				f"WHERE OWNER=\'{schema}\' ORDER BY TABLE_NAME" )
tupleList = cursor.fetchall()

## step 4.2) Make dataframe 
meta02Columns = [ 'CNNC_MANAGE_NO', 'DB_TABLE_ID', 'TABLE_ENG_NM', 'TABLE_KOREAN_NM', 'TABLE_DC',
	'BR_DC', 'HASHTAG_CN', 'MNGR_NM', 'WHERE_INFO_NM', 'INS_NUM_MAPPERS', 'INS_SPLIT_BY_COL',
	'APD_WHERE', 'APD_CHK_COL', 'APD_LAST_VAL', 'HIVE_TABLE_NM', 'ANAL_PRUSE_TRGET_AT',
	'BIGDATA_GTRN_AT', 'SCHDUL_APPLC_AT', 'GTHNLDN_MTH_CODE', 'GTHNLDN_MTH_NM', 'REGIST_DE',
	'REGIST_EMPL_NO', 'UPDT_DE', 'UPDT_EMPL_NO' ]

meta02List = pd.DataFrame(columns=meta02Columns)

## step 4.2) Search table info
for i, tuple in enumerate(tupleList):
	try:
		tableEngName = ''.join(tuple[0])
		meta02List.loc[i, ['CNNC_MANAGE_NO','DB_TABLE_ID']] = (sysname, (acntname+'_')+(tableEngName))
		meta02List.loc[i, ['BIGDATA_GTRN_AT','SCHDUL_APPLC_AT']] = 'Y'
		meta02List.loc[i, ['GTHNLDN_MTH_CODE', 'GTHNLDN_MTH_NM']] = ('02', '재적재')
		
		sql = (	f"SELECT COMMENTS FROM ALL_TAB_COMMENTS " 
				f"WHERE 1=1 AND OWNER=\'{schema}\' " 
				f"AND TABLE_NAME=\'{tableEngName}\'" )
		cursor.execute(sql)
		tableKorName = cursor.fetchone()
		
		if None in tableKorName:
			continue
		else :
			meta02List.loc[i,['TABLE_KOREAN_NM', 'TABLE_DC']] = ''.join(tableKorName)

	except Exception as error:
		print("Error SQL : {0}".format(sql))
		print("Caused by : {0}\n".format(error))

## print result
print(meta02List.head(10))

# step 5) TB_META_0003
# step 5.1)



# step 6) Connection Close  
cursor.close()
conn.close()