# Reference : https://stackoverflow.com/questions/35045879/cx-oracle-how-can-i-receive-each-row-as-a-dictionary

import argparse
import csv
import re
import cx_Oracle as ora
from collections import defaultdict

def run(user, password, ip, port, sid, schema):
	# step 1) Make URI
	init_URI = "%s/%s@%s:%s/%s" % ("ID", "Password", "Host", "Port", "SID")
	meta_URI = "%s/%s@%s:%s/%s" % (user, password, ip, port, sid)

	# step 2) Check database connect
	init_conn = ora.connect(init_URI)
	meta_conn = ora.connect(meta_URI)

	# step 3) Make cursor object
	init_cursor = init_conn.cursor()
	meta_cursor = meta_conn.cursor()

	################################################ TB_META_0001 ################################################
	fp = open("TB_META_0001.csv", "w", newline='', encoding='utf-8-sig')
	writer = csv.writer(fp)

	query_fp = open("TB_META_0001_query.txt", "w", encoding='utf-8-sig')

	init_cursor.execute(f"SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS \
		WHERE TABLE_NAME=\'TB_META_0001\' AND OWNER=\'EWPBIGDATA\'")
	init_cursor.rowfactory = lambda *args: "".join(args)
	colum_list = init_cursor.fetchall()
	writer.writerow(colum_list)

	init_cursor.execute(f" SELECT CASE WHEN SUBSTR(MAX(CNNC_MANAGE_NO), 2)+1 < 100 THEN \
	CONCAT(\'O\', TO_CHAR(SUBSTR(MAX(CNNC_MANAGE_NO), 2)+1, \'FM099\')) \
	WHEN SUBSTR(MAX(CNNC_MANAGE_NO), 2)+1 > 100 THEN \
	CONCAT(\'O\', TO_CHAR(SUBSTR(MAX(CNNC_MANAGE_NO), 2)+1, \'FM999\')) \
	ELSE \'UNKNOWN\' END AS CNNC_MANAGE_NO FROM TB_META_0001 WHERE CNNC_MANAGE_NO LIKE \'O%\'")
	init_cursor.rowfactory = lambda *args: "".join(args)
	CNNC_MANAGE_NO = "".join(init_cursor.fetchall())

	meta_dict=defaultdict(list,{ key:[] for key in colum_list })
	meta_dict['CNNC_MANAGE_NO'] = CNNC_MANAGE_NO
	meta_dict['SYS_NM'] = 'Unknown'
	meta_dict['CNTC_MTHD_CODE'] = '01'
	meta_dict['CNTC_MTHD_NM'] = 'JDBC'
	meta_dict['DB_TY_CODE'] = '01'
	meta_dict['DB_TY_NM'] = '오라클'
	meta_dict['DB_ACNT_NM'] = schema
	# meta_dict['HIVE_DB_NM'] =
	meta_dict['DB_1_SERVICE_NM'] = sid
	meta_dict['DB_1_SERVER_IP'] = ip
	meta_dict['DB_1_SERVER_PORT_NO'] = port
	# meta_dict['DB_2_SERVER_IP'] =
	# meta_dict['DB_2_SERVER_PORT_NO'] =
	meta_dict['DB_USER_ID'] = user
	meta_dict['DB_USER_SECRET_NO'] = password
	# meta_dict['REMOTE_SERVER_IP'] =
	# meta_dict['REMOTE_SERVER_PORT_NO'] =
	# meta_dict['REMOTE_SERVER_USER_ID'] =
	# meta_dict['REMOTE_SERVER_USER_SECRET_NO'] =
	# meta_dict['REMOTE_DRCTRY_NM'] =
	# meta_dict['DATA_TY_CODE'] =
	# meta_dict['API_DATA_AUTHKEY_NM'] =
	# meta_dict['API_DATA_URL'] =
	meta_dict['NTWK_SE_CODE'] = '1'
	meta_dict['NTWK_SE_NM'] = '업무망'
	meta_dict['APPLC_SE_CODE'] = '01' 
	meta_dict['APPLC_SE_NM'] = '적용'
	# meta_dict['REGIST_DE'] =
	# meta_dict['REGIST_EMPL_NO'] =
	# meta_dict['UPDT_DE'] =
	# meta_dict['UPDT_EMPL_NO'] =

	for key, value in meta_dict.items():
		if not value and value is not None:
			if value == 0:
				meta_dict[key] = value
			else:
				meta_dict[key] = ", ".join(value)
		elif value is None:
			meta_dict[key] = ''
	
	elements = re.sub('dict_values|\[|\]', '', str(meta_dict.values()))
	# Write to Query File
	query_fp.write("INSERT INTO TB_META_0001 VALUES "+ elements + ";\n")
	# Write to CSV file
	writer.writerow(meta_dict.values())
	
	meta_dict.clear()
	fp.close()
	query_fp.close()

	################################################ TB_META_0002 ################################################
	fp = open("TB_META_0002.csv", "w", newline='', encoding='utf-8-sig')
	writer = csv.writer(fp)

	query_fp = open("TB_META_0002_query.txt", "w", encoding='utf-8-sig')

	init_cursor.execute(f"SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS \
		WHERE TABLE_NAME=\'TB_META_0002\' AND OWNER=\'EWPBIGDATA\'")
	init_cursor.rowfactory = lambda *args: "".join(args)
	colum_list = init_cursor.fetchall()
	writer.writerow(colum_list)

	meta_cursor.execute( f"SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER=\'{schema}\' ORDER BY TABLE_NAME" )
	meta_cursor.rowfactory = lambda *args: "".join(args)
	table_list = meta_cursor.fetchall()

	for table_name in table_list:
		meta_cursor.execute(f"SELECT T1.OWNER || '_' || T1.TABLE_NAME AS DB_TABLE_ID \
		, T1.TABLE_NAME AS TABLE_ENG_NM \
		, T1.COMMENTS AS TABLE_DC \
		, (T1.COMMENTS||' '||(SELECT LISTAGG(COMMENTS, ',') \
		WITHIN GROUP(ORDER BY COMMENTS) \
		FROM ALL_COL_COMMENTS \
		WHERE TABLE_NAME=T1.TABLE_NAME)) AS HASHTAG_CN \
		FROM ALL_TAB_COMMENTS T1 WHERE TABLE_NAME=\'{table_name}\'")
		meta_cursor.rowfactory = lambda *args: dict(zip([column[0] for column in meta_cursor.description], args))
		
		# TABLE_DC, TABLE_ENG_NAME, HASTAG_CN
		table_info = meta_cursor.fetchone()
		
		meta_dict=defaultdict(list,{ key:[] for key in colum_list })
		meta_dict['CNNC_MANAGE_NO'] = CNNC_MANAGE_NO
		meta_dict['DB_TABLE_ID'] = table_info['DB_TABLE_ID']
		meta_dict['TABLE_ENG_NM'] = table_info['TABLE_ENG_NM']
		# meta_dict['TABLE_KOREAN_NM'] =
		meta_dict['TABLE_DC'] = table_info['TABLE_DC']
		# meta_dict['BR_DC'] =
		meta_dict['HASHTAG_CN'] = table_info['HASHTAG_CN']
		# meta_dict['MNGR_NM'] =
		# meta_dict['WHERE_INFO_NM'] =
		# meta_dict['INS_NUM_MAPPERS'] =
		# meta_dict['INS_SPLIT_BY_COL'] =
		# meta_dict['APD_WHERE'] =
		# meta_dict['APD_CHK_COL'] =
		# meta_dict['APD_LAST_VAL'] =
		# meta_dict['HIVE_TABLE_NM'] =
		# meta_dict['ANAL_PRUSE_TRGET_AT'] =
		# meta_dict['BIGDATA_GTRN_AT'] =
		# meta_dict['SCHDUL_APPLC_AT'] =
		# meta_dict['GTHNLDN_MTH_CODE'] =
		# meta_dict['GTHNLDN_MTH_NM'] =
		# meta_dict['REGIST_DE'] =
		# meta_dict['REGIST_EMPL_NO'] =
		# meta_dict['UPDT_DE'] =
		# meta_dict['UPDT_EMPL_NO'] =

		for key, value in meta_dict.items():
			if not value and value is not None:
				if value == 0:
					meta_dict[key] = value
				else:
					meta_dict[key] = ", ".join(value)
			elif value is None:
				meta_dict[key] = ''

		elements = re.sub('dict_values|\[|\]', '', str(meta_dict.values()))
		# Write to Query File
		query_fp.write("INSERT INTO TB_META_0002 VALUES "+ elements + ";\n")
		# Write to CSV file
		writer.writerow(meta_dict.values())
		meta_dict.clear()
	
	fp.close()
	query_fp.close()

	################################################ TB_META_0003 ################################################
	fp = open("TB_META_0003.csv", "w", newline='', encoding='utf-8-sig')
	writer = csv.writer(fp)

	query_fp = open("TB_META_0003_query.txt", "w", encoding='utf-8-sig')

	init_cursor.execute(f"SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS \
		WHERE TABLE_NAME=\'TB_META_0003\' AND OWNER=\'EWPBIGDATA\'")
	init_cursor.rowfactory = lambda *args: "".join(args)
	colum_list = init_cursor.fetchall()
	writer.writerow(colum_list)

	for table_name in table_list:
		meta_cursor.execute(f"SELECT DISTINCT T1.OWNER || '_' || T1.TABLE_NAME AS DB_TABLE_ID \
		, T1.COLUMN_ID AS DB_TABLE_ATRB_SN \
		, T1.COLUMN_NAME AS TABLE_ATRB_ENG_NM \
		, T2.COMMENTS AS TABLE_KOREAN_ATRB_NM \
		, T2.COMMENTS AS TABLE_ATRB_DC \
		, T1.DATA_TYPE AS TABLE_ATRB_TY_NM \
		, T1.DATA_LENGTH AS TABLE_ATRB_LT_VALUE \
		, T1.DATA_PRECISION AS TABLE_ATRB_PRECISION \
		, T1.DATA_SCALE AS TABLE_ATRB_SCALE \
		, T1.NULLABLE AS TABLE_ATRB_NULL_POSBL_AT \
		, DECODE(T3.ENABLED, 'Y', 'Y', 'N') AS TABLE_ATRB_PK_AT \
		, DECODE(T1.DATA_TYPE, 'DATE', 'Y', 'N') AS DATE_YN \
		FROM ALL_TAB_COLUMNS T1 \
		JOIN ALL_COL_COMMENTS T2 \
		ON T1.COLUMN_NAME=T2.COLUMN_NAME AND T1.TABLE_NAME=T2.TABLE_NAME AND T1.OWNER=T2.OWNER \
		LEFT OUTER JOIN (SELECT B.COLUMN_NAME, A.TABLE_NAME, \
		DECODE(A.STATUS, 'ENABLED', 'Y', '') ENABLED,A.CONSTRAINT_TYPE \
		FROM ALL_CONSTRAINTS A, ALL_CONS_COLUMNS B \
		WHERE A.CONSTRAINT_TYPE IN ('P') \
		AND A.TABLE_NAME = B.TABLE_NAME \
		AND A.CONSTRAINT_NAME = B.CONSTRAINT_NAME \
		AND A.TABLE_NAME=\'{table_name}\' \
		AND A.OWNER=\'{schema}\' ) T3 \
		ON T1.COLUMN_NAME=T3.COLUMN_NAME AND T1.TABLE_NAME=T3.TABLE_NAME \
		WHERE T1.OWNER=\'{schema}\' AND T1.TABLE_NAME=\'{table_name}\' \
		ORDER BY T1.COLUMN_ID")
		meta_cursor.rowfactory = lambda *args: dict(zip([column[0] for column in meta_cursor.description], args))
		table_columns = meta_cursor.fetchall()

		for table_column in table_columns:
			meta_dict=defaultdict(list,{ key:[] for key in colum_list })
			meta_dict['DB_TABLE_ID'] = table_column['DB_TABLE_ID']
			meta_dict['DB_TABLE_ATRB_SN'] = table_column['DB_TABLE_ATRB_SN']
			meta_dict['TABLE_ATRB_ENG_NM'] = table_column['TABLE_ATRB_ENG_NM']
			meta_dict['TABLE_KOREAN_ATRB_NM'] = table_column['TABLE_KOREAN_ATRB_NM']
			meta_dict['TABLE_ATRB_DC'] = table_column['TABLE_ATRB_DC']
			meta_dict['DSTNG_TRGET_AT'] = 'N' # default
			meta_dict['TABLE_ATRB_TY_NM'] = table_column['TABLE_ATRB_TY_NM']
			meta_dict['TABLE_ATRB_LT_VALUE'] = table_column['TABLE_ATRB_LT_VALUE']
			meta_dict['TABLE_ATRB_PRECISION'] = table_column['TABLE_ATRB_PRECISION']
			meta_dict['TABLE_ATRB_SCALE'] = table_column['TABLE_ATRB_SCALE']
			meta_dict['TABLE_ATRB_NULL_POSBL_AT'] = table_column['TABLE_ATRB_NULL_POSBL_AT']
			meta_dict['TABLE_ATRB_PK_AT'] = table_column['TABLE_ATRB_PK_AT']
			# meta_dict['REGIST_DE'] =
			# meta_dict['REGIST_EMPL_NO'] =
			# meta_dict['UPDT_DE'] =
			# meta_dict['UPDT_EMPL_NO'] =
			meta_dict['DATE_YN'] = table_column['DATE_YN']

			for key, value in meta_dict.items():
				if not value and value is not None:
					if value == 0:
						meta_dict[key] = value
					else:
						meta_dict[key] = ", ".join(value)
				elif value is None:
					meta_dict[key] = ''
			
			elements = re.sub('dict_values|\[|\]', '', str(meta_dict.values()))
			# Write to Query File
			query_fp.write("INSERT INTO TB_META_0003 VALUES "+ elements + ";\n")
			# Write to CSV file
			writer.writerow(meta_dict.values())
			meta_dict.clear()

	fp.close()
	query_fp.close()
	
	# step 4) Connection Close
	init_cursor.close()
	init_conn.close()
	meta_cursor.close()
	meta_conn.close()

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-id', dest="id", help="user_id", required=True)
	parser.add_argument('-pw', dest="pw", help="user_password", required=True)
	parser.add_argument('-host', dest="host", help="host_name", required=True)
	parser.add_argument('-p', dest="port", help="port_number", required=True)
	parser.add_argument('-s', dest="sid", help="service_name", required=True)
	parser.add_argument('-schema', dest="schema", help="schema_name", required=True)
	args = parser.parse_args()

	run(args.id, args.pw, args.host, args.port, args.sid, args.schema)







