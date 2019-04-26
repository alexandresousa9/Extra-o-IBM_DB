#!/usr/bin/python
# -*- coding: ascii -*-

import ibm_db
import sys
import xlsxwriter
import ipdb
import string
import datetime
import os
import csv

#Informacoes sobre o data base do BI de pecas e servicos
database = 'DB'
hostname = 'HOSTNAME'
port = 'PORT'
protocol = 'TCPIP'
userid = 'LOGIN'
password = 'PASSWORD'


# mypath should be the complete path for the directory containing the input text files
mypath = 'c:/Users/santal1/Desktop/extracao/'


path = os.path.dirname(os.path.abspath(__file__))

arquivo = open(path + '/tbl_PRO540P_V30.csv')
linhas = csv.reader(arquivo, delimiter=';')
tab_v30 = []
for linha in linhas:
	tab_v30.append([linha[0] + linha[1], linha[2]])

#Realiza a conexao com o banco de dados, caso seja bem sucedido a mensagem
#'CONNECTED! ...' e exibida, caso contrario: 'Connection fault with DATABASE!...'
def conectaDB2():
	try:
		conn = ibm_db.pconnect("DATABASE="+database+"; HOSTNAME="+hostname+"; PORT="+port+"; PROTOCOL="+protocol+"; UID="+userid+"; PWD="+password+";", '', '')
	except :
		print('Connection fault with DATABASE!\nError Code: {0}'.format(ibm_db.conn_error()))
		sys.exit()
		
	info = ibm_db.client_info(conn)
	print('CONNECTED! DATA_SOURCE_NAME: {0} CONN_CODEPAGE: {3} DRIVER_NAME: {1} DRIVER_ODBC_VER: {2} ODBC_VER: {4}'.format(info.DATA_SOURCE_NAME, info.DRIVER_NAME, info.DRIVER_ODBC_VER, info.CONN_CODEPAGE, info.ODBC_VER))
	return conn
	
#Se a conexao com o banco de dados estiver ativa, a funcao encerra a conexao 
#e exibe a mensagem 'Connection Terminated With Success!', caso contrario nada
#feito.
def encerraConexao(conn):
	if(ibm_db.close(conn) != False):
		print('Connection Terminated With Success!')

#Consulta do arquivo da pasta(mypath) e o nome do arquivo(fileName).	
def preparaSQL(mypath, fileName):

	mypath = mypath + fileName

	file = open(mypath, 'r')

	codSQL = ""

	for line in file:     # separate fields by commas
		codSQL += line.strip()

	file.close()

	return codSQL

#Monta a query com base nas informacoes passadas como paramentro.
def preparaSQL_Data(month, year):
	sql = "SELECT R.REG_TS_INCLUSAO, R.REG_NU_CONTA_CONCES, R.REG_CO_CANAL_VENDA, VARCHAR(VARCHAR_FORMAT((R.REG_DA_NOTA),'DDMMYYYY'), 8) AS REG_DATANF, R.REG_NU_NOTA, R.REG_CO_CNPJ, R.REG_CO_IBGE_CIDADE, CASE WHEN R.REG_CLA_PECA = '4' THEN 'QLANCMKT100 003      ' WHEN R.REG_CLA_PECA = '5' THEN 'QLANCMKT100 004      '	WHEN R.REG_CLA_PECA = '6' THEN 'QLANCMKT100 005      ' WHEN R.REG_CLA_PECA = '7' THEN 'QLANCMKT100 006      ' WHEN (LEFT(R.REG_NU_ITEM,'3') != 'MOO' AND LEFT(R.REG_NU_ITEM,'3') != 'MOF' AND R.REG_CLA_PECA = '8') THEN 'QLANCMKT100 007      ' WHEN (LEFT(R.REG_NU_ITEM,'3') = 'MOO' AND (R.REG_CLA_PECA = '8' OR R.REG_CLA_PECA = '99')) THEN 'QLANCMKT100 001      ' WHEN (LEFT(R.REG_NU_ITEM,'3') = 'MOF' AND (R.REG_CLA_PECA = '8' OR R.REG_CLA_PECA = '99')) THEN 'QLANCMKT100 002      ' WHEN R.REG_NU_ITEM LIKE 'W9305890023  0000%' THEN 'W  93058900230000    ' WHEN LEFT(R.REG_NU_ITEM,'1') = 'A' AND (R.REG_CLA_PECA = '1' OR R.REG_CLA_PECA = '2') AND LEFT(R.REG_NU_ITEM,'3') != 'A  ' THEN CONCAT(CONCAT(LEFT(R.REG_NU_ITEM,'1'),'  '), SUBSTR(R.REG_NU_ITEM,'2','18'))	ELSE R.REG_NU_ITEM END AS REG_ITEM, RTRIM(R.REG_CLA_PECA) AS REG_FLAGITEM,CASE WHEN (R.REG_CO_VIN = VEICVT.CHASSIS) THEN R.REG_CO_VIN ELSE '-1               ' END AS REG_CHASSIS, CASE WHEN (R.REG_CO_VIN = VEICVT.CHASSIS) THEN VEICVT.VAKCOVERS ELSE '-1                  ' END AS REG_AGRUPVEICULO, R.REG_QT_VENDA, R.REG_VA_VENDA, R.REG_CO_VENDA, LEFT(R.REG_NO_CLIENTE,'60') AS REG_NOMECLIENTE, R.REG_TI_PESSOA FROM ATPSC540.ATPTB_REGISTROS AS R LEFT JOIN (SELECT VEA.COVIN AS CHASSIS, VAK.COVERS AS VAKCOVERS FROM PRO540P.VEA AS VEA LEFT JOIN PRO540P.VAK AS VAK ON VEA.COBAUM = VAK.COBAUM WHERE VEA.COVIN > '0' AND VEA.NUNFIS > '0' ORDER BY 1) AS VEICVT ON R.REG_CO_VIN = VEICVT.CHASSIS WHERE YEAR(R.REG_DA_NOTA) = " + str(year) + "	AND MONTH(R.REG_DA_NOTA) = " + str(month) + " ORDER BY R.REG_TS_INCLUSAO WITH UR"
	return sql
	
#Consulta Banco de dados com os seguintes paramatros: codigo SQL (sql) e a conexao do banco (conn).
def consultaDB2(sql, conn):
	try:
		stmt = ibm_db.exec_immediate(conn, sql)
	except :
		print('Code SQL with problem!\nError Code: {0}'.format(ibm_db.stmt_error()))
		sys.exit()

	#print('Number of Fields: {0}'.format(ibm_db.num_fields(stmt)))

	print('Number of rows deleted, inserted, or updated: {0}'.format(ibm_db.num_rows(stmt)))


	return stmt

#Salva consulta realizada pela funcao 'conectaDB2' em txt, usando o caracter como separador de campos. 
def salvaTXT(stmt, mypath):
	
	file = open(mypath, 'w')
	
	#Returns a tuple, which is indexed by column position, representing a row in a result set. The columns are 0-indexed.
	wdict = ibm_db.fetch_tuple(stmt)
	
	num_rows = 0
	
	while wdict != False:
		data = []
		data.append(wdict)
		
		lineFormat = ''
		colAtual = 1
		
		numCol = len(data[0])
		linha = data[0]
		
		for col in linha:
			
			if colAtual == 6:
				
				for lin in tab_v30:
					if col == lin[0]:
						col = lin[1]

			if  str(col) != "None":
				if numCol != colAtual:
					lineFormat += str(col) + chr(221)
				else: 
					lineFormat += str(col)
			elif numCol != colAtual:
				lineFormat += chr(221)
			colAtual+=1

		file.write(lineFormat + '\n')

		wdict = ibm_db.fetch_tuple(stmt)
	
		num_rows += 1

	file.close()
	print('Number of rows found: {0}'.format(num_rows))

#Executa todas as querys dos arquivos da pasta mypath.
def ConsultaDiversa():

	inputFiles = os.listdir(mypath)

	for namefile in inputFiles:
		
		print("Extraindo o arquivo: ", namefile)
		
		#Executa a funcao 'preparaSQL' recebendo como paramentros: caminho e nome do arquivo. 
		sql = preparaSQL(mypath, namefile)
		
		stmt = consultaDB2(sql, conn)

		salvaTXT(stmt, 'C:\consultas\ ' + namefile)

#Salva consulta realizada na planilha de EXCEL.
def salvaXLSX(month, year, conn):
	
	#Inclui na primeira linha o layout abaixo
	layout = ['REG_TS_INCLUSAO',	'REG_NU_CONTA_CONCES',	'REG_CO_CANAL_VENDA',	'REG_DATANF',	'REG_NU_NOTA',	'REG_CO_CNPJ',	'REG_CO_IBGE_CIDADE',	'REG_ITEM',	'REG_FLAGITEM',	'REG_CHASSIS',	'REG_AGRUPVEICULO',	'REG_QT_VENDA',	'REG_VA_VENDA',	'REG_CO_VENDA',	'REG_NOMECLIENTE',	'REG_TI_PESSOA']

	nameXLSX = 'Registros ' + str(month) + '-' + str(year) + '.xlsx'
	
	# Create workbook and worksheet 
	wbk = xlsxwriter.Workbook(nameXLSX) 
	sheet = wbk.add_worksheet()
	
	col = 0
	lin = 1
	for campo in layout:
		sheet.write(0, col, campo)
		col+=1
	
	sql = preparaSQL_Data(month, year)

	stmt = consultaDB2(sql, conn)

	wdict = ibm_db.fetch_tuple(stmt)
	
	while wdict != False:
		data = []
		data.append(wdict)	
	
		col = 0
		lista = data[0]
		
		for value in lista:     # separate fields by commas
			
			if type(value) is datetime.datetime:
				valor = str(value)
#				valor.encode("ascii")
				sheet.write(lin, col, valor)
			else:
#				value.encode("ascii")
				sheet.write(lin, col, value)
				
			col += 1

		wdict = ibm_db.fetch_tuple(stmt)
		
		lin += 1
	
	wbk.close()

#Executa uma unica consulta variando apenas as datas.
def ConsultaUnica():
	for year in range(2017,2019):
		for month in range(1,13):
			print("Extraindo mes ", month, " do ano ", year)
			salvaXLSX(month, year, conn)

#Executa a funcao 'conectaDB2' e cria o objeto conn
conn = conectaDB2()

ConsultaDiversa()

#Executa a funcao 'encerraConexao' recebendo como paramentro o objeto 'conn'
encerraConexao(conn)
