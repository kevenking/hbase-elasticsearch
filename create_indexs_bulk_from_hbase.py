# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import re
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *
import datetime
import time
import hashlib

class ESIndexCreator:
	def __init__(self):
		reComments = re.compile('<!--[^>]*-->')
		reHtml = re.compile('</?\w+[^>]*>')

		self.host = "172.20.6.61"
		self.port = 9090
		self.transport = TBufferedTransport(TSocket(self.host, self.port))
		self.transport.open()
		self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
		self.client = Hbase.Client(self.protocol)
		self.es = Elasticsearch(
			['172.20.8.162'],
			sniff_on_start = True,
			sniff_on_connection_fail = True,
			sniffer_timeout = 60
		)

	def __del__(self):
		self.transport.close()

	def filterTags(self,htmlstr):  
		#先过滤CDATA  
		re_cdata=re.compile('//<!\[CDATA\[[^>]*//\]\]>',re.I) #匹配CDATA  
		re_script=re.compile('<\s*script[^>]*>[^<]*<\s*/\s*script\s*>',re.I)#Script  
		re_style=re.compile('<\s*style[^>]*>[^<]*<\s*/\s*style\s*>',re.I)#style  
		re_br=re.compile('<br\s*?/?>')#处理换行  
		re_h=re.compile('</?\w+[^>]*>')#HTML标签  
		re_comment=re.compile('<!--[^>]*-->')#HTML注释  
		s=re_cdata.sub('',htmlstr)#去掉CDATA  
		s=re_script.sub('',s) #去掉SCRIPT  
		s=re_style.sub('',s)#去掉style  
		s=re_br.sub('',s)#将br转换为换行  
		s=re_h.sub('',s) #去掉HTML 标签  
		s=re_comment.sub('',s)#去掉HTML注释  
		#去掉多余的空行  
		blank_line=re.compile('\n+')  
		s=blank_line.sub('',s)  
		return s

	def getAllTablesInfo(self):
		#get table info
		listTables = self.client.getTableNames()
		print "="*40
		print "Show all tables information...."

		for tableName in listTables:
			print "TableName:" + tableName
			print " "
			listColumns = self.client.getColumnDescriptors(tableName)
			print listColumns
			print " "

			listTableRegions = self.client.getTableRegions(tableName)
			print listTableRegions
			print "+"*40

	def deleteIndex(self,indexName):
		print "Remove index...."
		self.es.indices.delete(index=indexName,ignore=[400,404])

	def createIndex(self,indexName):
		print "Create index...."
		#create indexs
		self.es.indices.create(index=indexName)

		print "Define mapping...."
		#define mapping
		self.es.indices.put_mapping(
				index=indexName,
				doc_type="article",
				ignore_conflicts='true',
				body={
					"article":{
						"properties":{
							"sitename":{ "type":"string", "store":"true", "index":"not_analyzed" },
							"addtime":{"type":"date", "store":'true' },
							"publishtime":{"type":"date", "store":'true' },
							"keywords":{"type":"string", "store":'true',"analyzer":"ik" },
							"content":{"type":"string", "store":'true',"analyzer":"ik" },
							"url":{"type":"string", "store":'true', "index":"not_analyzed"},
							"title":{"type":"string","store":'true', "analyzer":"ik"}
							}
						}
					}
				)
		self.es.indices.put_mapping(
				index=indexName,
				doc_type="baidu",
				ignore_conflicts='true',
				body={
					"baidu":{
						"properties":{
							"sitename":{ "type":"string", "store":"true", "index":"not_analyzed" },
							"addtime":{"type":"date", "store":'true' },
							"publishtime":{"type":"date", "store":'true' },
							"keywords":{"type":"string", "store":'true',"analyzer":"ik" },
							"content":{"type":"string", "store":'true',"analyzer":"ik" },
							"url":{"type":"string", "store":'true', "index":"not_analyzed"},
							"title":{"type":"string","store":'true', "analyzer":"ik"}
							}
						}
					}
				)

		self.es.indices.put_mapping(
				index=indexName,
				doc_type="report",
				ignore_conflicts='true',
				body={
					"report":{
						"properties":{
							"sitename":{ "type":"string", "store":"true", "index":"not_analyzed" },
							"addtime":{"type":"date", "store":'true' },
							"publishtime":{"type":"date", "store":'true' },
							"infSource":{"type":"string", "store":'true',"index":"not_analyzed" },
							"url":{"type":"string", "store":'true', "index":"not_analyzed"},
							"title":{"type":"string","store":'true', "analyzer":"ik"}
							}
						}
					}
				)
		self.es.indices.put_mapping(
				index=indexName,
				doc_type="blog",
				ignore_conflicts='true',
				body={
					"blog":{
						"properties":{
							"sitename":{ "type":"string", "store":"true", "index":"not_analyzed" },
							"addtime":{"type":"date", "store":'true' },
							"author":{"type":"string", "store":'true', "index":"not_analyzed" },
							"content":{"type":"string", "store":'true',"analyzer":"ik" },
							"url":{"type":"string", "store":'true', "index":"not_analyzed"},
							"title":{"type":"string","store":'true', "analyzer":"ik"}
							}
						}
					}
				)
		self.es.indices.put_mapping(
				index=indexName,
				doc_type="weibo",
				ignore_conflicts='true',
				body={
					"weibo":{
						"properties":{
							"user_id":{ "type":"string", "store":"true"},
							"screen_name":{"type":"string", "store":'true', "index":"not_analyzed" },
							"content":{"type":"string", "store":'true',"analyzer":"ik" },
							"publishtime":{"type":"date", "store":'true' },
							"comments_count":{"type":"integer", "store":'true'},
							"reposts_count":{"type":"integer","store":'true'}
							}
						}
					}
				)


	def getHBaseList(self,tableName,listColumns):
		scanner = self.client.scannerOpen(tableName,'',listColumns,None)
		res = self.client.scannerGetList(scanner,100000)
		self.client.scannerClose(scanner)
		return res


	'''
	def createDataIndex(self,tableName,tableColumnFamily,tableColumnList,indexName,indexTypeName,indexColumnList):
		listColumns = []
		for key in range(len(tableColumnList)):
			listColumns.append(tableColumnFamily + ':' + tableColumnList[key])

		res = self.getHBaseList(tableName,listColumns)
		for i in res:
			body = {}
			for key in range(len(listColumns)):
				if indexColumnList[key] == 'addtime':
					body[indexColumnList[key]] = datetime.datetime.strptime(i.columns.get(listColumns[key]).value,'%Y-%m-%d %H:%M:%S')
				elif indexColumnList[key] == 'publishtime':
					if i.columns.get(listColumns[key]).value == '':
						body[indexColumnList[key]] = datetime.datetime.now()
					elif len((i.columns.get(listColumns[key]).value).strip()) <= 10:
						body[indexColumnList[key]] = datetime.datetime.strptime((i.columns.get(listColumns[key]).value).strip() + ' 00:00:00','%Y-%m-%d %H:%M:%S')
					else:
						body[indexColumnList[key]] = datetime.datetime.strptime(i.columns.get(listColumns[key]).value,'%Y-%m-%d %H:%M:%S')
				elif indexColumnList[key] == 'content':
					#body[indexColumnList[key]] = self.filterTags(i.columns.get(listColumns[key]).value).strip()
					body[indexColumnList[key]] = ''
				else:
					body[indexColumnList[key]] = i.columns.get(listColumns[key]).value

			self.es.index(index=indexName,doc_type=indexTypeName, body=body)

	'''


	def createOtherArticlesIndex(self,indexName):
		print "Create other articles index...."
		tableColumnList = ['siteName','publishTime','url','title','keyWords','content','addTime']
		indexColumnList = ['sitename','publishtime','url','title','keywords','content','addtime']
		self.createDataIndex('info_public_monitor','other_articles',tableColumnList,indexName,'article',indexColumnList)

	def createBaiduArticlesIndex(self,indexName):
		print "Create baidu articles index...."
		tableColumnList = ['siteName','publishTime','url','title','keyWords','content','addTime']
		indexColumnList = ['sitename','publishtime','url','title','keywords','content','addtime']
		self.createDataIndex('info_public_monitor','baidu_articles',tableColumnList,indexName,'baidu',indexColumnList)

	def createReportIndex(self,indexName):
		print "Create report index...."
		tableColumnList = ['siteName','publishTime','url','title','source','addTime']
		indexColumnList = ['sitename','publishtime','url','title','infsource','addtime']
		self.createDataIndex('info_public_monitor','report',tableColumnList,indexName,'report',indexColumnList)

	def createBlogIndex(self,indexName):
		print "Create blog index...."
		tableColumnList = ['siteName','author','url','title','content','addTime']
		indexColumnList = ['sitename','author','url','title','content','addtime']
		self.createDataIndex('info_public_monitor','blog',tableColumnList,indexName,'blog',indexColumnList)

	def createWeiboIndex(self,indexName):
		print "Create weibo index...."
		tableColumnList = ['user_id','created_at','content','screen_name','comments_count','reposts_count']
		indexColumnList = ['user_id','publishtime','content','screen_name','comments_count','reposts_count']
		self.createDataIndex('info_public_monitor','weibo',tableColumnList,indexName,'weibo',indexColumnList)

	def createActivityIndex(self,indexName):
		print "Create activity index...."
		tableColumnList = ['activityID','addTime','keyWords','location','siteName','time','title','trad','url']
		indexColumnList = ['activityid','addtime','keywords','location','sitename','time','title','trad','url']
		self.createDataIndex('info_public_monitor','activity',tableColumnList,indexName,'activity',indexColumnList)

	def createAllIndex(self,indexName):
		self.deleteIndex(indexName)
		self.createIndex(indexName)
		#self.createBaiduArticlesIndex(indexName)
		self.createOtherArticlesIndex(indexName)
		#self.createReportIndex(indexName)
		#self.createBlogIndex(indexName)
		#self.createWeiboIndex(indexName)
		#self.createActivityIndex(indexName)

	def createDataIndex(self,tableName,tableColumnFamily,tableColumnList,indexName,indexTypeName,indexColumnList):
		listColumns = []
		for key in range(len(tableColumnList)):
			listColumns.append(tableColumnFamily + ':' + tableColumnList[key])

		scannerId = self.client.scannerOpen(tableName, '', listColumns, None)
		actions = []
		while True:
			try:
				results = self.client.scannerGet(scannerId)
				i = results[0]
			except:
				break
			
			body = {}
			for key in range(len(listColumns)):
				if indexColumnList[key] == 'addtime':
					body[indexColumnList[key]] = datetime.datetime.strptime(i.columns.get(listColumns[key]).value,'%Y-%m-%d %H:%M:%S')
				elif indexColumnList[key] == 'publishtime':
					if i.columns.get(listColumns[key]).value == '':
						body[indexColumnList[key]] = datetime.datetime.now()
					elif len((i.columns.get(listColumns[key]).value).strip()) <= 10:
						body[indexColumnList[key]] = datetime.datetime.strptime((i.columns.get(listColumns[key]).value).strip() + ' 00:00:00','%Y-%m-%d %H:%M:%S')
					else:
						body[indexColumnList[key]] = datetime.datetime.strptime(i.columns.get(listColumns[key]).value,'%Y-%m-%d %H:%M:%S')
				elif indexColumnList[key] == 'content':
					body[indexColumnList[key]] = self.filterTags(i.columns.get(listColumns[key]).value).strip()
				else:
					body[indexColumnList[key]] = i.columns.get(listColumns[key]).value

			#self.es.index(index=indexName,doc_type=indexTypeName, body=body)
			action = {
				'_index':indexName,
				'_type':indexTypeName,
				'_source':body
			}

			actions.append(action)
			if(len(actions) == 500):
				print 'creating indexes....'
				helpers.bulk(self.es,actions)
				del actions[0:len(actions)]
				

		if(len(actions) > 0):
			helpers.bulk(self.es, actions)
			del actions[0:len(actions)]
		self.client.scannerClose(scannerId)


def main():
	print "="*40
	print datetime.datetime.now()
	print " "
	
	ic = ESIndexCreator()
	ic.createAllIndex('web-articles')
	#ic.getAllTablesInfo()

if __name__ == "__main__":
	main()


