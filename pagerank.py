#-*- coding: UTF-8 -*-
import numpy
import nltk
from scipy import array
import re
import math
import os
import os.path
import pyodbc
import networkx as nx
import time
import compileall
import datetime
import sys
import en.verb
from nltk.stem import WordNetLemmatizer

lemmatizer = WordNetLemmatizer()

lowquality_en = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now']
stopwords_en = ['to','be','do','have','I','a','an','the','and','s','t','can','will','don','should','.',',','?',':','(',')','[',']','"',"'",';','``','=','`','!','...']

g_Conn = None
def openDB():
    global g_Conn
    try:
        g_Conn = pyodbc.connect('Trusted_Connection=True',DRIVER='SQL Server',SERVER ='cia-sh-03', database ='ContentImpact')
        print 'connect success'
    except:
        print 'connect fail'

def closeDB():
    global g_Conn
    g_Conn.close()

def query(sql):
    rows = []
    try:
        openDB()
        global g_Conn
        cursor = g_Conn.cursor()
        ret = cursor.execute(sql)
        rows = cursor.fetchall()
        return rows
    except Exception, ex:
        print '%s:%s' % (Exception, ex)
        raise Exception, 'query database has wrong'
    finally:
        closeDB()

def insert_bulk(sql,paramformat,params):
    try:
        openDB()
        global g_Conn
        cursor = g_Conn.cursor()
        i = 0
        for param in params:
            if(i < 999):
                if(i == 0):
                    sql_temp = sql + " " + paramformat % param
                else:
                    sql_temp += "," + paramformat % param
                i += 1
            else:
                i = 0
                sql_temp += "," + paramformat % param
                cursor.execute(sql_temp)
        cursor.execute(sql_temp)
        g_Conn.commit()
        return True
    except Exception, ex:
        print '%s:%s' % (Exception, ex)
        g_Conn.rollback()
        raise Exception, 'insert database has wrong'
        return False
    finally:
        closeDB()

def insert_many(sql, params):
    '''
    新增多条记录，
    params:
        sql:执行的sql语句
        parmas:一个list，listitem为一个参数元组
    '''
    try:
        openDB()
        global g_Conn
        cursor = g_Conn.cursor()
        for param in params:
            sql_temp = sql % param
            try:
                cursor.execute(sql_temp)
            except:
                continue
        g_Conn.commit()
        return True
    except Exception,ex:
        print '%s:%s' % (Exception, ex)
        g_Conn.rollback()
        raise Exception, 'insert database has wrong'
        return False
    finally:
        closeDB()

def exesql(sql):
    try:
        openDB()
        global g_Conn
        cursor = g_Conn.cursor()
        cursor.execute(sql)
        g_Conn.commit()
        return True
    except:
        g_Conn.rollback()
        raise Exception, 'execute sql has wrong'
        return False
    finally:
        closeDB()

def highlight(main_sent,cpr_sent):
    highlightset = ""
    tempwordsi = nltk.word_tokenize(main_sent.lower())
    for m in range(0,len(tempwordsi)):
        try:
            tempwordsi[m] = lemmatizer.lemmatize(tempwordsi[m])
            tempwordsi[m] = verb.present(tempwordsi[m])
        except:
            continue
    tempwordsi = [i1 for i1 in tempwordsi if i1 not in lowquality_en and i1 not in stopwords_en ]

    tempwordsj = nltk.word_tokenize(cpr_sent)
    for m in range(0,len(tempwordsj)):
        try:
            word = tempwordsj[m].lower()
            word = lemmatizer.lemmatize(word)
            word = verb.present(word)
        except:
            pass
        if word in tempwordsi:
            highlightset+='%s ' % tempwordsj[m]

    return highlightset


def main():

    #culturekey = 55

    start = time.clock()
    now = datetime.datetime.now()

    SentenseTable = "[VerbatimSentence]"
    SimilarityTable = "[Similarity]"
    ResultTable = "[VerbatimComplains]"

    similarity_threshold = 0.4         ### parameter


    """sql_delete = "delete from [dbo]." + ResultTable
    print exesql(sql_delete)


    category = sys.argv[1]  # arguments[-category (kb/cluster) -kbid/clustername -culturekey -starttime
                        # -endtime]
    culturekey = sys.argv[3]
    starttime = sys.argv[4]
    endtime = sys.argv[5]

    if(category == "kb"):
        kbid = sys.argv[2]
        sql_sent = "SELECT [Identity],SentenceText FROM [ContentImpact].[dbo].[VerbatimSentence] WHERE ContentKey in (SELECT ContentKey FROM [ContentImpact].[dbo].[VW_DimContent]  where CultureKey = " + culturekey + " and KBID = '" + kbid + "') and DateKey > '" + starttime + "' and DateKey < '" + endtime + "'" + " order by [Identity]"
        sql = "SELECT [Identity],SentenceText FROM [ContentImpact].[dbo].[VerbatimSentence] WHERE ContentKey in (SELECT ContentKey FROM [ContentImpact].[dbo].[VW_DimContent]  where CultureKey = " + culturekey + " and KBID = '" + kbid + "') and DateKey > '" + starttime + "' and DateKey < '" + endtime + "'"

    if (category == "cluster"):
        clustername = sys.argv[2]
        sql_sent = "select [Identity],SentenceText FROM [ContentImpact].[dbo].[VerbatimSentence] WHERE ContentKey in (SELECT ContentKey FROM [ContentImpact].[dbo].[VW_DimContent] where ClusterName = '" + clustername + "' and CultureKey = " + culturekey + ")  and DateKey > '" + starttime + "' and DateKey < '" + endtime + "'" + " order by [Identity]"
        sql = "select [Identity],SentenceText FROM [ContentImpact].[dbo].[VerbatimSentence] WHERE ContentKey in (SELECT ContentKey FROM [ContentImpact].[dbo].[VW_DimContent] where ClusterName = '" + clustername + "' and CultureKey = " + culturekey + ")  and DateKey > '" + starttime + "' and DateKey < '" + endtime + "'"

    print sys.argv[2]
    print culturekey
    print starttime
    print endtime"""

    #sql = "SELECT [Identity],SentenceText FROM
    #[ContentImpact].[dbo].[VerbatimSentence] WHERE ContentKey = '" + kbid + "'
    #and DateKey > '" + starttime + "' and DateKey < '" + endtime + "'"
    #sql_similar = "WITH CTE AS(" + sql + ") SELECT Source,Target,Similarity
    #FROM [ContentImpact].[dbo].[Similarity] A JOIN CTE B ON
    #A.Source=B.[Identity] JOIN CTE C ON A.Target=C.[Identity] where Similarity
    #>0"

    sql_sent = "SELECT [Identity],[SentenceText] FROM [ContentImpact].[dbo].[VerbatimSentence] where ContentKey=2853047"
    sql_similar = "SELECT [Source],[Target],[Similarity] FROM [ContentImpact].[dbo].[Similarity_20141120_SingleKB] where Source in (SELECT [Identity] FROM [ContentImpact].[dbo].[VerbatimSentence] where ContentKey='2853047' ) and Target in(SELECT [Identity] FROM [ContentImpact].[dbo].[VerbatimSentence] where ContentKey='2853047')"
    sents = query(sql_sent)
    similarity = query(sql_similar)
    #endtime="20141013"
    #starttime="20140816"
    #kbid="5520810"
    #sql_sent = "SELECT [Identity],SentenceText FROM
    #[ContentImpact].[dbo].[VerbatimSentence] WHERE " + endtime + " = DateKey
    #AND " + starttime + " > 20100901 and ContentKey = '" + kbid + "' order by
    #[Identity]"

    #sql= "SELECT [Identity],SentenceText FROM
    #[ContentImpact].[dbo].[VerbatimSentence] WHERE " + endtime + " = DateKey
    #AND " + starttime + " > 20100901 and ContentKey = '" + kbid + "'"

    print "sents:"  , len(sents)
    print "similarity:" , len(similarity)

    dict = {}
    for item in sents:
        dict[item[0]] = item[1]

    dict2 = {}
    for item in similarity:
        i = int(min(item[0],item[1]))
        j = int(max(item[0],item[1]))
        dict2[(i,j)] = item[2]




    max_num = 0              ## class number , =len(resultsents)
    G = nx.Graph()
    for item in similarity:
        G.add_edge(int(item[0]),int(item[1]),weight=float(item[2]))
    pr = nx.pagerank(G,alpha=0.85)
    pr = sorted(pr.items(), key=lambda d:d[1], reverse=True)  #sorted by value desc
    pr1 = []
    classresult = []
    for i,(id_i,pagerank_i) in enumerate(pr):
        flag=0
        for j,(id_j) in enumerate(classresult):
            s = int(min(id_i,id_j))
            t = int(max(id_i,id_j))
            try:
                if dict2[(s,t)] >= similarity_threshold :
                    pr1.append((id_i,j,0,highlight(dict[id_i],dict[id_j]),now))
                    flag=1
                    break
                else:
                    continue
            except:
                continue
        if (flag == 0):                     #new class
            classresult.append(id_i)
            pr1.append((id_i,max_num,1,"",now))
            max_num+=1


    sql_insert = "insert into [dbo]." + ResultTable + "(SentenceID,Class,IsMainSentence,HighlightWord,RowModifiedDate) VALUES "
    #print insert_bulk(sql_insert,"(%d,%d,%d,'%s',CAST('%s' AS DATETIME2))",pr1)
    print pr1[:20]




main()

