# -*- coding: utf-8 -*-
import numpy
import nltk
from scipy import array
import verb
import re
import math
import os.path
import pyodbc
import compileall
#import pymssql
import time
import datetime
from nltk.stem import WordNetLemmatizer
import threading

lemmatizer = WordNetLemmatizer()
lowquality_en = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now']
stopwords_en = ['to','be','do','have','I','a','an','the','and','s','t','can','will','don','should','.',',','?',':','(',')','[',']','"',"'",';','``','=','`','!','...']
txt_path = "C:\\sql\\similarity.txt"
sql = "bulk insert [dbo].[similarity_GDR_en-us] From '" + txt_path +"' With (FieldTerminator = ',',RowTerminator = '\n')"
g_Conn = None


def openDB():
    global g_Conn
    try:
        #g_Conn = pyodbc.connect('Trusted_Connection=True',DRIVER='SQL
        #Server',SERVER ='cia-sh-03', database ='ContentImpact')
        #g_Conn =pyodbc.connect('DRIVER={SQL Server};SERVER=localhost;DATABASE=ms;UID=sa;PWD=zsh921023;')
        g_Conn = pyodbc.connect('Trusted_Connection=True',DRIVER='SQL Server',SERVER ='cia-sh-03', database ='ContentImpact')
        return g_Conn
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
    finally:
        closeDB()


def bulk_sql(sql):
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


def exesql(sql):
    try:
        #conn = openDB()
        global g_Conn
        cursor = g_Conn.cursor()
        #cursor = conn.cursor()
        #cursor.execute(sql)
        g_Conn.commit()
        return True
    except:
        g_Conn.rollback()
        raise Exception, 'execute sql has wrong'
        return False
    finally:
        closeDB()


def preprocess_sents(sents):
    #start = time.clock()

    result = []                    ## to save nonzero similarity between
                                   ## sents:(source_id,target_id,similarity)
    #sents2 = sents

    for i in range(len(sents)):
        ID_i,Text_i = sents[i][0],sents[i][1]
        tempwordsi1 = nltk.word_tokenize(Text_i.lower())
        for m in range(0,len(tempwordsi1)):
            try:
                tempwordsi1[m] = lemmatizer.lemmatize(tempwordsi1[m])
                tempwordsi1[m] = verb.present(tempwordsi1[m])         #change noun to singular , keep other words unchanged
            except:
                continue
        tempwordsi2 = [word for word in tempwordsi1 if word not in stopwords_en]

        sents[i] = (ID_i,tempwordsi1,tempwordsi2)
    #for i in range(len(sents)):
    #    cal_oneloop(i,sents)

    return sents

def cal_oneloop(i,sents):
    #start = time.clock()
    now = datetime.datetime.now()
    result = []
    sentsj = sents[(i + 1):]
    ID_i = sents[i][0]
    tempwordsi1 = sents[i][1]
    tempwordsi2 = sents[i][2]
    for j,(ID_j,tempwordsj1,tempwordsj2) in enumerate(sentsj):
        j = j + i + 1
        #tempwordsj1 = Text_j
        #for m in range(0,len(tempwordsj1)):
        #    try:
        #        tempwordsj1[m] = lemmatizer.lemmatize(tempwordsj1[m])
        #        tempwordsj1[m] = verb.present(tempwordsj1[m])
        #    except:
        #        continue
        #tempwordsj2 = tempwordsj1
        #tempwordsj2 = [i2 for i2 in tempwordsj1 if i2 not in stopwords_en]
        ret = [ tempi for tempi in tempwordsi2 if tempi in tempwordsj2 ]   #Words contained in tempwordsi also contained in tempwordsj
        ret1 = {}.fromkeys(ret).keys()
        highqua = [ word for word in ret if word not in lowquality_en]
        highqua1 = {}.fromkeys(highqua).keys()                                   #remove duplicate words
        count = 0.2 * len(ret1) + 0.8 * len(highqua1)                            #lower the weight of low quality words
        if len(highqua) == len(highqua1):
            k = 0
            while (k < (len(highqua1) - 1)):
                num_overlap = 1
                item = highqua1[k]
                k = k + 1
                item1 = highqua1[k]
                delta = tempwordsi1.index(item) - tempwordsj1.index(item)
                delta1 = tempwordsi1.index(item1) - tempwordsj1.index(item1)
                while (delta1 == delta and (tempwordsi1.index(item1) - tempwordsi1.index(item)) == num_overlap):
                    num_overlap = num_overlap + 1
                    k = k + 1
                    if k == len(highqua1):
                        break
                    else:
                        item1 = highqua1[k]
                        delta1 = tempwordsi1.index(item1) - tempwordsj1.index(item1)
                count+=num_overlap ** 2
        if count != 0:
            try:
                sim = math.tanh(count / math.sqrt((len(tempwordsi2) + len(tempwordsj2))))              #similarity
                if sim != 0:
                    if len(tempwordsi1) > len(tempwordsj1):
                        if len(tempwordsi1) > 20 and len(tempwordsj1) > 8:
                            result.append((ID_i,ID_j,sim,now))
                        else:
                            result.append((ID_j,ID_i,sim,now))
                    else:
                        if len(tempwordsj1) > 20 and len(tempwordsi1) > 8:
                            result.append((ID_j,ID_i,sim,now))
                        else:
                            result.append((ID_i,ID_j,sim,now))
            except Exception,ex:
                print '%s:%s' % (Exception, ex)
                continue
    #if(len(sentsj) >= 5000):
    #    print "i:" + str(i) + " group:" + str(len(sentsj)) + " cost:" +
    #    str(time.clock() - start)
    return result

def cal_sim(i,phrase,sentsj,Issame,now):
    start = time.clock()

    result = []                    ## to save nonzero similarity between
                                   ## sents:(source_id,target_id,similarity)
    sentsj2 = sentsj

    ID_i,Text_i = phrase[0],phrase[1]
    tempwordsi1 = nltk.word_tokenize(Text_i.lower())
    for m in range(0,len(tempwordsi1)):
        try:
            tempwordsi1[m] = lemmatizer.lemmatize(tempwordsi1[m])
            tempwordsi1[m] = verb.present(tempwordsi1[m])         #change noun to singular , keep other words unchanged
        except:
            continue
    tempwordsi2 = [word for word in tempwordsi1 if word not in stopwords_en]
    if Issame == 1:
        sentsj = sentsj2[(i + 1):]
    for j,(ID_j,Text_j) in enumerate(sentsj):
        if Issame == 1:
            j = j + i + 1
        tempwordsj1 = nltk.word_tokenize(Text_j.lower())
        for m in range(0,len(tempwordsj1)):
            try:
                tempwordsj1[m] = lemmatizer.lemmatize(tempwordsj1[m])
                tempwordsj1[m] = verb.present(tempwordsj1[m])
            except:
                continue
        tempwordsj2 = [i2 for i2 in tempwordsj1 if i2 not in stopwords_en]
        ret = [ tempi for tempi in tempwordsi2 if tempi in tempwordsj2 ]   #Words contained in tempwordsi also contained in tempwordsj
        ret1 = {}.fromkeys(ret).keys()
        highqua = [ word for word in ret if word not in lowquality_en]
        highqua1 = {}.fromkeys(highqua).keys()                                   #remove duplicate words
        count = 0.2 * len(ret1) + 0.8 * len(highqua1)                            #lower the weight of low quality words
        if len(highqua) == len(highqua1):
            k = 0
            while (k < (len(highqua1) - 1)):
                num_overlap = 1
                item = highqua1[k]
                k = k + 1
                item1 = highqua1[k]
                delta = tempwordsi1.index(item) - tempwordsj1.index(item)
                delta1 = tempwordsi1.index(item1) - tempwordsj1.index(item1)
                while (delta1 == delta and (tempwordsi1.index(item1) - tempwordsi1.index(item)) == num_overlap):
                    num_overlap = num_overlap + 1
                    k = k + 1
                    if k == len(highqua1):
                        break
                    else:
                        item1 = highqua1[k]
                        delta1 = tempwordsi1.index(item1) - tempwordsj1.index(item1)
                count+=num_overlap ** 2
        if count != 0:
            try:
                sim = math.tanh(count / math.sqrt((len(tempwordsi2) + len(tempwordsj2))))              #similarity
                if sim != 0:
                    if len(tempwordsi1) > len(tempwordsj1):
                        if len(tempwordsi1) > 20 and len(tempwordsj1) > 8:
                            result.append((ID_i,ID_j,sim,now))
                        else:
                            result.append((ID_j,ID_i,sim,now))
                    else:
                        if len(tempwordsj1) > 20 and len(tempwordsi1) > 8:
                            result.append((ID_j,ID_i,sim,now))
                        else:
                            result.append((ID_i,ID_j,sim,now))
            except Exception,ex:
                print '%s:%s' % (Exception, ex)
                continue
    #if(len(sentsj) >= 5000):
    #    print "i:" + str(i) + " group:" + str(len(sentsj)) + " cost:" +
    #    str(time.clock() - start)
    return result

mutex3 = threading.Lock()
mutex = threading.Lock()
mutex2 = threading.Lock()
contentkeyList = []
#SourceTable = "[Stag_VerbatimRawData]"
SentenseTable = "[VerbatimSentence]"
SimilarityTable = "[Similarity]"
DimContentTable = "[VW_DimContent]"

class CalThread(threading.Thread):

    #def __init__(self):
    #    super(StoppableThread, self).__init__()
    #    self._stop = threading.Event()

    #def stop(self):
    #    self._stop.set()

    #def stopped(self):
    #    return self._stop.isSet()

    def run(self):
        time.sleep(1)
        while(True):
            i = 0
            global culturecode
            global clustername
            global cal_sim_cursor
            global finishedloop

            if(mutex.acquire(1)):
                if(cal_sim_cursor >= len(sents)):
                    #remained_thread = 0
                    print str(self.name) + "---cursor: " + str(cal_sim_cursor) + "len: " + str(len(sents))
                    print
                    #time.sleep(1)
                    mutex.release()
                    #self.stop()
                    break
                i = cal_sim_cursor
                cal_sim_cursor += 1
                mutex.release()

            if(len(sents) >= 0):
                print str(self.name) + "---" + str(clustername) + "-" + str(culturecode) + " i:" + str(i) + " sents:" + str(len(sents)) + " ---start"
            starttime = time.clock()
            result = cal_oneloop(i,sents)
            if(len(sents) >= 0):
                print str(self.name) + "---" + str(clustername) + "-" + str(culturecode) + " i:" + str(i) + " sents:" + str(len(sents)) + " ---calculation cost: " + str(time.clock() - starttime)

            if mutex2.acquire(1):
                if result != []:
                    starttime = time.clock()
                    #insert into the database:
                    sql_insert2 = "insert into [dbo]." + SimilarityTable + "(Source,Target,Similarity,RowModifiedDate) VALUES "
                    #insert_bulk(sql_insert2,"('%d','%d','%f',CAST('%s' AS DATETIME2))",result)
                    result.append(result)
                    #print str(self.name) + "---" + " i:" + str(i) + " sents:" + str(len(sents)) + " ---insert cost: " + str(time.clock() - starttime)
                    print

                mutex2.release()

            if(mutex3.acquire(1)):
                finishedloop += 1
                print str(self.name) + "--- finishedloop: " + str(finishedloop) + " i:" + str(i) + " sents:" + str(len(sents))
                print
                mutex3.release()

def main():
    global finishedloop
    global cal_sim_cursor
    #global oneloopresult
    global sents
    #global start
    global clusternameList
    global culturecode
    global clustername
    global cluster_culturecode_list

    #start = time.clock()

    sql_clusternames = "SELECT distinct ClusterName FROM [ContentImpact].[dbo]." + DimContentTable + "where ClusterName != 'No Cluster (Unmapped)' order by ClusterName"
    clusternameList = query(sql_clusternames)

    sql_culturecode = "select distinct culturecode from VW_DimContent"
    culturecodeList = query(sql_culturecode)

    cluster_culturecode_list = []
    for cn in clusternameList:
        for cc in culturecodeList:
            cluster_culturecode_list.append((cn[0],cc[0]))


    #names = ['WLGS','System Center & Security' , 'SQL', 'Unified
    #Communications', 'SharePoint', 'PC Health', 'Online Services', 'Office',
    #'O365','No Cluster (Unmapped)']

    #for clustername in clusternameList[::-1]:
    #    for item in names:
    #        if(item in clustername):
    #            clusternameList.remove(clustername)


    #for i in range(32):
    #    t = MyThread()
    #    t.start()
    while(True):
        if(len(cluster_culturecode_list) <= 0):
            break

        item = cluster_culturecode_list.pop()
        clustername = item[0]
        culturecode = item[1]
        #clustername = 'WLGS'
        #culturecode = 'cs'

        sql_sent = "SELECT [identity],SentenceText FROM [ContentImpact].[dbo]." + SentenseTable + " where ContentKey in (select ContentKey from [ContentImpact].[dbo]." + DimContentTable + " where ClusterName = '" + clustername + "' and CultureCode = '" + culturecode + "')"
        sents = query(sql_sent)

        print str(clustername) + "-" + str(culturecode) + " sentence: " + str(len(sents))

        result=[]
        # calculate similarities
        if(len(sents) > 1):
            #Issame = 1
            cal_sim_cursor = 0
            threadcount = 100
            finishedloop = 0

            sents = preprocess_sents(sents)

            if(len(sents) < threadcount):
                threadcount = len(sents)
            for i in range(threadcount):
                t = CalThread()
                t.start()

            # wait for current sents calculation finished
            while(finishedloop < len(sents)):
                time.sleep(5)


        print "remained items: " + str(len(cluster_culturecode_list))



    #cur.execute(sql,path)

main()

