# -*- coding: utf-
import nltk
#import verb
import re
import math
#import os.path
#import time
#import datetime
from nltk.stem import WordNetLemmatizer
import MySQLdb
import networkx as nx

lemmatizer = WordNetLemmatizer()
lowquality_en = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now']
stopwords_en = ['to','be','do','have','I','a','an','the','and','s','t','can','will','don','should','.',',','?',':','(',')','[',']','"',"'",';','``','=','`','!','...']

g_Conn = None
def openDB():
    global g_Conn
    try:
        g_Conn = MySQLdb.connect(host="localhost",user="root",passwd="you",db="sys")
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


def cal_sim(sentsi,sentsj,Issame):
    scounti = len(sentsi)
    scountj = len(sentsj)
    result = []                    ## to save nonzero similarity between
                                   ## sents:(source_id,target_id,similarity)
    sentsj2 = sentsj

    for i,(ID_i,Text_i) in enumerate(sentsi):
        tempwordsi1 = nltk.word_tokenize(Text_i.lower())
        for m in range(0,len(tempwordsi1)):
            try:
                tempwordsi1[m] = lemmatizer.lemmatize(tempwordsi1[m])
                #tempwordsi1[m] = verb.present(tempwordsi1[m])
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
                    #tempwordsj1[m] = verb.present(tempwordsj1[m])
                except:
                    continue
            tempwordsj2 = [i2 for i2 in tempwordsj1 if i2 not in stopwords_en]
            ret = [ tempi for tempi in tempwordsi2 if tempi in tempwordsj2 ]   #Words contained in tempwordsi also contained in tempwordsj
            ret1 = {}.fromkeys(ret).keys()
            highqua = [ word for word in ret if word not in lowquality_en]
            highqua1 = {}.fromkeys(highqua).keys()                                   #remove duplicate words
            count = 0.2 * len(ret1) + 0.8 * len(highqua1)                            #lower the weight of low quality words
            #print count,len(ret1),len(highqua1)
            #print ret1,highqua1
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
            #print count
            if count != 0:
                try:
                    #sim = math.tanh(math.sqrt(len(tempwordsi2)+len(tempwordsj2))/(count*2.0))         ###distance
                    sim = math.tanh(count / math.sqrt((len(tempwordsi2) + len(tempwordsj2))))              #similarity
                    if sim != 0:
                        if len(tempwordsi1) > len(tempwordsj1):
                            if len(tempwordsi1) > 20 and len(tempwordsj1) > 8:
                                result.append((ID_i,ID_j,sim))
                            else:
                                result.append((ID_j,ID_i,sim))
                        else:
                            if len(tempwordsj1) > 20 and len(tempwordsi1) > 8:
                                result.append((ID_j,ID_i,sim))
                            else:
                                result.append((ID_i,ID_j,sim))
                except Exception,ex:
                    print '%s:%s' % (Exception, ex)
                    continue
    return result

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


def pagerank(result,sentsi):

    #culturekey = 55

    #start = time.clock()
    #now = datetime.datetime.now()

    #SentenseTable = "[VerbatimSentence]"
    #SimilarityTable = "[Similarity]"
    #ResultTable = "[VerbatimComplains]"

    #"""similarity_threshold = 0.4         ### parameter


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


    max_num = 0              ## class number , =len(resultsents)
    G = nx.Graph()
    for item in result:
        G.add_edge(int(item[0]),int(item[1]),weight=float(item[2]))
    pr = nx.pagerank(G,alpha=0.85)
    pr = sorted(pr.items(), key=lambda d:d[1], reverse=True)  #sorted by value desc
    pr1 = []
    classresult = []
    for i,(id_i,pagerank_i) in enumerate(pr):
        flag=0
        for j,(id_j) in enumerate(classresult):
            s = int(min(id_i,id_j))          #取ID小的句子的坐标
            t = int(max(id_i,id_j))          #取ID大的句子的坐标
            try:
                if dict2[(s,t)] >= similarity_threshold :
                    #pr1.append((id_i,j,0,highlight(sentsi[id_i],sentsi[id_j])))
                    r1.append((id_i,j,0))
                    flag=1
                    break
                else:
                    continue
            except:
                continue
        if (flag == 0):                     #new class
            classresult.append(id_i)
            #pr1.append((id_i,max_num,1,""))
            pr1.append((id_i,max_num,1))
            max_num+=1
        for k in pr1[ :20]:
            print k
            ##print sentsi[ k[0] ][1]




def main():
    #sentsi=[[1,"I DONT KNOW MUCH ABOUT COMPUTERS AND IVE PUT ALOT OF TIME IN ALREADY TRYING TO GET THIS INSTALLED."]]
    #sentsj=[[2,'Owing to some problems with my Surface pro 2, I restored to its original status, and after that I  tried to re- install office 365 on my Surface pro 2 several times, but failed again and agian.']]
    sql= "SELECT id,comment FROM `sys`.`table1`"
    sentsi=query(sql)
    sentsj=sentsi
    #print sentsi[0][1]
    #print sentsj[0][1]
    Issame=1
    #now = datetime.datetime.now()
    result = cal_sim(sentsi,sentsj,Issame)
    pagerank(result,sentsi)
main()
