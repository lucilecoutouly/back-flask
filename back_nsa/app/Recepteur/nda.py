__author__ = 'lucile'
# -*- coding: = utf-8 -*-

# from astropy.time import Time
#import ctypes

import timeit
import gzip
from struct import *
from collections import namedtuple
import shutil
import array
import binascii
import datetime
import scipy.signal as signal
import requests
#from astropy.time import Time
import urllib
from time import sleep
from requests_futures.sessions import FuturesSession
import re
import tarfile
import uuid
import numpy as np
import time
import math
#import json
import simplejson as json
import sys
import traceback
import io

import matplotlib

matplotlib.use('Agg')

import os.path
#import tables as tb

from influxdb import client as influxdb
from influxdb import DataFrameClient

import matplotlib.pyplot as plt
import pandas as pd

import psycopg2
import csv
from elasticsearch import Elasticsearch


class nda(object):

    MAX_SPECTRE = 20
    NB_TABLE = range(0, 1)
    VALUE_COL = 400
    HOST = "localhost"
    PORT = 8086
    USERNAME = "root"
    PASSWORD = "root"
    DATABASE = "rsd"

    #DB =psycopg2.connect(database="postgres", user="postgres", password="user", host="nosql")

    def getRequest(self, filtre, type, frequence):
        """
        :param filtre:
        :param type:
        :param frequence:
        :return:
        """
        print(filtre, type)
        if type == "canvas":
            return self.getCanvas(filtre, frequence)
        elif type == "import":
            return self.setImport(filtre, frequence)
        elif type == "export":
            return self.setExport(filtre, frequence)
        elif type == "view":
            return self.getView(filtre, frequence)
        elif type == "query":
            return self.Query(filtre, frequence)
        elif type == "image":
            return self.getImage(filtre, frequence)
        elif type == "flux":
            return self.getFlux(filtre, frequence)
        elif type == "json":
            return self.getJson(filtre, frequence)
        elif type == "csv":
            return self.getCsv(filtre, frequence)
        elif type == "data":
            return self.getData(filtre, frequence)
        elif type == "w3d":
            return self.getw3d(filtre, frequence)

        elif type == "index":
            return self.setIndex(filtre, frequence)
        elif type == "getQuery":
            return self.getQuery(filtre, frequence)
        elif type == "setQuery":
            return self.setQuery(query)
        elif type == "getDate":
            return self.getDate(frequence)

    def getQuery(self, f, sel):
        # print("getQuery")
        filtre = json.loads(f)

        date = datetime.datetime.strptime(
            filtre["date"], "%Y-%m-%dT%H:%M:%S.%fZ")
        time = datetime.datetime.strptime(
            filtre["time"], "%Y-%m-%dT%H:%M:%S.%fZ")
        duree = int(filtre["dure"])
        recepteur = filtre["recepteur"]
        frequence = filtre["frequence"]
        integration = str(filtre["integration"]) + 's'
        polarite = (filtre["polarite"])
        if filtre["find"]:
            find = filtre["find"]
        else:
            find = "avg"
        matchObj = re.search(r'f\d*', find)
        if matchObj:
            col = matchObj.group()

            num = re.search(r'\d+', col)
            if num:
                SEL = round(int(num.group()) / 100)
                print(SEL)

        fmin = int(frequence[0])
        fmax = int(frequence[1])

        if int(recepteur):
            metric = "new"
        else:
            metric = "old"

        # print("recepteur",old,new)

        L = " "
        tag = 0
        for j in np.arange(fmin, fmax):
            z = str(j)
            if j < 100:
                z = "0" + z
                if j < 10:
                    z = "0" + z

            if tag > 0:
                L += ", "
            L += " f" + z
            tag += 1
        L += " "

        """
        B   business day frequency
        C   custom business day frequency (experimental)
        D   calendar day frequency
        W   weekly frequency
        M   month end frequency
        BM  business month end frequency
        MS  month start frequency
        BMS business month start frequency
        Q   quarter end frequency
        BQ  business quarter endfrequency
        QS  quarter start frequency
        BQS business quarter start frequency
        A   year end frequency
        BA  business year end frequency
        AS  year start frequency
        BAS business year start frequency
        H   hourly frequency
        T   minutely frequency
        S   secondly frequency
        L   milliseconds
        U   microseconds
        """

        oneday = datetime.timedelta(days=1)
        dureeday = duree / 86400
        counterDay = 0
        D = datetime.timedelta(seconds=duree)
        DU = datetime.timedelta(days=dureeday)

        DD = date
        DF = date + DU

        D = datetime.timedelta(seconds=duree)
        DT = datetime.datetime.strptime(date.strftime("%Y-%m-%d") + time.strftime("T%H:%M:%S.%fZ"),
                                        "%Y-%m-%dT%H:%M:%S.%fZ")

        DF = DT + D

        # pd.set_option('io.hdf.default_format','table')
        TT = (DT.timestamp())

        DF = DT + D
        TF = (DF.timestamp())
        #SELECTOR = "BRUT/F0"

        """
        QUERY = "index >= Timestamp('"+DT.strftime("%Y-%m-%d %H:%M:%S.%f")+"') & index <= Timestamp('"+DF.strftime(
            "%Y-%m-%d %H:%M:%S.%f")+"') & pol == "+str(polarite)+" & columns="+ L
        # select * from routine where time > '2014-01-02 07:56:06' and time < '2014-01-02 07:56:11.400000' and pol = 1
        QUERY = "select "+L+" from routine where time > '"+str(DT.isoformat(' '))+"' and time <  '"+str(DF.isoformat(
            ' '))+"' and polarite = '"+str(polarite) +"' group by time ("+integration+")"


        if not find =="":

            QUERY += " & "+ find
        """
        #QUERY = "select "+L+" from "+integration+".R"+polarite+" where time > '"+str(DT.isoformat(' '))+"' and  time <  '"+str(  DF.isoformat( ' '))+"'"

        """
        $ gzip -9c clear-32k.json > gzip-32k.json

        $ file gzip-32k.json
        gzip-32k.json: gzip compressed data, was "clear-32k.json", from Unix, last modified: Thu Jan 16 15:31:55 2014

        $ ls -l gzip-32k.json
        -rw-r--r-- 1 root root 1666 févr.  4 09:57 gzip-32k.json

        $ curl -X POST --data-binary "@gzip-32k.json"
         --header "Content-Type: application/json"
          --header "Content-Encoding: gzip" http://mytsdb1:4242/api/put?details
        {"errors":[],"failed":0,"success":280}

        """
        # http://nosql:4242/api/query?start=1359702000&end=1359730800&m=avg:60s-avg:routine.old{polarite=1,status=0,frequence=*}

        QUERY = []
        """
        for f in np.arange(fmin, fmax):
                QUERY.append({
                    "aggregator": "sum",
                    "metric": "routine.old",
                    "rate": "true",
                    "tags": {
                        "polarite":"D",
                        "status":"BRUT",
                        "frequence":int(f)
                    }
                })
        """

        QUERY.append({
            "aggregator": find,
            "downsample": integration + "-" + find,
            "metric": "nda." + metric,
            "rate": "false",
                    "tags": {
                        "p": polarite,
                        "s": 0,
                        "f": "*"
                    }
        })

        payload = {
            "start": round(DT.timestamp()),
            "end": round(DF.timestamp()),
            "queries": QUERY
        }

        """
        payload = {
            "start": round(DT.timestamp()),
            "end": round(DF.timestamp()),
            "m":"sum:1m-avg:routine.old{polarite=D,status=BRUT,frequence=10}"
        }
        """

        return {'query': json.dumps(payload), "status": "success",
                "message": "query set succesfully.", "recepteur": sel}

    def setQuery(self, query):

        headers = {
            "content-type": "application/json"
            #,"Content-Encoding": "gzip"
        }
        f = {}

        #payload = {'key1': 'value1', 'key2': 'value2'}
        url = 'http://localhost:4242/api/query'
        q = json.loads(query)
        r = requests.post(url, data=json.dumps(q), headers=headers)

        return self.setResponse(r, f)

    def setResponse(self, r, f):

        filtre = json.loads(f)

        date = datetime.datetime.strptime(
            filtre["date"], "%Y-%m-%dT%H:%M:%S.%fZ")
        time = datetime.datetime.strptime(
            filtre["time"], "%Y-%m-%dT%H:%M:%S.%fZ")
        duree = int(filtre["dure"])
        recepteur = filtre["recepteur"]
        frequence = filtre["frequence"]
        integration = str(filtre["integration"]) + 's'
        polarite = (filtre["polarite"])
        if filtre["find"]:
            find = filtre["find"]
        else:
            find = "avg"
        matchObj = re.search(r'f\d*', find)
        if matchObj:
            col = matchObj.group()

            num = re.search(r'\d+', col)
            if num:
                SEL = round(int(num.group()) / 100)
                print(SEL)

        fmin = int(frequence[0])
        fmax = int(frequence[1])

        result = r.json()

        """
        {
        'tags': {
            'polarite': 'D',
            'frequence': '398',
            'status': 'BRUT'
            },
        'aggregateTags': [],
        'dps': {
            '1388908822': 67.0,
            '1388908821': 70.0,
            '1388908826': 65.0,
            '1388908820': 67.0,
            '1388908823': 74.0,
            '1388908816': 68.0,
            '1388908825': 66.0,
            '1388908819': 73.0,
            '1388908817': 64.0,
            '1388908824': 67.0,
            '1388908818': 68.0
            },
        'metric': 'routine.old'
        }


        """
        V = {}

        found = False
        #Z = pd.read_json(result,dtype={'A' : 'float32', 'bools' : 'int8'})
        #Z = pd.io.json.json_normalize(result,{'dps'},'metric',['tags','polarite'],['tags','frequence'])
        # print(Z)
        # exit()
        for r in result:

            f = r["tags"]["f"]
            F = str(f)
            if int(f) < 100:
                F = "0" + F
                if int(f) < 10:
                    F = "0" + F
            L = {}

            for k in r["dps"]:

                L[k] = r["dps"][k]
            V[F] = L

            found = True

        Z = pd.DataFrame(V)
        if fmin:
            Z = Z.iloc[:, fmin:fmax]

        return {'result': result, 'found': found, 'DF': Z}

    def Query(self, f, sel):

        headers = {
            "content-type": "application/json"
            #,"Content-Encoding": "gzip"
        }
        #payload = {'key1': 'value1', 'key2': 'value2'}
        url = 'http://opentsdb-rsdb.obs-nancay.fr:4242/api/query'

        req = self.getQuery(f, sel)
        query = req["query"]

        r = requests.post(url, data=query, headers=headers)

        return self.setResponse(r, f)

    def getImage(self, f, sel):
        """
        :param f:
        :param sel:
        :return:
        """

        filtre = json.loads(f)

        date = datetime.datetime.strptime(
            filtre["date"], "%Y-%m-%dT%H:%M:%S.%fZ")
        time = datetime.datetime.strptime(
            filtre["time"], "%Y-%m-%dT%H:%M:%S.%fZ")
        duree = int(filtre["dure"])
        recepteur = filtre["recepteur"]
        frequence = filtre["frequence"]
        integration = str(filtre["integration"]) + 's'
        polarite = (filtre["polarite"])
        if filtre["find"]:
            find = filtre["find"]
        else:
            find = "avg"
        matchObj = re.search(r'f\d*', find)
        if matchObj:
            col = matchObj.group()

            num = re.search(r'\d+', col)
            if num:
                SEL = round(int(num.group()) / 100)
                print(SEL)

        fmin = int(frequence[0])
        fmax = int(frequence[1])
        if int(recepteur):
            metric = "new"
        else:
            metric = "old"

        fig = plt.figure()
        print("coucou2")
        fig.clf()

        #ax1 = fig.add_subplot(1,1, 1)

        #ax2 = ax1.twinx()
        #t = np.linspace(0., 10., 100)
        # ax1.plot(t, t ** 2, 'b-')
        #ax2.plot(t, 1000 / (t + 1), 'r-')

        # ax1.set_ylabel('Density (cgs)', color='red')
        # ax2.set_ylabel('Temperature (K)', color='blue')
        #ax1.set_xlabel('Time (s)')
        print("coucou3")
        resp = self.Query(f, sel)

        if resp["found"]:
            #result = resp["DF"]
            if metric == "old":
                result = resp["DF"]
            if metric == "new":
                result = resp["DF"]
            #y = numpy.asarray(result.index)
            # print(y.shape)
            #x = np.arange(0,y.shape[0])
            # ax1.plot(x,y)
            #ax1.plot(x, 'b-')

            #result = result.fillna('NaT')
            #result = result.dropna(axis=0)
            file = str(uuid.uuid4()) + '.png'
            url = '/Public/Images/' + file
            uri = '/var/www/html' + url
            # result.plot()

            Z = np.asarray(result)

            # i = ax1.imshow(Z)
            # fig.colorbar(i)

            # plt.colorbar()
            #rax = result.plot()
            #fig.subplots_adjust(left=0.20, bottom=0.15, right=0.78, top=0.92, wspace=0.05, hspace=0)
            #fig1 = ax1.get_figure()
            #fig.savefig(uri, vmin=0, vmax=255, format='png', dpi=1600)
            # plt.set_xlabel([80,122])
            plt.imsave(uri, Z, format='png', )

            #plt.imsave(uri,Z , vmin=0, vmax=255, format='png', dpi=1600);
            # plt.draw()
        else:
            file = "NoData" + '.png'
            url = '/Prive/Images/' + file
            uri = '/var/www/html' + url
        response = {
            "status": "success",
            "message": "Image loaded succesfully.",
            "recepteur": sel,
            "url": url
        }

        return response

    def getFind(self, f, sel):

        filtre = json.loads(f)

        date = datetime.datetime.strptime(
            filtre["date"], "%Y-%m-%dT%H:%M:%S.%fZ")
        time = datetime.datetime.strptime(
            filtre["time"], "%Y-%m-%dT%H:%M:%S.%fZ")
        duree = int(filtre["dure"])
        recepteur = filtre["recepteur"]
        frequence = filtre["frequence"]
        integration = str(filtre["integration"]) + 's'
        polarite = (filtre["polarite"])
        aggregator = filtre["find"]
        matchObj = re.search(r'f\d*', find)
        if matchObj:
            col = matchObj.group()

            num = re.search(r'\d+', col)
            if num:
                SEL = round(int(num.group()) / 100)
                print(SEL)

        fmin = int(frequence[0])
        fmax = int(frequence[1])

        oneday = datetime.timedelta(days=1)
        dureeday = duree / 86400
        counterDay = 0
        D = datetime.timedelta(seconds=duree)
        DU = datetime.timedelta(days=dureeday)

        DD = date
        DF = date + DU

        D = datetime.timedelta(seconds=duree)
        DT = datetime.datetime.strptime(date.strftime("%Y-%m-%d") + time.strftime("T%H:%M:%S.%fZ"),
                                        "%Y-%m-%dT%H:%M:%S.%fZ")

        DF = DT + D

        # pd.set_option('io.hdf.default_format','table')
        TT = (DT.timestamp())

        DF = DT + D
        TF = (DF.timestamp())

        QUERY.append({
            "aggregator": aggregator,
            "downsample": integration + "-avg",
            "metric": "routine.old",
            "rate": "false",
            "tags": {
                "polarite": polarite,
                "status": "BRUT",
                "frequence": "*"
            }
        })

        url = 'http://localhost:4242/api/query'
        payload = {
            "start": round(DT.timestamp()),
            "end": round(DF.timestamp()),
            "queries": QUERY
        }

        headers = {
            "content-type": "application/json"
            #,"Content-Encoding": "gzip"
        }
        #payload = {'key1': 'value1', 'key2': 'value2'}

        r = requests.post(url, data=json.dumps(payload), headers=headers)

        """
        " cli
        "
        """
        # query [Gnuplot opts] START-DATE [END-DATE] <aggregator> [rate]
        # [counter,max,reset] [downsample N FUNC] <metric> [<tagk=tagv>]
        # [...<tagk=tagv>] [...queries]

        #r = requests.post(url, data=json.dumps(stream), headers=headers)

        result = r.json()

        return sel

    def getView(self, f, sel):
        """
        :param f:
        :param sel:
        :return:

        """
        resp = self.Query(f, sel)

        if resp["found"]:
            # print(result)
            #print (result.to_string(formatters={'cost':'${:,.2f}'.format}))
            res = []
            result = resp["DF"]
            #result = result.dropna(axis=0)
            ligne = 0
            for row_index, row in result.iterrows():

                i = 0
                if ligne > self.MAX_SPECTRE:
                    break
                values = []
                for y in row:
                    if i < 400:
                        values.append({'x': i, 'y': str(y)})
                    i += 1
                DD = datetime.datetime.utcfromtimestamp(int(row_index))
                res.append(
                    {'key': DD.strftime("%Y-%m-%d %H:%M:%S"), 'values': values})
                ligne += 1
                # res.append({'values':{'x':row_index,'y':row}})

        else:
            file = "NoData" + '.png'
            url = '/Prive/Images/' + file
            uri = '/var/www/html' + url
            res = [
                {
                    "key": "No Data Found",
                    "values": [[]]
                }
            ]

        response = {
            "status": "success",
            "message": "Image loaded succesfully.",
            "recepteur": sel,
            "data": res
        }

        return response

    def getJson(self, f, sel):
        """
        :param f:
        :param sel:
        :return:

        """

        resp = self.Query(f, sel)
        print(resp)

        if resp["found"]:
            # print(result)
            #print (result.to_string(formatters={'cost':'${:,.2f}'.format}))
            res = []
            result = resp["DF"]
            #result = result.dropna(axis=0)
            ligne = 0

            for row_index, row in result.iterrows():

                i = 0

                values = []
                for y in row:
                    if i < 400:
                        #values.append({'x': i, 'y': y})
                        values.append(y)
                    i += 1
                DD = datetime.datetime.fromtimestamp(int(row_index))
                res.append(
                    {'key': DD.strftime("%Y-%m-%d %H:%M:%S"), 'values': values})
                ligne += 1
                # res.append({'values':{'x':row_index,'y':row}})

        else:
            file = "NoData" + '.png'
            url = '/Prive/Images/' + file
            uri = '/var/www/html' + url
            res = [
                {
                    "key": "No Data Found",
                    "values": [[]]
                }
            ]

        response = {
            "status": "success",
            "message": "Image loaded succesfully.",
            "recepteur": sel,
            "data": res
        }

        return response

    def getCsv(self, f, sel):
        """
        :param f:
        :param sel:
        :return:

        """
        filtre = json.loads(f)

        resp = self.Query(f, sel)

        if resp["found"]:
            # print(result)
            #print (result.to_string(formatters={'cost':'${:,.2f}'.format}))
            res = []
            result = resp["DF"]
            file = str(uuid.uuid4()) + '.csv'
            url = '/Public/Images/' + file
            uri = '/var/www/html' + url
            output = io.StringIO()

            result.to_csv(output, line_terminator="\n\r")

        else:
            file = "NoData" + '.png'
            url = '/Prive/Images/' + file
            uri = '/var/www/html' + url
            res = [
                {
                    "key": "No Data Found",
                    "values": [[]]
                }
            ]

        response = {
            "status": "success",
            "message": "Image loaded succesfully.",
            "recepteur": sel,
            "data": output.getvalue()
        }
        output.close()
        return response

    def getData(self, f, sel):
        """
        :param f:
        :param sel:
        :return:

        """
        headers = {
            "content-type": "application/json"
            #,"Content-Encoding": "gzip"
        }
        #payload = {'key1': 'value1', 'key2': 'value2'}
        url = 'http://localhost:4242/api/query'

        req = self.getQuery(f, sel)
        query = req["query"]

        r = requests.post(url, data=query, headers=headers)
        result = r.json()
        V = {}

        found = False
        #Z = pd.read_json(result,dtype={'A' : 'float32', 'bools' : 'int8'})
        #Z = pd.io.json.json_normalize(result,{'dps'},'metric',['tags','polarite'],['tags','frequence'])
        # print(Z)
        # exit()
        res = []
        for r in result:

            f = r["tags"]["frequence"]
            F = str(f)
            p = r["tags"]["polarite"]
            P = str(p)
            s = r["tags"]["status"]
            S = str(s)

            if int(f) < 100:
                F = "0" + F
                if int(f) < 10:
                    F = "0" + F

            L = {}
            for k in r["dps"]:
                v = r["dps"][k]
                DD = datetime.datetime.fromtimestamp(int(k))
                res.append({'datetime': DD.strftime("%Y-%m-%d %H:%M:%S"),
                            'frequence': F,
                            'polarite': P,
                            'status': S,
                            'value': round(v, 2)})

            found = True

        response = {
            "status": "success",
            "message": "Image loaded succesfully.",
            "recepteur": sel,
            "data": res
        }

        return response

    def getw3d(self, f, sel):
        """
        :param f:
        :param sel:
        :return:

        """
        headers = {
            "content-type": "application/json"
            #,"Content-Encoding": "gzip"
        }
        #payload = {'key1': 'value1', 'key2': 'value2'}
        url = 'http://localhost:4242/api/query'

        req = self.getQuery(f, sel)
        query = req["query"]

        r = requests.post(url, data=query, headers=headers)
        result = r.json()
        V = {}

        found = False
        #Z = pd.read_json(result,dtype={'A' : 'float32', 'bools' : 'int8'})
        #Z = pd.io.json.json_normalize(result,{'dps'},'metric',['tags','polarite'],['tags','frequence'])
        # print(Z)
        # exit()
        res = []
        x = []
        y = []
        z = []

        for r in result:

            f = r["tags"]["frequence"]
            F = str(f)
            p = r["tags"]["polarite"]
            P = str(p)
            s = r["tags"]["status"]
            S = str(s)
            print(r)
            if int(f) < 100:
                F = "0" + F
                if int(f) < 10:
                    F = "0" + F

            L = {}
            for k in r["dps"]:
                v = r["dps"][k]
                DD = datetime.datetime.fromtimestamp(int(k))
                # res.append({'x':k,
                #    'y': f,

                #    'z':round(v,2)})
                res.append(f)
                print(v)
                exit()
                res[f].append(k)
                res[f][k] = round(v, 2)
                print(res)
                exit()

            found = True

        response = {
            "status": "success",
            "message": "Image loaded succesfully.",
            "recepteur": sel,
            "data": res
        }

        return response

    def getFlux(self, f, sel):
        """
        :param f:
        :param sel:
        :return:

        """

        req = self.getQuery(f, sel)

        query = req["query"]

        # url = nosql/WWW/RESERVE/api/v8/recepteur/Nda/flux/1?filtre=%7B"date":"1996-02-01T00:00:00.000Z",\
        #
        #                                                                "time":"2013-02-01T09:00:00.000Z",
        # "frequence":%5B"1"%5D,"recepteur":%5B10,300%5D,"integration":12,"dure":36000,"polarite":"D"%7D

        url = "/instrument/Nda/json/1?filtre=" + urllib.parse.quote_plus(f)
        title = "Json"
        response = {
            "status": "success",
            "message": "JSON ready ",
            "recepteur": sel,
            "url": url,
            "title": title
        }

        return response

    def setIndex(self, f, sel):
        """

        :param f:
        :param sel:
        :return:
        """

        filtre = json.loads(f)

        # "2014-01-01T00:00:00.000Z"

        date = datetime.datetime.strptime(
            filtre["date"], "%Y-%m-%dT%H:%M:%S.%fZ")
        time = datetime.datetime.strptime(
            filtre["time"], "%Y-%m-%dT%H:%M:%S.%fZ")
        duree = int(filtre["dure"])
        frequence = filtre["recepteur"]
        recepteur = filtre["frequence"]

        if os.path.isfile(self.HDF5_CURRENT_OLD):
            print("")
            store = pd.io.pytables.HDFStore(self.HDF5_CURRENT_OLD)

            # for i in self.NB_TABLE:
            print("ré-indexation")
            #

            for i in self.NB_TABLE:
                print("table", i)
                store.create_table_index(self.BRUT_NAME + "/" + self.FREQUENCE_NAME + str(i), columns=["index"],
                                         optlevel=9,
                                         kind='full')

            store.close()

        response = {
            "status": "success",
            "message": "Indexation ok "

        }

        return response

    def getDate(self, f):

        cur = self.DB.cursor()
        cur.execute(
            """SELECT min(time_min) FROM rsd.catalogue WHERE  status_id = %s and  flag_id = %s and  recepteur_id = %s""",
            (2, 3, f))
        r = cur.fetchone()
        if r:
            id = r[0]
            # update
            cur.execute(
                """UPDATE  rsd.stream SET (timestamp, status_id, flag_id,niveau_id,recepteur_id) =
                (%s, %s, %s, %s, %s)
                WHERE id = %s
                ;""",
                (dt, 2, 3, 5, 1, id))
        else:

            cur.execute(
                """INSERT INTO rsd.stream (timestamp, status_id, flag_id,niveau_id,recepteur_id)
                VALUES (%s, %s, %s, %s, %s);""",
                (dt, 2, 3, 5, 1))

        self.DB.commit()

    def setImport(self, f, sel):
        """

        :param f:
        :param sel:
        :return:
        """
        # self.db.create_database("rsd")

        filtre = json.loads(f)

        # "2014-01-01T00:00:00.000Z"

        date = datetime.datetime.strptime(
            filtre["date"], "%Y-%m-%dT%H:%M:%S.%fZ")
        time = datetime.datetime.strptime(
            filtre["time"], "%Y-%m-%dT%H:%M:%S.%fZ")
        duree = int(filtre["dure"])
        frequence = filtre["frequence"]
        recepteur = filtre["recepteur"]

        oneday = datetime.timedelta(days=1)
        counterDay = 0
        DU = datetime.timedelta(days=duree)
        DD = date
        DF = date + DU

        # pd.set_option('io.hdf.default_format','table')

        if int(recepteur):
            metric = "new"

            old = 0
        else:

            print(int(recepteur))
            metric = "old"
            old = 1

        oneday = datetime.timedelta(days=1)

        counterDay = 1

        print("import")

        #session = FuturesSession(max_workers=2)
        session = requests.Session()
        headers = {
            "Content-Type": "application/json", "Content-Encoding": "gzip"
        }
        session.headers.update(headers)

        print("boucle date debut : ", date)
        while counterDay < duree:

            if old:
                #fname = "/datadam/routine/2013/S130215.RT1"
                fdam = "S" + date.strftime("%y%m%d") + ".RT1"
                rdam = date.strftime("%Y/%m/")
                fname = "/media/data/Data_nda/routine_soleil/" + rdam + "" + fdam

                #fname = "/datadam/routine_soleil/" + fdam
            else:
                fdam = date.strftime("%Y%m%d") + "_000000.dat"
                rdam = date.strftime("%Y")
                fname = "/data/data_nda/Routine_New/" + rdam + "/" + fdam

            self.DAY_NAME = "D" + date.strftime("%d")
            self.MONTH_NAME = "M" + date.strftime("%m")
            self.YEAR_NAME = "Y" + date.strftime("%Y")
            i = 0
            #name = "/" + self.YEAR_NAME+"/"+self.MONTH_NAME + "/"+self.DAY_NAME
            # name = "S" + date.strftime("%Y%m%d")
            D = datetime.timedelta(seconds=3600)
            DT = date
            DF = DT + D

            DT = datetime.datetime.strptime(date.strftime("%Y-%m-%d") + time.strftime("T%H:%M:%S.%fZ"),
                                            "%Y-%m-%dT%H:%M:%S.%fZ")

            DF = DT + D

            # pd.set_option('io.hdf.default_format','table')
            TT = (DT.timestamp())

            DF = DT + D
            TF = (DF.timestamp())

            QUERY = []
            """
            for f in np.arange(fmin, fmax):
                    QUERY.append({
                        "aggregator": "sum",
                        "metric": "routine."+ metric,
                        "rate": "true",
                        "tags": {
                            "polarite":"D",
                            "status":"BRUT",
                            "frequence":int(f)
                        }
                    })
            """
            QUERY.append({
                "aggregator": "avg",
                "downsample": "120s-avg",
                "metric": "routine." + metric,
                "rate": "false",
                        "tags": {
                            "polarite": 1,
                            "status": 1,
                            "frequence": 0
                        }
            })

            url = 'http://master-rsdb:4242/api/query'
            payload = {
                "start": round(DT.timestamp()),
                "end": round(DF.timestamp()),
                "queries": QUERY
            }
            """
            payload = {
                "start": round(DT.timestamp()),
                "end": round(DF.timestamp()),
                "m":"sum:1m-avg:routine.old{polarite=D,status=BRUT,frequence=10}"
            }
            """

            # print(payload,DT)
            # Create your header as required

            #payload = {'key1': 'value1', 'key2': 'value2'}

            #r = requests.post(url, data=json.dumps(payload), headers=headers)
            #r = session.post(url, data=json.dumps(payload))

            """
            " cli
            "
            """
            # query [Gnuplot opts] START-DATE [END-DATE] <aggregator> [rate]
            # [counter,max,reset] [downsample N FUNC] <metric> [<tagk=tagv>]
            # [...<tagk=tagv>] [...queries]

            #r = requests.post(url, data=json.dumps(stream), headers=headers)

            #result = r.json()

            # print(result)

            ajout = True

            # for r in result:

            #    for k in r["dps"]:
            #        ajout = False

            print("ajout à la base ? : ", ajout, date)

            if ajout:

                if os.path.isfile(fname):
                    print("ok", fname)
                    # sleep( 600 )

                    """
                    entete
                    """

                    file = open(fname, "rb")

                    bytes_read = file.read(405)
                    dt = np.dtype([('dummy', 'S1'),
                                   ('fmin', 'S2'),
                                   ('fmax', 'S2'),
                                   ('resolution', 'S3'),
                                   ('niveaureference', 'S3'),
                                   ('vitessebalayage', 'S5'),
                                   ('echelle', 'S2'),
                                   ('heuremeridien', 'S2'),
                                   ('minutemeridien', 'S2'),
                                   ('filtrerfdebut', 'S1'),
                                   ('heurefiltredebut', 'S5'),
                                   ('filtrerf1changement', 'S1'),
                                   ('heure1changement', 'S5'),
                                   ('filtrerf2changement', 'S1'),
                                   ('heure2changement', 'S5'),
                                   ('datemeridien', 'S10'),
                                   ('datedebutobservation', 'S9'),
                                   ('heurefinobservation', 'S6'),
                                   ('TBD', 'S340')
                                   ])

                    c = np.fromstring(bytes_read, dtype=dt)

                    entete = {
                        'dummy':  c['dummy'].astype(str)[0],
                        'fmin':  c['fmin'].astype(str)[0],
                        'fmax':  c['fmax'].astype(str)[0],
                        'resolution': c['resolution'].astype(str)[0],
                        'niveaureference':  c['niveaureference'].astype(str)[0],
                        'vitessebalayage':  c['vitessebalayage'].astype(str)[0],
                        'echelle':  c['echelle'].astype(str)[0],
                        'heuremeridien':  c['heuremeridien'].astype(str)[0],
                        'minutemeridien':  c['minutemeridien'].astype(str)[0],
                        'filtrerfdebut':  c['filtrerfdebut'].astype(str)[0],
                        'heurefiltredebut':  c['heurefiltredebut'].astype(str)[0],
                        'filtrerf1changement':  c['filtrerf1changement'].astype(str)[0],
                        'heure1changement':  c['heure1changement'].astype(str)[0],
                        'filtrerf2changement':  c['filtrerf2changement'].astype(str)[0],
                        'heure2changement':  c['heure2changement'].astype(str)[0],
                        'datemeridien':  c['datemeridien'].astype(str)[0],
                        'datedebutobservation':  c['datedebutobservation'].astype(str)[0],
                        'heurefinobservation':  c['heurefinobservation'].astype(str)[0]
                    }

                    """
                    an = int(d[0][6:8])
                    if an > 50:
                        an = an + 1900
                    else:
                        an = an + 2000

                    jour =  int(d[0][0:2])

                    mois =  int(d[0][3:5])

                    jj = datetime.datetime(an, mois, jour)


                    """
                    an = int(date.strftime("%Y"))
                    mois = int(date.strftime("%m"))
                    jour = int(date.strftime("%d"))

                    #path = jj.strftime("S%Y%m%d")
                    #path = 'S2014'

                    #t = time.Time('2010-01-01 00:00:00', format='iso', scale='utc')
                    """
                    dt = '405B'

                    dataall = np.fromfile(fname, dtype=dt)
                    dataall = np.delete(dataall, 0, axis=0)
                    data = dataall

                    datas = pd.DataFrame(data)

                    """

                    """
                    data[::2,3] = 0
                    data[1::2,3] = 1
                    data = data[:,3:]

                    z = np.arange(0,data.shape[0])
                    conditionDebut = data[:,-1] == 17
                    conditionFin = data[:,-1] == 0

                    indiceDebut = z[conditionDebut]
                    indiceFin = z[conditionFin]



                    selection0 = data[indiceDebut[0]:indiceDebut[1],:]
                    sel0_0 = selection0[selection0[:,0] == 0]
                    sel1_0 = selection0[selection0[:,0] == 1]
                    moy0_0 = np.mean(sel0_0[1:], axis=0)*80000/256
                    moy1_0 = np.mean(sel1_0[1:], axis=0)*80000/256

                    T1_0 = signal.savgol_filter(moy1_0,17,0)
                    T0_0 = signal.savgol_filter(moy0_0,17,0)


                    #plt.plot(T1_0)
                    #plt.plot(T0_0)

                    selection3 = data[indiceDebut[3]:indiceFin[1],:]

                    sel0_3 = selection3[selection3[:,0] == 0]
                    sel1_3 = selection3[selection3[:,0] == 1]
                    moy0_3 = np.mean(sel0_3[1:], axis=0)*80000/256
                    moy1_3 = np.mean(sel1_3[1:], axis=0)*80000/256

                    T1_3 = signal.savgol_filter(moy1_3,17,0)
                    T0_3 = signal.savgol_filter(moy0_3,17,0)


                    #plt.plot(T1_3)
                    #plt.plot(T0_3)


                    # scipy.signal.savgol_filter(x, window_length, polyorder, deriv=0, delta=1.0, axis=-1, mode='interp', cval=0.0)[source]¶
                    # fil = savgol(16, 16, 0, 4)
                    # Result = SAVGOL( Nleft, Nright, Order, Degree [, /DOUBLE] )


                    T1sys = (1000*T1_0 - T1_3)/999
                    T0sys = (1000*T0_0 - T0_3)/999
                    #plt.plot(T1sys)
                    #plt.plot(T0sys)

                    G1 = (T1_3 - T1_0) / 999
                    G0 = (T0_3 - T0_0) / 999

                    #plt.plot(G1)
                    #plt.plot(G0)

                    reseau1 = data[indiceFin[1]:indiceDebut[4],:]

                    #rezz = reseau1-G1
                    #plt.imshow(reseau1)


                    """

                    #JJ =  Time(DT,format='datetime',scale='utc').jd
                    #JF = Time(DF,format='datetime',scale='utc').jd

                    """

                    #print(d[0])

                    #datas["datetime"]=pd.to_datetime(d[0] + "T" +datas[0].astype(str)+":"+datas[1].astype(
                    # str)+":"+datas[2].astype(str)+"."+datas[3].astype(str), format='%d/%m/%yT%H:%M:%S.%f')
                    datas["datetime"]=pd.to_datetime(date.strftime("%d/%m/%y") + "T" +datas[0].astype(str)+":"+datas[
                        1].astype(str)+":"+datas[2].astype(str)+"."+datas[3].astype(str), format='%d/%m/%yT%H:%M:%S.%f')
                   #datas["datetime"]=pd.to_datetime(an,mois,jour ,datas[0],datas[ 1],datas[2],datas[3])
                    #datas["datetime"]=pd.to_datetime(date.strftime("%Y%m%d") + "T" +datas[0].astype(str)+":"+datas[
                    # 1].astype(str)+":"+datas[2].astype(str)+"."+datas[3].astype(str), format='"%Y%m%dT%H:%M:%S.%f')



                    ind = datas["datetime"]
                    #datas["datetime_string"] = (an + "-" + mois + "-" + jour + " " +datas[0].astype(str)+":"+datas[1].astype(
                    #    str)+":"+datas[2].astype(str)+"."+datas[3].astype(str))
                    #datas["datetime_string"] = datas["datetime"].to_timestamp()

                    polarite = np.empty((data.shape[0],1),dtype ="U")
                    polarite[::2,0]="D"
                    polarite[1::2,0]="G"

                    """

                    print("Create ")
                    # importation from file fname:
                    if old:
                        """
                        OLD DAM
                        """

                        dt = '405B'

                        # print(dt_entete.itemsize)

                        # et = fid.read(dt_entete.itemsize)

                        # entete = np.fromfile(fname,count=1, dtype=dt)
                        # entete = np.fromfile(filename,count=dt_entete.itemsize, dtype=dt_entete)

                        # print(entete)
                        # exit()
                        dataall = np.fromfile(fname, dtype=dt)

                        dataall = np.delete(dataall, 0, axis=0)

                        D = dataall

                        datas = pd.DataFrame(D)

                        #DU = datetime.timedelta(minutes=1)
                        #DT = datetime.datetime(an, mois, jour, dataall[0,0], dataall[0,1], dataall[0,2], dataall[0,3])
                        # print(DT)
                        #DF = DT + DU

                        # calcul of index
                        # INDEX=pd.to_datetime(d[0] + "T" +datas[0].astype(str)+":"+datas[1].astype(str)+":"+datas[
                        # 2].astype(str)+"."+datas[3].astype(str),
                        # format='%d/%m/%yT%H:%M:%S.%f')
                        INDEX = pd.to_datetime(
                            date.strftime("%d/%m/%y") + "T" + datas[0].astype(str) + ":" + datas[1].astype(
                                str) + ":" + datas[2].astype(str) + "." + datas[3].astype(str),
                            format='%d/%m/%yT%H:%M:%S.%f')

                        nbTable = self.NB_TABLE
                        valcol = self.VALUE_COL

                        # print(INDEX.values)
                        data = dataall.copy()

                        #DATA = data[:,coldeb:colfin]
                        # name of the column
                        #L = ['pol']
                        L = []
                        for j in np.arange(0, 400):

                            z = str(j)
                            if j < 100:
                                z = "0" + z
                                if j < 10:
                                    z = "0" + z
                            z = "f" + z
                            L.append(z)

                        # L.append("stat")
                        dt = np.dtype((str, 4))
                        Z = np.fromiter(L, dt)

                        # some change in the importation numpy
                        # D <-> 1
                        # G <-> 0

                        # data[::2,coldeb-1]="D"
                        # data[1::2,coldeb-1]="G"
                        # data[:,colfin+1]=data[:,-1]

                        polarite = np.empty((data.shape[0], 1), dtype="U")
                        polarite[::2, 0] = "D"
                        polarite[1::2, 0] = "G"

                        table = pd.DataFrame(
                            data[:, 4:404], index=INDEX, columns=Z)
                        # table["polarite"] = polarite
                        # table["status"]=data[:,-1]

                    else:

                        """
                        NEW DAM
                        """
                        #block_cubes = 65408
                        block_cubes = -1
                        offset = -1
                        filename = fname
                        print(filename)
                        # fid = open(filename,mode='r',encoding = "ISO-8859-1")
                        fid = io.FileIO(filename, mode='r')
                        print(fid)
                        name = os.path.basename(filename)
                        filesize = os.path.getsize(filename)
                        print(filesize)
                        fig_start = 1

                        # ENTETE

                        dt_entete = np.dtype([('siz_tet', 'uint32'),
                                              ('sel_prod0', 'uint32'),
                                              ('sel_prod1', 'uint32'),
                                              ('acc', 'uint32'),
                                              ('subband', 'uint32', (64)),
                                              ('Nfreq', 'uint32')])

                        # print(dt_entete.itemsize)

                        # et = fid.read(dt_entete.itemsize)

                        entete = np.frombuffer(
                            fid.read(dt_entete.itemsize), count=1, dtype=dt_entete)
                        # entete = np.fromfile(filename,count=dt_entete.itemsize, dtype=dt_entete)
                        print(entete)

                        # extrait Nprod et Nfreq

                        sel_prod = np.binary_repr(
                            entete[0]['sel_prod1'], 32) + np.binary_repr(entete[0]['sel_prod0'], 32)
                        # print(sel_prod)
                        Nprod = sel_prod.count('1')
                        sel_chan = [pos for pos, bit in enumerate(
                            reversed(sel_prod)) if bit == '1']
                        sub_band = ''
                        for i in entete[0]['subband']:
                            sub_band += np.binary_repr(i, 32)

                        Nfreq = entete[0]['Nfreq']
                        # print(Nfreq)
                        # CONTENT

                        df = fid.read(Nfreq * np.dtype('float32').itemsize)
                        f = np.frombuffer(df, dtype='float32')
                        print(f)

                        #add_rev = numpy.arange(2048)
                        dadd = fid.read(Nfreq * np.dtype('uint32').itemsize)
                        add_rev = np.frombuffer(dadd, dtype='uint32')
                        # print(add_rev)

                        # construit les structures en conséquence
                        dt_spectre = np.dtype([('magic', 'uint32'),
                                               ('no', 'uint32'),
                                               ('data', 'float32', (Nfreq))])

                        dt_time = np.dtype([('JDN', 'uint32'),
                                            ('seconds', 'uint32'),
                                            ('num_sec', 'uint32'),
                                            ('den_sec', 'uint32')])
                        dt_cube = np.dtype([('magic', 'uint32'),
                                            ('id', 'uint32'),
                                            ('date', dt_time),
                                            ('length', 'uint32'),
                                            ('status', 'uint32'),
                                            ('corr', dt_spectre, (Nprod))])
                        print(dt_cube.itemsize)
                        #nbcubes = ( filesize - dt_entete.itemsize ) / dt_cube.itemsize
                        nbcubes = int(
                            (filesize - entete[0]['siz_tet']) / dt_cube.itemsize)
                        print("nbcubes", nbcubes)

                        if offset == -1:
                            offset = nbcubes - block_cubes

                        elif offset < 0:
                            offset += nbcubes - block_cubes

                        if block_cubes == -1:
                            block_cubes = nbcubes
                            offset = 0

                        if block_cubes + offset > nbcubes or offset < 0:
                            print("Out of range data selection")
                            print("You are trying to extract data from spectrum %d to spectrum %d\nwhile only spectrae %d to %d are available" % (
                                offset, block_cubes + offset - 1, 0, nbcubes - 1))
                            raise SystemExit(0)

                        f = f.copy()
                        # f[-1]=78/2.
                        print(entete[0]['siz_tet'])

                        # get to the first cube
                        r = fid.seek(entete[0]['siz_tet'], 0)
                        print("first cube", r)
                        print("first cube", dt_cube.itemsize, block_cubes)
                        s = fid.read(dt_cube.itemsize * 10000)
                        print("coucou")
                        #s = fid.read(dt_cube.itemsize*block_cubes)
                        #s = fid.readall()
                        print("coucou")
                        super_cube = np.frombuffer(
                            s, count=10000, dtype=dt_cube)
                        print("coucou")
                        #super_cube = np.frombuffer(s, count=block_cubes, dtype=dt_cube)

                        # print(super_cube.shape,super_cube["corr"]["data"].shape,super_cube["date"].shape,super_cube['corr']['data'][:,0,:])
                        fid.close()

                    pdres = []

                    if not list(pdres):

                        print("Insert ")
                        es = Elasticsearch(['master-rsdb'])
                        es.indices.create(index='nda', ignore=400, timeout=30)

                        try:
                            # self.DB.write_points({"RD":table.iloc[::2,:]},batch_size=1000)
                            # self.DB.write_points({"RG":table.iloc[1::2,:]},batch_size=1000)

                            if old:
                                print("insert old")
                                """
                                OLD DAM
                                """
                                #cur = self.DB.cursor()
                                AT = 'OF'
                                etalonnage = False

                                for i in np.arange(0, data.shape[0]):

                                    if not i % 1000:
                                        print("ligne", i)

                                    dt = datetime.datetime(an, mois, jour, data[
                                                           i, 0], data[i, 1], data[i, 2])

                                    if i % 2:
                                        P = "d"
                                        #P = 1
                                    else:
                                        P = "g"
                                        # P=0

                                    """
                                    if data[i, -1] == 0:
                                        AT = "OF"
                                        print(AT)
                                        etalonnage = False
                                    if (data[i, -1] == 17) & (etalonnage == True):
                                        AT = AT - 10
                                        print(AT)
                                    if (data[i, -1] == 17) & (etalonnage == False):

                                        AT = 30
                                        etalonnage = True
                                        print(AT)
                                    """
                                    if (data[i, -1] == 17) & (AT == "OF"):
                                        AT = 30
                                        print(AT)
                                    elif (data[i, -1] == 17) & (AT in [10, 20, 30]):
                                        AT = AT - 10
                                        print(AT)
                                    elif data[i, -1] == 0:
                                        AT = "OF"
                                        print(AT)

                                        # print(dt)
                                    if i == 0:
                                        datedebut = dt

                                        # cur.execute(
                                        #    """SELECT id FROM rsd.stream WHERE timestamp::date = %s and status_id = %s and  flag_id = %s and  recepteur_id = %s""",
                                        #    (dt.strftime("%Y/%m/%d"),2,3,1))
                                        #r = cur.fetchone()
                                        # if r:
                                        #id = r[0]
                                        # update
                                        # cur.execute(
                                        #    """UPDATE  rsd.stream SET (timestamp, status_id, flag_id,niveau_id,recepteur_id) =
                                        #    (%s, %s, %s, %s, %s)
                                        #    WHERE id = %s
                                        #    ;""",
                                        #     ( dt,2, 3,5,1,id))
                                        # else:

                                        # cur.execute(
                                        #    """INSERT INTO rsd.stream (timestamp, status_id, flag_id,niveau_id,recepteur_id)
                                        #    VALUES (%s, %s, %s, %s, %s);""",
                                        #     ( dt,2, 3,5,1))

                                        # self.DB.commit()
                                        #timestamp_min = dt

                                        # cur.execute(
                                        #    """SELECT id FROM rsd.stream WHERE timestamp = %s and recepteur_id = %s""",
                                        #    (timestamp_min,1))
                                        #id_min = cur.fetchone()[0]
                                        # print(id_min)

                                    url = 'http://192.168.130.30:4242/api/put?details'

                                    stream = []
                                    mean = 0
                                    count = 0
                                    tab = {}

                                    for k in np.arange(0, 400):
                                        value = {}
                                        value["metric"] = "NDA.OLD"

                                        value["timestamp"] = round(
                                            dt.timestamp())
                                        value["value"] = int(data[i, k + 4])
                                        tags = {"P": P, "AT": str(
                                            AT), "V": int(k)}
                                        value["tags"] = tags
                                        mean = mean + int(data[i, k + 4])
                                        count = count + 1
                                        tab[int(k)] = int(data[i, k + 4])
                                        stream.append(value)

                                        """
                                        if T > 0:
                                            doc = {
                                                'status': T,
                                                'polarite': P,
                                                #'moyenne': mean / count,
                                                #'value':tab,
                                                'timestamp': dt
                                            }
                                        """
                                        #res = es.index(index="nda", doc_type='old',  body=doc, timeout=30 , timestamp=dt)

                                    s_in = binascii.a2b_qp(
                                        json.dumps(stream, encoding='utf8'))

                                    s_out = gzip.compress(s_in)

                                    rep = session.post(url, data=s_out)
                                    s = rep.status_code

                                entete['@timestamp'] = datedebut

                                res = es.index(
                                    index="nda", doc_type='old',  body=entete, timeout=30)
                                # print(res)

                                # cur.execute(
                                #    """SELECT id FROM rsd.stream WHERE timestamp::date = %s and status_id = %s and  flag_id = %s and recepteur_id = %s""",
                                #    (dt.strftime("%Y/%m/%d"),2,4,1))
                                #r = cur.fetchone()
                                # if r:
                                #id = r[0]
                                # update
                                # cur.execute(
                                #    """UPDATE  rsd.stream SET (timestamp, status_id, flag_id,niveau_id,recepteur_id) =
                                #    (%s, %s, %s, %s, %s)
                                #    WHERE id = %s
                                #    ;""",
                                #     ( dt,2, 4,5,1,id))
                                # else:

                                # cur.execute(
                                #    """INSERT INTO rsd.stream (timestamp, status_id, flag_id,niveau_id,recepteur_id)
                                #    VALUES (%s, %s, %s, %s, %s);""",
                                #    ( dt,2, 4,5,1))
                                #timestamp_max = dt
                                # self.DB.commit()
                                # cur.execute(
                                #    """SELECT id FROM rsd.stream WHERE timestamp = %s and recepteur_id = %s""",
                                #    (timestamp_max,1))
                                #id_max = cur.fetchone()[0]

                                # cur.execute(
                                #    """INSERT INTO rsd.catalogue (time_min, time_max)
                                #    VALUES (%s, %s);""",
                                #    (id_min,id_max ))
                                # self.DB.commit()
                                # cur.close()

                            else:

                                dd = 1357023600
                                df = 1357052400
                                JD_UNIX_TIMESTAMP = 2440587.5

                                for ligne in range(super_cube['corr']['data'].shape[0]):

                                    stream = []
                                    if not ligne % 100:
                                        print("ligne", ligne)
                                    d = (super_cube['date'][ligne]['JDN'] - JD_UNIX_TIMESTAMP) * 86400 + super_cube['date'][ligne][
                                        'seconds'] + (super_cube['date'][ligne]['num_sec'] / super_cube['date'][ligne]['den_sec'])

                                    if True:
                                        # if d > dd and d < df:

                                        for i in range(super_cube['corr']['data'].shape[1]):

                                            if i == 1 or i == 2:
                                                #ref = median(TF,axis=0)
                                                # ref = 0  #10*log10(medfilt(ref,5))
                                                # print(dt_cube.itemsize)
                                                #stream =[]
                                                # for j in
                                                # range(super_cube['corr']['data'].shape[2]):
                                                for l in np.arange(0, 1):

                                                    for j in np.arange(l * 2048, (l + 1) * 2048):

                                                        TF = float(super_cube['corr'][
                                                                   'data'][ligne, i, j])
                                                        value = {}
                                                        #stream[Z[k]] = data[i, k+4]

                                                        value[
                                                            "metric"] = "nda.new"
                                                        value["timestamp"] = round(
                                                            d, 0)
                                                        value["value"] = TF
                                                        tags = {
                                                            "p": i, "s": 0, "f": int(j)}
                                                        value["tags"] = tags

                                                        stream.append(value)
                                                        # print(stream)

                                    s_in = binascii.a2b_qp(
                                        json.dumps(stream, encoding='utf8'))

                                    s_out = gzip.compress(s_in)

                                    url = 'http://opentsdb-rsdb.obs-nancay.fr:4242/api/put'
                                    #r = session.post(url, data=json.dumps(stream))
                                    rep = session.post(url, data=s_out)

                        except ValueError:
                            print("Oops!  That was no valid number.  Try again...")

                        print("end append", datetime.datetime.now())
                    else:
                        print("not empty")
                print("Not ok", fname)

            counterDay += 1

            date = date + oneday

        response = {"message": "importation términée"}
        # self.DB.close()

        return response

    def setExport(self, f, sel):
        """

        :param f:
        :param sel:
        :return:
        """

        filtre = json.loads(f)

        # "2014-01-01T00:00:00.000Z"

        date = datetime.datetime.strptime(
            filtre["date"], "%Y-%m-%dT%H:%M:%S.%fZ")
        time = datetime.datetime.strptime(
            filtre["time"], "%Y-%m-%dT%H:%M:%S.%fZ")
        duree = int(filtre["dure"])
        frequence = filtre["frequence"]

        oneday = datetime.timedelta(days=1)
        counterDay = 0
        DU = datetime.timedelta(days=duree)
        DD = date
        DF = date + DU

        oneday = datetime.timedelta(days=1)

        counterDay = 1

        print("export")

        print("boucle date debut : ", date)
        while counterDay < duree:

            #fname = "/datadam/routine/2013/S130215.RT1"
            fdam = "S" + date.strftime("%y%m%d") + ".RT1"
            rdam = date.strftime("%Y/%m/")
            fname = "/datadam/routine_soleil/" + rdam + "/" + fdam

            self.DAY_NAME = "D" + date.strftime("%d")
            self.MONTH_NAME = "M" + date.strftime("%m")
            self.YEAR_NAME = "Y" + date.strftime("%Y")
            i = 0
            #name = "/" + self.YEAR_NAME+"/"+self.MONTH_NAME + "/"+self.DAY_NAME
            name = "S" + date.strftime("%Y%m%d")
            #file = open(self.HDF5_REP + name,"w")
            file = gzip.open(self.HDF5_REP + name + ".gz", 'wt')
            ajout = True

            print("ajout à la base ? : ", ajout, date)

            if ajout:

                if os.path.isfile(fname):
                    print("ok", fname)

                    """
                    entete
                    """

                    """
                    file = open(fname, "rb")

                    bytes_read = file.read(405)
                    dt = np.dtype([('a','S50'),('b','S8'),('c','S347')])
                    c = np.fromstring(bytes_read, dtype=dt)
                    d = c['b'].astype(str)


                    an = int(d[0][6:8])
                    if an > 50:
                        an = an + 1900
                    else:
                        an = an + 2000

                    jour =  int(d[0][0:2])

                    mois =  int(d[0][3:5])

                    jj = datetime.datetime(an, mois, jour)
                    """

                    an = int(date.strftime("%Y"))
                    mois = int(date.strftime("%m"))
                    jour = int(date.strftime("%d"))

                    #path = jj.strftime("S%Y%m%d")
                    #path = 'S2014'

                    #t = time.Time('2010-01-01 00:00:00', format='iso', scale='utc')

                    dt = '405B'

                    data = np.fromfile(fname, dtype=dt)
                    data = np.delete(data, 0, axis=0)

                    datas = pd.DataFrame(data)

                    """
                    data[::2,3] = 0
                    data[1::2,3] = 1
                    data = data[:,3:]

                    z = np.arange(0,data.shape[0])
                    conditionDebut = data[:,-1] == 17
                    conditionFin = data[:,-1] == 0

                    indiceDebut = z[conditionDebut]
                    indiceFin = z[conditionFin]



                    selection0 = data[indiceDebut[0]:indiceDebut[1],:]
                    sel0_0 = selection0[selection0[:,0] == 0]
                    sel1_0 = selection0[selection0[:,0] == 1]
                    moy0_0 = np.mean(sel0_0[1:], axis=0)*80000/256
                    moy1_0 = np.mean(sel1_0[1:], axis=0)*80000/256

                    T1_0 = signal.savgol_filter(moy1_0,17,0)
                    T0_0 = signal.savgol_filter(moy0_0,17,0)


                    #plt.plot(T1_0)
                    #plt.plot(T0_0)

                    selection3 = data[indiceDebut[3]:indiceFin[1],:]

                    sel0_3 = selection3[selection3[:,0] == 0]
                    sel1_3 = selection3[selection3[:,0] == 1]
                    moy0_3 = np.mean(sel0_3[1:], axis=0)*80000/256
                    moy1_3 = np.mean(sel1_3[1:], axis=0)*80000/256

                    T1_3 = signal.savgol_filter(moy1_3,17,0)
                    T0_3 = signal.savgol_filter(moy0_3,17,0)


                    #plt.plot(T1_3)
                    #plt.plot(T0_3)


                    # scipy.signal.savgol_filter(x, window_length, polyorder, deriv=0, delta=1.0, axis=-1, mode='interp', cval=0.0)[source]¶
                    # fil = savgol(16, 16, 0, 4)
                    # Result = SAVGOL( Nleft, Nright, Order, Degree [, /DOUBLE] )


                    T1sys = (1000*T1_0 - T1_3)/999
                    T0sys = (1000*T0_0 - T0_3)/999
                    #plt.plot(T1sys)
                    #plt.plot(T0sys)

                    G1 = (T1_3 - T1_0) / 999
                    G0 = (T0_3 - T0_0) / 999

                    #plt.plot(G1)
                    #plt.plot(G0)

                    reseau1 = data[indiceFin[1]:indiceDebut[4],:]

                    #rezz = reseau1-G1
                    #plt.imshow(reseau1)


                    """

                    # print(d[0])

                    # datas["datetime"]=pd.to_datetime(d[0] + "T" +datas[0].astype(str)+":"+datas[1].astype(
                    # str)+":"+datas[2].astype(str)+"."+datas[3].astype(str),
                    # format='%d/%m/%yT%H:%M:%S.%f')
                    datas["datetime"] = pd.to_datetime(date.strftime("%d/%m/%y") + "T" + datas[0].astype(str) + ":" + datas[
                                                       1].astype(str) + ":" + datas[2].astype(str) + "." + datas[3].astype(str), format='%d/%m/%yT%H:%M:%S.%f')
                   #datas["datetime"]=pd.to_datetime(an,mois,jour ,datas[0],datas[ 1],datas[2],datas[3])
                    # datas["datetime"]=pd.to_datetime(date.strftime("%Y%m%d") + "T" +datas[0].astype(str)+":"+datas[
                    # 1].astype(str)+":"+datas[2].astype(str)+"."+datas[3].astype(str),
                    # format='"%Y%m%dT%H:%M:%S.%f')

                    # datas["datetime_string"] = (an + "-" + mois + "-" + jour + " " +datas[0].astype(str)+":"+datas[1].astype(
                    #    str)+":"+datas[2].astype(str)+"."+datas[3].astype(str))
                    #datas["datetime_string"] = datas["datetime"].to_timestamp()

                    polarite = np.empty((data.shape[0], 1), dtype="U")
                    polarite[::2, 0] = "D"
                    polarite[1::2, 0] = "G"
                    datas["polarite"] = polarite

                    CSV = datas.to_csv()
                    file.write(CSV)

                    print("Export")

                    try:

                        """
                        This function returns a Python's Pandas Timeseries object with data fetched from OpenTSDB basing on the provided parameters.
                        If there are no results it returns an empty Pandas Series, in case of any other exception it throws that exception.

                        Parameters:
                        metric - metric name as in OpenTSDB, one metric only, e.g. 'cipsi.test1.temperature'
                        start, end - start and end time for the query, should be of type datetime from datetime module, e.g. dt.datetime(2013, 4, 3, 14, 10), assuming: import datetime as dt
                        tags - tags formatted according to OpenTSDB specification e.g. 'host=foo,type=user|system'
                        agg - aggregate function to be used, default is 'avg', options are min, sum, max, avg
                        rate - specifies if rate should be calculated instead of raw data, default False
                        downsample - specifies downsample function and interval in OpenTSDB format, default none, e.g. '60m-avg'
                        trim - specifies if values received from OpneTSDB should be trimed to exactly match start and end parameters, OpenTSDB by default returns additional values before the start and after the end
                        hostname - address of OpenTSB installation, default localhost
                        port - port of OpenTSDB installation, default 4242

                        Example usage:
                        import opentsdb_pandas as opd
                        import datetime as dt
                        ts1 = opd.ts_get('cipsi.test1.temperature', dt.datetime(2013, 4, 3, 14, 10), dt.datetime(2013, 4, 10, 11, 30), 'node=0024C3145172746B', hostname='opentsdb.at.your.place.edu')
                        ts1

                        <metric> <timestamp> <value> <tagk=tagv> [<tagkN=tagvN>]

                        """
                        """
                        etalonnage = False

                        for i in np.arange(data.shape[0]):


                            if i % 2:
                                P = "D"
                            else:
                                P = "G"

                            if data[i, -1] == 17 or etalonnage:
                                etalonnage = True
                                T = "ETALONNAGE"
                            else:
                                T = "BRUT"
                            if data[i, -1] == 0:
                                etalonnage = False
                                T = "BRUT"

                            dt = datetime.datetime(an, mois, jour, data[i, 0], data[i, 1], data[i, 2])

                            for l in np.arange(0, 8):
                                stream = []
                                for k in np.arange(l*50, (l + 1)*50):
                                    value = ""
                                    #stream[Z[k]] = data[i, k+4]

                                    value += "routine.old "

                                    value += str(round(dt.timestamp()))+ " "
                                    value += str(data[i, k + 4])+ " "
                                    tags = "polarite="+ P + " status="+ T+" frequence="+str(k)
                                    value += tags
                                    file.write( value + "\n");


                        """

                    except ValueError:
                        print("Oops!  That was no valid number.  Try again...")

                    print("end append", datetime.datetime.now())
                else:
                    print("not empty")
            file.close()
            counterDay += 1

            date = date + oneday

        response = {"message": "importation términée"}

        return response
