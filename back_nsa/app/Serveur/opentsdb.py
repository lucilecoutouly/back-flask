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
from astropy.time import Time
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
import pytz
from matplotlib import *
from mpl_toolkits import *
from mpl_toolkits.axes_grid1 import make_axes_locatable
import mpl_toolkits.axisartist as AA
import os.path
import matplotlib as mpl
from mpl_toolkits.axes_grid.axislines import Subplot
mpl.use('Agg')
from influxdb import client as influxdb
from influxdb import DataFrameClient
import matplotlib.cm as cm
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import psycopg2
from elasticsearch import Elasticsearch
from PIL import Image
import PIL
import PIL.ImageOps

class opentsdb(object):

    MAX_SPECTRE = 20
    NB_TABLE = range(0, 1)
    VALUE_COL = 400
    HOST = "localhost"
    PORT = 8086
    USERNAME = "root"
    PASSWORD = "root"
    DATABASE = "rsd"

    URL = 'http://opentsdbr-rsdb:4242/api/query'
    URL = 'http://192.168.130.10:4242/api/query'
    #DB =psycopg2.connect(database="postgres", user="postgres", password="user", host="nosql")

    def getRequest(self, filtre, type, frequence):
        """
        :param filtre:
        :param type:
        :param frequence:
        :return:
        """

        if type == "canvas":
            return self.getCanvas(filtre, frequence)
        elif type == "setImport":
            return self.setImport(filtre, frequence)
        elif type == "export":
            return self.setExport(filtre, frequence)
        elif type == "view":
            return self.getView(filtre, frequence)
        elif type == "query":
            return self.Query(filtre, frequence)
        elif type == "getImage":
            return self.getImage(filtre, frequence)
        elif type == "flux":
            return self.getFlux(filtre, frequence)
        elif type == "json":
            return self.getJson(filtre, frequence)
        elif type == "data":
            return self.getData(filtre, frequence)

        elif type == "index":
            return self.setIndex(filtre, frequence)
        elif type == "getQuery":
            return self.getQuery(filtre, frequence)
        elif type == "setQuery":
            return self.setQuery(query)
        elif type == "getDate":
            return self.getDate(frequence)
        elif type == "setImportbyDay":
            return self.setImportbyDay(filtre, frequence)

    def getQuery(self, f, sel):
        #filtre = json.loads(f)
        filtre = f
        print(filtre)

        # date = datetime.datetime.strptime(filtre["date"], "%Y-%m-%dT%H:%M:%S.%f")

        # "2005-12-19T09:18:25.060+0100"
        datedeb = datetime.datetime.strptime(
            filtre["datedeb"], "%Y-%m-%dT%H:%M:%S.%f%z")

        datefin = datetime.datetime.strptime(
            filtre["datefin"], "%Y-%m-%dT%H:%M:%S.%f%z")
        print("coucou")
        #duree = filtre["dure"]
        frequence = filtre["frequence"]
        recepteur = filtre["recepteur"]
        print("coucou")
        # integration = (filtre["integration"])
        integration = str(filtre["integration"]) + 's'
        #tags = filtre["tags"]
        tags = {"coor": ".*"}

        #fRH = ["2d" + datedeb.strftime("%y%m%d") +".01", "2d" + datedeb.strftime("%y%m%d") +".01Z", "2d" + datedeb.strftime("%y%m%d") +".01ZZ"]

        #sel = filtre["sel"]

        """
        fichier à utiliser en fonction de la date
        """

        find = False
        files = []
        timehdeb = []
        timehfin = []
        unixdatedeb = time.mktime(datedeb.timetuple())
        unixdatefin = time.mktime(datefin.timetuple())

        """
        {
            "start": 1356998400,
            "end": 1356998460,
            "queries": [
                {
                    "aggregator": "sum",
                    "metric": "sys.cpu.0",
                    "rate": "true",
                    "filters": [
                        {
                           "type":"wildcard",
                           "tagk":"host",
                           "filter":"*",
                           "groupBy":true
                        },
                        {
                           "type":"literal_or",
                           "tagk":"dc",
                           "filter":"lga|lga1|lga2",
                           "groupBy":false
                        },
                    ]
                },
                {
                    "aggregator": "sum",
                    "tsuids": [
                        "000001000002000042",
                        "000001000002000043"
                      ]
                    }
                }
            ]
        }

        {
            "msResolution": "true",
            "end": 1383567360,
            "start": 1383545700,
            "queries": [
                {
                    "downsample": "1s-avg",
                    "metric": "nsa",
                    "filters": [
                        {
                            "tagk": "frequence",
                            "type": "wildcard",
                            "groupBy": "true",
                            "filter": "*"
                        }, {
                            "tagk": "obs",
                            "type": "wildcard",
                            "groupBy": "true",
                            "filter": "1"
                        }
                    ],
                    "aggregator": "avg",
                    "rate": "false"
                }
            ]
        }
        """
        agg = "avg"

        # http://nosql:4242/api/query?start=1359702000&end=1359730800&m=avg:60s-avg:routine.old{polarite=1,status=0,frequence=*}

        QUERY = []
        F = [{"tagk": "coor",
              "type": "regexp",
              "groupBy": "true",
              "filter": ".*"
              }]
        q = {
            "aggregator": agg,
            "downsample": integration + "-" + agg,

            "metric": "nrh.ew",
            "rate": "false"

        }

        print(q)
        """
        T =  {}
        print ("TAGS", tags)
        for index, key in enumerate(tags):
            print (index, key)
            #if key["tag"] != "frequence":
            if key["value"] != "":
                T[key["tag"]] =  key["value"]
            #else:
            #    T[key["tag"]] =  "*"
        print (T)
        q["tags"]=T
        """
        """
        F = []

        for index, key in enumerate(tags):
            print (index, key)
            S = {}
            if key["value"] != "":

                S = {
                        "id":index,
                       "type":"regexp",
                       "tagk":key["tag"]
                    }
                if key["tag"] == "canal":
                    #print (key["tag"] )
                    S["filter"]=key["value"]
                    S["groupBy"]= True
                else:

                    S["filter"]= key["value"]
                    #print (key["tag"] )
                    S["groupBy"]= False

                F.append(S)
        print (F)
        """

        #print (q)
        q["filters"] = F
        QUERY.append(q)


        payload = {
            "start": round(unixdatedeb),
            "end": round(unixdatefin),
            "msResolution": "true",
            "queries": QUERY
        }

        print(json.dumps(payload))

        return {'query': json.dumps(payload), "status": "success",
                "message": "query set succesfully.", "recepteur": sel}

    def setResponse(self, r, f):
        #filtre = json.loads(f)
        filtre = f
        # date = datetime.datetime.strptime(filtre["date"], "%Y-%m-%dT%H:%M:%S.%f")

        # "2005-12-19T09:18:25.060+0100"
        datedeb = datetime.datetime.strptime(
            filtre["datedeb"], "%Y-%m-%dT%H:%M:%S.%f%z")

        datefin = datetime.datetime.strptime(
            filtre["datefin"], "%Y-%m-%dT%H:%M:%S.%f%z")

        #duree = filtre["dure"]
        frequence = filtre["frequence"]
        recepteur = filtre["recepteur"]
        integration = (filtre["integration"])
        integration = str(filtre["integration"]) + 's'
        #fRH = ["2d" + datedeb.strftime("%y%m%d") +".01", "2d" + datedeb.strftime("%y%m%d") +".01Z", "2d" + datedeb.strftime("%y%m%d") +".01ZZ"]

        #sel = filtre["sel"]

        """
        fichier à utiliser en fonction de la date
        """

        find = False
        files = []
        timehdeb = []
        timehfin = []
        unixdatedeb = time.mktime(datedeb.timetuple())
        unixdatefin = time.mktime(datefin.timetuple())

        """
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
        """
        find = "avg"
        #fmin = int(frequence[0])
        #fmax = int(frequence[1])
        print ("coucou")
        fmin = 0
        fmax = 1000

        if int(recepteur):
            metric = "new"
        else:
            metric = "old"

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

        result = r.json()

        """
        [{
          'tags': {
            'int': '120',
            'coor': '-2.0',
            'ori': 'ew',
            'f': '4320'
          },
          'aggregateTags': [],
          'metric': 'nrh.ew',
          'dps': {
            '975665030000': 0.7747431993484497,
            '975665150000': 0.9129600524902344,
            '975664910000': 0.8299075961112976,
            '975665270000': 0.917038083076477,
            '975665390000': 0.685206949710846
          }
        }, {
          'tags': {
            'int': '120',
            'coor': '-1.9375',
            'ori': 'ew',
            'f': '4320'
          },
          'aggregateTags': [],
          'metric': 'nrh.ew',
          'dps': {
            '975665030000': 1.0352449417114258,
            '975665150000': 0.8340571522712708,
            '975664910000': 2.354736804962158,
            '975665270000': 0.8720930814743042,
            '975665390000': 0.7848808765411377
          }
        },

        """
        V = {}

        found = False

        R = []
        Q = []
        T = []
        I = []
        for r in result:

            L = {}
            # c : coordonnée
            c = float(r["tags"]["coor"])


            for k in r["dps"]:

                # k : timestamp
                print(int(k))
                DD = datetime.datetime.utcfromtimestamp(int(k))
                print(DD)
                # v  : value
                v = r["dps"][k]

                # construction du tableau

                L[DD] = v

                # Index

                I.append(k)

            # colonne

            C.append(c)


            V[c] = L
            found = True
        print(V)
        Z = pd.DataFrame(V)
        print(Z)

        """
        -2.0000   -1.9375   -1.8750   -1.8125   -1.7500   -1.6875  \
        975664910000  0.829908  2.354737  2.914107  2.175781  1.408059  1.211072
        975665030000  0.774743  1.035245  2.275561  2.390887  2.120759  1.232770
        975665150000  0.912960  0.834057  0.872268  1.606465  1.915806  1.515949
        975665270000  0.917038  0.872093  0.824532  0.961632  1.473786  1.303180
        975665390000  0.685207  0.784881  0.798467  0.820027  2.138857  1.757386

                      -1.6250   -1.5625   -1.5000   -1.4375    ...       1.3750  \
        975664910000  1.270714  1.338905  1.418045  1.501759    ...     1.960769
        975665030000  1.192974  1.255545  1.327793  1.407040    ...     2.205446
        975665150000  1.069529  1.136217  1.207996  1.282228    ...     2.003558
        975665270000  1.106029  1.171112  1.242478  1.319135    ...     2.422592
        975665390000  1.049710  1.116843  1.180608  1.242529    ...     2.204581

                       1.4375    1.5000    1.5625    1.6250    1.6875    1.7500  \
        975664910000  1.892398  1.885292  1.879483  1.866782  1.842157  1.802687
        975665030000  2.120214  2.012478  1.903418  1.902651  1.890632  1.862794
        975665150000  1.890757  1.750261  1.692321  1.687899  1.673673  1.643128
        975665270000  2.288702  2.099301  1.875765  1.629042  1.544657  1.501634
        975665390000  2.061218  1.871791  1.645554  1.401753  1.184763  1.129644

                       1.8125    1.8750    1.9375
        975664910000  1.742810  1.657852  1.617173
        975665030000  1.813921  1.739511  1.858618
        975665150000  1.588596  1.518444  1.419743
        975665270000  1.445254  1.369704  1.286720
        975665390000  1.081697  1.027339  0.972953
        """

        return {'result': result, 'found': found, 'DF': Z}

    def Query(self, f, sel):

        headers = {
            "content-type": "application/json"
            #,"Content-Encoding": "gzip"
        }
        #print(headers)
        #payload = {'key1': 'value1', 'key2': 'value2'}
        url = self.URL
        #print(url)
        req = self.getQuery(f, sel)
        print(url)
        query = req["query"]

        r = requests.post(url, data=query, headers=headers)
        print(r)
        return self.setResponse(r, f)

    def getImage(self, f, sel):
        """
        :param f:
        :param sel:
        :return:
        """

        #recepteur = filtre["recepteur"]
        print("coucou")
        #filtre = json.loads(f)

        filtre =f
        print("coucou",filtre)
        # date = datetime.datetime.strptime(filtre["date"], "%Y-%m-%dT%H:%M:%S.%f")
        print("coucou",filtre["datedeb"])
        # "2005-12-19T09:18:25.060+0100"
        datedeb = datetime.datetime.strptime(
            filtre["datedeb"], "%Y-%m-%dT%H:%M:%S.%f%z")

        datefin = datetime.datetime.strptime(
            filtre["datefin"], "%Y-%m-%dT%H:%M:%S.%f%z")

        #duree = filtre["dure"]
        frequence = filtre["frequence"]
        recepteur = filtre["recepteur"]
        integration = (filtre["integration"])
        integration = str(filtre["integration"]) + 's'
        #fRH = ["2d" + datedeb.strftime("%y%m%d") +".01", "2d" + datedeb.strftime("%y%m%d") +".01Z", "2d" + datedeb.strftime("%y%m%d") +".01ZZ"]

        #sel = filtre["sel"]

        """
        fichier à utiliser en fonction de la date
        """

        find = False
        files = []
        timehdeb = []
        timehfin = []
        unixdatedeb = time.mktime(datedeb.timetuple())
        unixdatefin = time.mktime(datefin.timetuple())

        """
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
        """

        #fmin = int(frequence[0])
        #fmax = int(frequence[1])
        fmin = 0
        fmax = 1000

        if int(recepteur):
            metric = "new"
        else:
            metric = "old"

        fig = plt.figure()
        fig.clf()

        #ax1 = fig.add_subplot(1,1, 1)

        #ax2 = ax1.twinx()
        #t = np.linspace(0., 10., 100)
        # ax1.plot(t, t ** 2, 'b-')
        #ax2.plot(t, 1000 / (t + 1), 'r-')

        # ax1.set_ylabel('Density (cgs)', color='red')
        # ax2.set_ylabel('Temperature (K)', color='blue')
        #ax1.set_xlabel('Time (s)')
        print("coucou1")
        resp = self.Query(f, sel)
        print("coucou2")
        if resp["found"]:
            #result = resp["DF"]
            if metric == "old":
                result = resp["DF"]
            if metric == "new":
                result = resp["DF"]


            try:
                file = str(uuid.uuid4()) + '.png'

                url = '/Public/Images/' + file
                uri = '/var/www/html' + url

                data = result.transpose()
                ax = sns.heatmap(data,cmap= cm.Greys,square=True,xticklabels=60,yticklabels=20)

                # turn the axis label

                for item in ax.get_yticklabels():
                    item.set_rotation(0)

                for item in ax.get_xticklabels():
                    item.set_rotation(90)

                # save figure
                plt.savefig(uri, dpi=100)
                plt.show()
                exit()

                T = numpy.asarray(result)
                print(T.shape)
                fig, ax = plt.subplots()
                heatmap = ax.pcolor(T)
                ax.set_yticks(np.arange(T.shape[0])+0.5, minor=False)
                ax.set_xticks(np.arange(T.shape[1])+0.5, minor=False)


                #ax.set_xticklabels(row_labels, minor=False)
                #ax.set_yticklabels(column_labels, minor=False)
                #ax.show()
                #plt.savefig(uri, format='png',  vmin=T.min(), vmax=T.max(), dpi=1600)
                plt.imsave(uri, T, format='png',vmin=T.min(), vmax=T.max(),cmap = cm.Greys , dpi=1600);
                #plt.imsave(uri, T,format='png')
                #exit()
                """
                plt.imshow(a, cmap='hot', interpolation='nearest')
                heatmap = plt.pcolor(data)
                res = result.copy(deep=True)

                T = numpy.asarray(result)
                #Z /= (Z.max()/255.0)
                Z = numpy.transpose(T)
                if Z.min()<0:
                    Z -= (Z.min())
                print("max",Z.max())
                print("min",Z.min())
                Z *= (255/Z.max())
                print("max",Z.max())
                print("min",Z.min())
                """

                #i = plt.figure("heatmap")


                #cm = ImageCms.createProfile(“sRGB”, colorTemp=-1)

                #im = Image.fromarray(np.uint8(cm.gist_earth(Z)*255))
                """
                im = Image.fromarray(np.uint8(Z),"L")
                out = im.transpose(Image.FLIP_TOP_BOTTOM)
                inverted_image = PIL.ImageOps.invert(out)
                w, h = inverted_image.size
                print(w,h)
                baseheigh = 100
                #img = Image.open('somepic.jpg')

                wpercent = (baseheigh/h)
                print(wpercent)


                wsize = int((float(w)*float(wpercent)))
                print(wsize)
                inverted_image= inverted_image.resize((wsize,baseheigh), PIL.Image.ANTIALIAS)

                #img.save('sompic.jpg')



                inverted_image.save(uri)



                #exit()
                #plt.savefig(uri, format='png',  vmin=0, vmax=255, dpi=1600)
                #extent = (-3, 4, -4, 3)
                #im = i.imshow(Z, extent=extent, interpolation="nearest")
                #plt.setp(cb.i.get_yticklabels(), visible=False)
                #cb = plt.colorbar(im)
                #plt.imsave(uri, Z, format='png', cmap=cm.Greys)
                #plt.savefig(uri, format='png',  vmin=0, vmax=255, dpi=1600)



                #fig = plt.figure(1, (3,3))
                #ax = Subplot(fig, 111)
                #fig.add_subplot(ax)
                #ax = AA.Axes(fig, [0.1, 0.1, 0.8, 0.8])
                #fig.add_axes(ax)
                #fig = plt.figure()
                #ax1 = fig.add_axes([0.05, 0.80, 0.9, 0.15])


                #ax = fig.add_subplot(2,1,1)
                #divider = make_axes_locatable(fig)

                #fig_cb = divider.new_horizontal(size="5%", pad=0.05)
                #fig1 = fig.get_figure()
                #fig1.add_axes(fig_cb)
                #im = ax.imshow(Z)

                #cb = plt.colorbar(im)
                #plt.setp(cb.ax.get_yticklabels())
                #cax = divider.append_axes("right", size="5%", pad=0.05)

                #plt.colorbar(im, cax=cax)
                #plt.colorbar(im, cax=fig_cb)
                #fig_cb.yaxis.tick_right()
                #for tl in fig_cb.get_yticklabels():
                #    tl.set_visible(False)
                #fig_cb.yaxis.tick_right()

                #norm = mpl.colors.Normalize(vmin=0, vmax=1)

                #cb1 = mpl.colorbar.ColorbarBase(
                #    ax1, cmap="spectral", norm=norm, orientation='horizontal')
                #cb1.set_label('Échelle')

                #filec = str(uuid.uuid4()) + '.png'
                #urlc = '/Public/Images/' + filec
                #uric = '/var/www/html' + urlc
                #plt.draw()
                #plt.show()
                #plt.savefig(uri, format='png')
                #imc = Image.open(uric)
                #widthc, heightc = imc.size
                #plt.imsave(uri, Z, format='png',vmin=0, vmax=1,cmap = cm.Greys_r , dpi=1600);
                #im = Image.open(uri)
                """
                #width, height = inverted_image.size
                width, height = T.shape
                print(width,height)





            except:
                print("Unexpected error:", sys.exc_info()[0])

            """
            try:

                column_labels = list('ABCD')
                row_labels = list('WXYZ')
                data = np.random.rand(4,4)

                fig, ax = plt.subplots()

                #heatmap = ax.pcolor(data, cmap=plt.cm.Blues)
                heatmap = ax.pcolor(Z, cmap=plt.cm.Greys_r)

                # put the major ticks at the middle of each cell
                ax.set_xticks(np.arange(data.shape[0])+0.5, minor=False)
                ax.set_yticks(np.arange(data.shape[1])+0.5, minor=False)

                # want a more natural, table-like display
                ax.invert_yaxis()
                ax.xaxis.tick_top()

                ax.set_xticklabels(row_labels, minor=False)
                ax.set_yticklabels(column_labels, minor=False)
                plt.show()

                plt.savefig(uri, format='png' )

            except:
                print ("Unexpected error:", sys.exc_info()[0])
            """
            # plt.show()
            # exit()
            """
            #data = [go.Heatmap( z=result.values.tolist(), colorscale='spectral')]
            #print(data)
            #print(uri)
            #py.iplot(data, filename=uri)
            #im.size
            #plt.imsave(uri,Z , vmin=0, vmax=255, format='png', dpi=1600);
            #plt.draw()

            file = "NoData" + '.png'
            url = '/Prive/Images/' + file
            uri = '/var/www/html' + url
            """

        else:
            print("coucou1")
            file = "nodata" + '.png'
            url = '/Public/' + file
            uri = '/var/www/html' + url
            im = Image.open(uri)
            width, height = im.size
            json = {}
            print("coucou")
            #exit()

            #exit()

        response = {
            "status": "success",
            "message": "Image loaded succesfully.",
            "recepteur": sel,
            "type": "img",
            "width": width,
            "height": height,
            #"widthc": widthc,
            #"heightc": heightc,
            "url": url,
            #"urlc": urlc,
            "json": result.to_json(orient='split', date_format='iso')
            #"json":result.to_json(orient='index',date_format='iso')

            #"json":indexed_df.to_json(orient='split',date_format='iso')
            #"json":result

        }
        #exit()
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
                # print(SEL)

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

        url = 'http://nosql:4242/api/query'
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

    def getJson(self, f, sel):
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
        url = 'http://opentsdb-nsadb:4242/api/query'

        req = self.getQuery(f, sel)
        query = req["query"]

        r = requests.post(url, data=query, headers=headers)
        result = r.json()

        """
        if resp["found"]:
            #print(result)
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
                res.append({'key': DD.strftime("%Y-%m-%d %H:%M:%S"), 'values': values})
                ligne += 1
                #res.append({'values':{'x':row_index,'y':row}})


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
        """

        response = {
            "status": "success",
            "message": "Image loaded succesfully.",
            "recepteur": sel,
            "type": "img",
            "data": result
        }

        return response

    def getData(self, f, sel):
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
            """
            for row_index, row in result.iterrows():


                i = 0

                values = []
                for y in row:
                    if i < 400:
                        values.append(round( y))
                    i += 1
                DD = datetime.datetime.fromtimestamp(int(row_index))
                res.append(values)
                ligne += 1
                #res.append({'values':{'x':row_index,'y':row}})
            """

            for row_index, row in result.iterrows():

                i = 0

                values = []
                DD = datetime.datetime.fromtimestamp(int(row_index))
                for y in row:

                    if i < 400:
                        #values.append({'x': i, 'y': y})
                        # values.append(y)

                        res.append({'datetime': DD.strftime(
                            "%Y-%m-%d %H:%M:%S"), 'frequence': i, 'value': round(y, 2)})
                    i += 1

                # res.append({'key': DD.strftime("%Y-%m-%d %H:%M:%S"), 'values': values})
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

    def setImportbyDay(self, f, sel):
        """

        :param f:
        :param sel:
        :return:
        """

        # logging.basicConfig(filename='/data/var_nsa/nrh.import4.log',level=logging.DEBUG,\
        #    format='%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s')

        filtre = json.loads(f)
        datedeb = datetime.datetime.strptime(
            filtre["datedeb"], "%Y-%m-%dT%H:%M:%S.%f%z")

        #datefin = datetime.datetime.strptime(filtre["datefin"], "%Y-%m-%dT%H:%M:%S.%fZ")
        duree = int(filtre["dure"])
        frequence = filtre["frequence"]
        recepteur = filtre["recepteur"]

        oneday = datetime.timedelta(days=1)
        counterDay = 0
        DU = datetime.timedelta(days=duree)

        DD = datedeb
        DF = datedeb + DU

        oneday = datetime.timedelta(days=1)

        counterDay = 1

        #es = Elasticsearch(['master-nsasdb'])
        #es = Elasticsearch(['192.168.140.2'])
        es = Elasticsearch(['master-nsadb'])

        es.indices.create(index='nsa', ignore=400)

        #idl = pidly.IDL('/usr/local/bin/idl',long_delay=0.05)
        url = 'http://opentsdb-nsadb:4242/api/put'
        print("boucle date debut : ", datedeb)

        # pd.set_option('io.hdf.default_format','table')

        if int(recepteur):
            metric = "new"

            old = 0
        else:

            print(int(recepteur))
            metric = "old"
            old = 1
        print(metric)
        oneday = datetime.timedelta(days=1)

        counterDay = 1

        print("import")

        #session = FuturesSession(max_workers=2)
        session = requests.Session()
        headers = {
            "Content-Type": "application/json", "Content-Encoding": "gzip"
        }
        print("coucou")
        session.headers.update(headers)

        print("boucle date debut : ", datedeb, counterDay, duree)
        while counterDay < duree:

            if old:
                #fname = "/datadam/routine/2013/S130215.RT1"
                #/data/data_nsa/old/rh/2014/

                # RH15031400.DAT
                print("coucou")
                fnsa = "RH" + datedeb.strftime("%y%m%d") + "00.DAT"
                rnsa = datedeb.strftime("%Y")
                typ = "rh"
                fname = "/data/data_nsa/old/" + typ + "/" + rnsa + "/" + fnsa
                print("coucou", fname)
                #fname = "/datadam/routine_soleil/" + fdam
            else:
                fdam = date.strftime("%Y%m%d") + "_000000.dat"
                fname = "/datadam/Routine_New/" + fdam

            ajout = True

            print("ajout à la base ? : ", ajout)

            if ajout:

                if os.path.isfile(fname):
                    print("ok", fname)

                    print("Create ")
                    # importation from file fname:
                    if old:
                        """
                        OLD NSA
                        """

                        #dt = '405B'
                        dt = np.dtype([('dat', 'uint16', (3)),
                                       ('h', 'uint16', (3)),
                                       ('nb1', 'B'),
                                       ('ah', 'S', (10)),
                                       ('nb2', 'B'),
                                       ('az', 'S10', (1)),
                                       ('integ', 'B'),
                                       ('nb3', 'B'),
                                       ('detect', 'S', (8)),
                                       ('spectre', '>i2', (701)),
                                       ('nul1', 'int8', (61)),
                                       ('nbdiv', 'c'),
                                       ('dbdiv', 'c'),
                                       ('yunit', 'c'),
                                       ('mode', 'c'),
                                       ('ampl', 'c'),
                                       ('nul2', 'int8', (3)),
                                       ('comment', 'S', (155)),

                                       #('param',('S',(18)),10)
                                       ('param', [
                                        ('n', 'uint8', (1)), ('nb', 'S', (17))], 10)
                                       ])

                        data = np.fromfile(fname, dtype=dt)

                        #np.dtype([('f1', [('f1', np.int16)])])
                        #np.dtype([('f1', np.uint), ('f2', np.int32)])

                    else:

                        """
                        NEW NSA
                        """

                    pdres = []

                    if not list(pdres):

                        print("Insert ")
                        #es = Elasticsearch(['193.55.144.48'])
                        #es.indices.create(index='nsa', ignore=400, timeout=30)
                        try:

                            if old:

                                """
                                OLD NSA
                                """
                                obs = 0
                                frqcent = {}
                                params = ["niv-ref", "frq-cent", "span", "band-res",
                                          "vbw", "att", "swp", "niv-marq", "niv-marq", "frq-marq"]
                                #params = ["niv-ref","frq-cent","span","band-res","vbw","att","swp"]
                                for i in np.arange(data.shape[0]):

                                    # print(i,data['dat'][i][0])

                                    # print(i,data['h'][i][0])
                                    # print(i,data['nb1'][i])
                                    # print(i,float(data['ah'][i]))
                                    # print(i,float(data['az'][i]))
                                    # print(i,data['integ'][i])
                                    # print(i,str(data['detect'][i]))
                                    # print(i,data['spectre'][i])
                                    # print(i,int(data['nbdiv'][i]))
                                    # print(i,data['dbdiv'][i])
                                    # print(i,data['yunit'][i])
                                    # print(i,data['mode'][i])
                                    # print(i,data['ampl'][i])
                                    # print(i,data['comment'][i])
                                    # print(i,data['param'][i])

                                    dd = datetime.datetime(data['dat'][i][0], data['dat'][i][1], data['dat'][i][
                                                           2], data['h'][i][0], data['h'][i][1], data['h'][i][2], 0, pytz.UTC)
                                    # datetime.datetime(idl.dat[2],idl.dat[1],
                                    # idl.dat[0],idl.hdeb[0],idl.hdeb[1],
                                    # idl.hdeb[2], idl.hdeb[3]*1000, pytz.UTC)}

                                    # print(dt)

                                    if not i % 1000:
                                        print("ligne", i)

                                    #

                                    for l in np.arange(0, 1):
                                        stream = []
                                        for k in np.arange(l * 701, (l + 1) * 701):
                                            value = {}
                                            K = str(k)
                                            if int(k) < 1000:
                                                K = "0" + K
                                                if int(k) < 100:
                                                    K = "0" + K
                                                    if int(k) < 10:
                                                        K = "0" + K
                                            #K = "C" + K

                                            value["metric"] = "nsa"
                                            """
                                            value["metric"] = "nsa.old.nbdiv"+\
                                            str(int(data['nbdiv'][i]))+\
                                            ".dbdiv"+str(int(data['dbdiv'][i]))+\
                                            ".yunit"+str(int(data['yunit'][i]))+\
                                            ".mode"+str(int(data['mode'][i]))+\
                                            ".ampl"+str(int(data['ampl'][i]))
                                            #print(value)
                                            """

                                            value["timestamp"] = round(
                                                dd.timestamp())
                                            value["value"] = int(
                                                data['spectre'][i][k])
                                            tags = {
                                                "type": typ,
                                                "ah": float(data['ah'][i]),
                                                "az": float(data['az'][i]),
                                                #"nbdiv":int (data['nbdiv'][i]),
                                                #"dbdiv": int(data['dbdiv'][i]),
                                                #"yunit": int(data['yunit'][i]),
                                                #"mode":int (data['mode'][i]),
                                                #"ampl": int(data['ampl'][i]),
                                                #"value":int(data['spectre'][i][k]),
                                                "canal": int(k)
                                                #"detect": str(data['detect'][i]),
                                            }
                                            # print(tags)
                                            T = {}
                                            for m in np.arange(0, 10):

                                                if data['param']['n'][i][m]:
                                                    T[params[m]] = float(
                                                        data['param']['nb'][i][m])

                                                    if params[m] == "frq-cent":
                                                        temp = float(
                                                            data['param']['nb'][i][m])

                                                        # if frqcent.length ==
                                                        # 0:
                                                        if not frqcent:
                                                            frqcent[obs] = temp

                                                        if temp != frqcent[obs]:
                                                            # print("tag",tags[params[m]],frqcent[obs])
                                                            if temp not in frqcent.values():

                                                                obs = obs + 1
                                                                frqcent[
                                                                    obs] = temp

                                                            else:
                                                                obs = dict((v, k) for k, v in frqcent.items())[
                                                                    temp]

                                                        tags["conf"] = obs
                                            tags["fmin"] = T[
                                                "frq-cent"] - T["span"] / 2
                                            tags["fmax"] = T[
                                                "frq-cent"] + T["span"] / 2

                                            value["tags"] = tags

                                            stream.append(value)

                                        doc = {}
                                        doc['nbdiv'] = int(data['nbdiv'][i])
                                        doc['dbdiv'] = int(data['dbdiv'][i])
                                        doc['yunit'] = int(data['yunit'][i])
                                        doc['mode'] = int(data['mode'][i])
                                        doc['ampl'] = int(data['ampl'][i])
                                        doc['ah'] = float(data['ah'][i])
                                        doc['az'] = float(data['az'][i])
                                        doc['dbdiv'] = int(data['dbdiv'][i])
                                        doc['fmax'] = float(tags["fmin"])
                                        doc['fmin'] = float(tags["fmax"])
                                        doc['type'] = typ
                                        doc['conf'] = obs
                                        for m in np.arange(0, 10):
                                            if data['param']['n'][i][m]:
                                                doc[params[m]] = float(
                                                    data['param']['nb'][i][m])
                                        doc['@timestamp'] = dd
                                        #'timestamp': dd
                                        body = {"query":
                                                {"bool":
                                                    {"must":
                                                        [{"term":
                                                            {"@timestamp": dd}
                                                          }]
                                                     }
                                                 }
                                                }

                                        try:
                                            # res = es.search(index="nrh", doc_type='entFI', body={"query": { "term":{
                                            #'@timestamp':datedeb}}},fields='_id')
                                            res = es.search(index="nsa",
                                                            doc_type='an',
                                                            body=body, fields='_id')

                                            if res['hits']['total'] > 0:
                                                # print("update")
                                                _id = res['hits'][
                                                    'hits'][0]['_id']
                                                res = es.index(
                                                    index="nsa", doc_type='an', id=_id, body=doc)

                                            else:

                                                # print("new")
                                                #res = es.index(index="nrh", doc_type='entFI', body=entete, timeout=30 )
                                                res = es.index(
                                                    index="nsa", doc_type='an',  body=doc)

                                        except:
                                            print("Unexpected error:",
                                                  sys.exc_info()[0])
                                        try:

                                            s_in = binascii.a2b_qp(
                                                json.dumps(stream, encoding='utf8'))

                                        except:
                                            print("Unexpected error:",
                                                  sys.exc_info()[0])

                                        s_out = gzip.compress(s_in)

                                        rep = session.post(url, data=s_out)
                                        # print(rep)

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
                                                            "metric"] = "routine.new"
                                                        value["timestamp"] = d
                                                        value["value"] = TF
                                                        tags = {
                                                            "polarite": i, "status": 1, "frequence": int(j)}
                                                        value["tags"] = tags

                                                        stream.append(value)

                                    s_in = binascii.a2b_qp(
                                        json.dumps(stream, encoding='utf8'))

                                    # print("coucou");
                                    s_out = gzip.compress(s_in)

                                    url = 'http://nosql:4242/api/put'
                                    #r = session.post(url, data=json.dumps(stream))
                                    rep = session.post(url, data=s_out)

                            self.DB.close()

                        except ValueError:
                            print("Oops!  That was no valid number.  Try again...")

                        print("end append", datetime.datetime.now())
                    else:
                        print("not empty")
                print("Not ok", fname)

            counterDay += 1

            date = date + oneday

        response = {"message": "importation términée"}

        return response

    def setImport(self, f, sel):
        """

        :param f:
        :param sel:
        :return:
        """

        # logging.basicConfig(filename='/data/var_nsa/nrh.import4.log',level=logging.DEBUG,\
        #    format='%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s')

        filtre = json.loads(f)

        recepteur = filtre["recepteur"]

        #es = Elasticsearch(['master-nsasdb'])
        #es = Elasticsearch(['192.168.140.2'])
        es = Elasticsearch(['master-nsadb'])
        """
        doc = {
            "settings" : {
                "index" : {
                    "number_of_shards" : 3,
                    "number_of_replicas" : 2
                }
            }
        es.indices.create(index='nsa', body=doc, ignore=400)
        """
        #idl = pidly.IDL('/usr/local/bin/idl',long_delay=0.05)
        url = 'http://opentsdb-nsadb:4242/api/put'

        # pd.set_option('io.hdf.default_format','table')

        if int(recepteur):
            metric = "new"

            old = 0
        else:

            print(int(recepteur))
            metric = "old"
            old = 1

        print("import")

        #session = FuturesSession(max_workers=2)
        session = requests.Session()
        headers = {
            "Content-Type": "application/json", "Content-Encoding": "gzip"
        }

        session.headers.update(headers)

        dirUrl = "/data/data_nsa/" + metric
        print(dirUrl)

        Dirs = os.listdir(dirUrl)
        for dirname in Dirs:

            if os.path.isdir(dirUrl + "/" + dirname):
                n = dirname.split('.')
                print(n[0])
                if n[0] != "":

                    for filename in os.listdir(dirUrl + "/" + dirname):
                        print("filename", filename)
                        m = filename.split('.')

                        ext = m[1]
                        print("ext", ext)
                        if ext == "DAT":
                            fname = dirUrl + "/" + dirname + "/" + filename
                            print("fname", fname)
                            ajout = True
                        else:
                            ajout = False

                        print("ajout à la base ? : ", ajout)

                        if ajout:

                            if os.path.isfile(fname):
                                print("ok", fname)

                                print("Create ")
                                # importation from file fname:
                                if old:
                                    """
                                    OLD NSA
                                    """

                                    #dt = '405B'
                                    dt = np.dtype([('dat', 'uint16', (3)),
                                                   ('h', 'uint16', (3)),
                                                   ('nb1', 'B'),
                                                   ('ah', 'S', (10)),
                                                   ('nb2', 'B'),
                                                   ('az', 'S10', (1)),
                                                   ('integ', 'B'),
                                                   ('nb3', 'B'),
                                                   ('detect', 'S', (8)),
                                                   ('spectre', '>i2', (701)),
                                                   ('nul1', 'int8', (61)),
                                                   ('nbdiv', 'c'),
                                                   ('dbdiv', 'c'),
                                                   ('yunit', 'c'),
                                                   ('mode', 'c'),
                                                   ('ampl', 'c'),
                                                   ('nul2', 'int8', (3)),
                                                   ('comment', 'S', (155)),

                                                   #('param',('S',(18)),10)
                                                   ('param', [
                                                    ('n', 'uint8', (1)), ('nb', 'S', (17))], 10)
                                                   ])

                                    data = np.fromfile(fname, dtype=dt)

                                    #np.dtype([('f1', [('f1', np.int16)])])
                                    #np.dtype([('f1', np.uint), ('f2', np.int32)])

                                else:

                                    """
                                    NEW DAM
                                    """

                                print("Insert ")
                                #es = Elasticsearch(['193.55.144.48'])
                                #es.indices.create(index='nsa', ignore=400, timeout=30)
                                try:

                                    if old:

                                        """
                                        OLD NSA
                                        """

                                        params = ["niv-ref", "frq-cent", "span", "band-res",
                                                  "vbw", "att", "swp", "niv-marq", "niv-marq", "frq-marq"]
                                        #params = ["niv-ref","frq-cent","span","band-res","vbw","att","swp"]
                                        for i in np.arange(data.shape[0]):
                                            # print(i,data['dat'][i][0])

                                            # print(i,data['h'][i][0])
                                            # print(i,data['nb1'][i])
                                            # print(i,float(data['ah'][i]))
                                            # print(i,float(data['az'][i]))
                                            # print(i,data['integ'][i])
                                            # print(i,str(data['detect'][i]))
                                            # print(i,data['spectre'][i])
                                            # print(i,int(data['nbdiv'][i]))
                                            # print(i,data['dbdiv'][i])
                                            # print(i,data['yunit'][i])
                                            # print(i,data['mode'][i])
                                            # print(i,data['ampl'][i])
                                            # print(i,data['comment'][i])
                                            # print(i,data['param'][i])

                                            dd = datetime.datetime(data['dat'][i][0], data['dat'][i][1], data['dat'][i][
                                                                   2], data['h'][i][0], data['h'][i][1], data['h'][i][2])
                                            # datetime.datetime(idl.dat[2],idl.dat[1],
                                            # idl.dat[0],idl.hdeb[0],idl.hdeb[1],
                                            # idl.hdeb[2], idl.hdeb[3]*1000,
                                            # pytz.UTC)}

                                            # print(dt)

                                            if not i % 1000:
                                                print("ligne", i)

                                            #

                                            for l in np.arange(0, 1):
                                                stream = []
                                                for k in np.arange(l * 701, (l + 1) * 701):
                                                    value = {}

                                                    value["metric"] = "nsa"
                                                    """
                                                    value["metric"] = "nsa.old.nbdiv"+\
                                                    str(int(data['nbdiv'][i]))+\
                                                    ".dbdiv"+str(int(data['dbdiv'][i]))+\
                                                    ".yunit"+str(int(data['yunit'][i]))+\
                                                    ".mode"+str(int(data['mode'][i]))+\
                                                    ".ampl"+str(int(data['ampl'][i]))
                                                    #print(value)
                                                    """

                                                    value["timestamp"] = round(
                                                        dd.timestamp())
                                                    value["value"] = int(
                                                        data['spectre'][i][k])
                                                    tags = {
                                                        "ah": float(data['ah'][i]),
                                                        "az": float(data['az'][i]),
                                                        "nbdiv": int(data['nbdiv'][i]),
                                                        "dbdiv": int(data['dbdiv'][i]),
                                                        "yunit": int(data['yunit'][i]),
                                                        "mode": int(data['mode'][i]),
                                                        "ampl": int(data['ampl'][i]),

                                                        "frequence": int(k)
                                                        #"detect": str(data['detect'][i]),
                                                    }
                                                    """
                                                    for m in np.arange(0,10):

                                                        if data['param']['n'][i][m]:

                                                            tags[params[m]] = float(data['param']['nb'][i][m])
                                                    """
                                                    value["tags"] = tags

                                                    stream.append(value)

                                                doc = {}
                                                doc['nbdiv'] = int(
                                                    data['nbdiv'][i])
                                                doc['dbdiv'] = int(
                                                    data['dbdiv'][i])
                                                doc['yunit'] = int(
                                                    data['yunit'][i])
                                                doc['mode'] = int(
                                                    data['mode'][i])
                                                doc['ampl'] = int(
                                                    data['ampl'][i])
                                                doc['ah'] = float(
                                                    data['ah'][i])
                                                doc['az'] = float(
                                                    data['az'][i])
                                                doc['dbdiv'] = int(
                                                    data['dbdiv'][i])
                                                for m in np.arange(0, 10):
                                                    if data['param']['n'][i][m]:
                                                        doc[params[m]] = float(
                                                            data['param']['nb'][i][m])
                                                doc['@timestamp'] = dd
                                                #'timestamp': dd

                                                body = {"query":
                                                        {"bool":
                                                         {"must":
                                                          [{"term":
                                                            {"@timestamp": dd}
                                                            }]
                                                          }
                                                         }
                                                        }

                                                try:
                                                    # res = es.search(index="nrh", doc_type='entFI', body={"query": { "term":{
                                                    #'@timestamp':datedeb}}},fields='_id')
                                                    res = es.search(index="nsa",
                                                                    doc_type='old',
                                                                    body=body, fields='_id')

                                                    if res['hits']['total'] > 0:
                                                        print("update")
                                                        _id = res['hits'][
                                                            'hits'][0]['_id']
                                                        res = es.index(
                                                            index="nsa", doc_type='old', id=_id, body=doc, timeout=30)

                                                    else:

                                                        print("new")
                                                        #res = es.index(index="nrh", doc_type='entFI', body=entete, timeout=30 )
                                                        res = es.index(
                                                            index="nsa", doc_type='old',  body=doc)

                                                except:
                                                    print(
                                                        "Unexpected error:", sys.exc_info()[0])

                                                s_in = binascii.a2b_qp(
                                                    json.dumps(stream, encoding='utf8'))

                                                s_out = gzip.compress(s_in)

                                                rep = session.post(
                                                    url, data=s_out)

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
                                                                    "metric"] = "routine.new"
                                                                value[
                                                                    "timestamp"] = d
                                                                value[
                                                                    "value"] = TF
                                                                tags = {
                                                                    "polarite": i, "status": 1, "frequence": int(j)}
                                                                value[
                                                                    "tags"] = tags

                                                                stream.append(
                                                                    value)

                                            s_in = binascii.a2b_qp(
                                                json.dumps(stream, encoding='utf8'))

                                            # print("coucou");
                                            s_out = gzip.compress(s_in)

                                            url = 'http://nosql:4242/api/put'
                                            #r = session.post(url, data=json.dumps(stream))
                                            rep = session.post(url, data=s_out)

                                    self.DB.close()

                                except ValueError:
                                    print(
                                        "Oops!  That was no valid number.  Try again...")

                                print("end append", datetime.datetime.now())

                            print("Not ok", fname)

                        print("fin", fname)
        response = {"message": "importation términée"}

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

                    print("end append", datetime.datetime.now())
                else:
                    print("not empty")
            file.close()
            counterDay += 1

            date = date + oneday

        response = {"message": "importation términée"}

        return response
