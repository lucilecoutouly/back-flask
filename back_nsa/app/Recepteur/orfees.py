# -*- coding: = utf-8 -*-
# __author__ = 'lucile'

import ctypes
from struct import *
import logging
import array
import binascii
import datetime
import pytz
import base64
import uuid
import numpy as np
import time
import json
import sys, traceback
import pidly
#import astropy
from astropy.io import fits
import re
from matplotlib import *
import os.path
import uuid
import shutil
#import h5py
import strict_rfc3339
#import cv2
#from PIL import Image,ImageOps
#from astropy.table import Table
import matplotlib.pyplot as plt
#import SimpleCV
from elasticsearch import Elasticsearch

class orfees(object):



    def getRequest(self,filtre,type,frequence):
        """
        :param filtre:
        :param type:
        :param frequence:
        :return:
        """
        if type == "canvas":
            return self.getCanvas(filtre,frequence)
        elif type == "setImport":
            return self.setImport(filtre,frequence)
        elif type == "view":
            return self.getView(filtre,frequence)
        elif type == "image":
            return self.getImage(filtre,frequence)
        elif type == "getFits":
            return self.getFits(filtre,frequence)
        elif type == "1d":
            return self.get1d(filtre,frequence)
        elif type == "getVideo":
            return self.getVideo(filtre,frequence)
        

    def getImage(self,f,sel):
        print("getImage")
        """
        ; ---------------------------------------------------------------- 
        ;                            RH_IM_2DSEL
        ; ---------------------------------------------------------------- 
        ; Calcule l'image a la frequence nof non polar ou polar
        ; integree kint fois a partir de klu 
        ; Le resultat est dans le tableau ima
        ;
        ; INPUTS:
        ;	klu  : numero de l'acquisition a lire	LONG
        ;	       debut du fichier = 0L
        ;	nof  : numero de frequence
        ;	npol : 0-Non Polar, 1-Polar
        ;	kint : facteur d'integration
        ;	sz   : taille de l'image
        ;	larg : largeur de l'image
        ;	h    : Heure de l'image integree (milieu)
        ;	ima  : image integree
        ;
        ; OUTPUTS:
        ;	h    : Heure de l'image integree (milieu)
        ;	ima  : image integree
        ;
        ; COMMON BLOCKS: 
        ;       RH
        ; MODIFICATIONS:
        ; 03 sep 08  : Appelle RH_MALCROND_IM_2D : qui calcule l'image en
        ; utilisant au mieux tous les harmoniques y compris anti_alias
        ; 05/07/2011: gestion de la panne du correlateur depuis le
        ;              27/11/2009 (antennes AA3 et AA4 avec NS12 a NS16)
        ;             on corrige la panne pour l'option image2d de menu_rh
        ; 25/06/2013 : gestion de la panne du correlateur ns08 avec ew01(h1)
        ; -----------------------------------------------------------------  

        PRO RH_IM_2DSEL, klu, nof, npol, kint, sz, larg, h, ima
        """
        filtre = json.loads(f)
        
        # date = datetime.datetime.strptime(filtre["date"], "%Y-%m-%dT%H:%M:%S.%f")
        
        # "2005-12-19T09:18:25.060+0100"
        datedeb = datetime.datetime.strptime(filtre["datedeb"], "%Y-%m-%dT%H:%M:%S.%f%z")
        print(datedeb)
        #duree = filtre["dure"]
        frequence = filtre["frequence"]
        print(frequence)
        integration = (filtre["integration"])
        print(integration)
        #fRH = ["2d" + datedeb.strftime("%y%m%d") +".01", "2d" + datedeb.strftime("%y%m%d") +".01Z", "2d" + datedeb.strftime("%y%m%d") +".01ZZ"]
        
        sel = filtre["sel"]
        print(sel)
        
        """
        fichier à utiliser en fonction de la date
        """
        
        find = False
        for res in sel:
            nrhfile = res["file"][0]
            nrhfrq = res["frq"]
            hdeb = (strict_rfc3339.rfc3339_to_timestamp(res["hdeb"][0]))
            hfin = (strict_rfc3339.rfc3339_to_timestamp(res["hfin"][0]))
            
            unixdatedeb = time.mktime(datedeb.timetuple())
            print(hdeb,unixdatedeb,hfin)
            if (unixdatedeb>hdeb and unixdatedeb<hfin):
                find = True
                break
        if  find:
            
            fname = "/data/data_nrh/rh/"+datedeb.strftime("%Y")+"/"+datedeb.strftime("%m")+"/"+ datedeb.strftime("%d")+"/"+nrhfile

            print(fname)
            uid = str(uuid.uuid4())

            idl = pidly.IDL('/usr/local/bin/idl')
            #$IDL = "/home/user/idl83/bin/idl  ".self::$image_rh." -quiet -args  ".$file." ".json_encode($time,JSON_NUMERIC_CHECK)." ".$frequence." ".$polarite." ".$integration." ".self::$uri." ";

            idl.fichier = fname
            print("datedeb",datedeb.utctimetuple()[3:6],int(datedeb.microsecond/10000))
            heurededebut = list(datedeb.utctimetuple()[3:6])
            heurededebut.append(int(datedeb.microsecond/10000))
            idl.h = heurededebut

            idl.kint=int(integration)
            idl.frequence = frequence
            idl('print,frequence')

            idl.npol=0

            idl.url='/media/data/Public/Images/'+uid+'.png'
            
            idl('@rh_common.inc')
            idl('status=RH_OPEN(fichier,/SEL,/MALAX)')
            idl('print, entFI.frq')
            idl('nof =  WHERE(entFI.frq EQ frequence)')
            idl('print,nof')
            idl('ima=fltarr(300,300)')
            idl('rh_hdeb,h, klu, heurlu')
            print(idl.h)
            print(idl.heurlu)
            print(idl.klu)
            print(idl.nof)
            print(idl.kint)
            #idl.pro('IMAGE_RH',fichier,h,frequence,polarite,integration,url)
            #idl('rh_hfin, h, klu, heurlu ')
            print(idl.h)
            print(idl.heurlu)
            print(idl.klu)


            idl.sz=200
            idl.LARG=4
            idl("set_plot,'PS'")
            idl('RH_IM_2DSEL, klu, nof, npol, kint, sz, larg, h, ima')
            idl('loadct,3, /SILENT')
            idl('TVLCT, R, G, B, /GET')
            idl("write_image, url,'PNG',bytscl(ima),R, G, B")
            #IDL procedure with Python argument(s)::
            #idl.pro('IMAGE_RH', range(10), range(10), xstyle=True, ystyle=True)
            print(idl.status)
            

        response = {
            "status":"success",
            "message":"Image loaded succesfully.",
            "type":"img",
            "date": idl.h.tolist(),
            "url":"/Public/Images/"+uid+".png"
        }
        #idl.close()
        idl('exit')
        return response
        
        
        

    def getVideo(self,f,sel):
        print("getVideo")
        """
        une video par jour
        """
        filtre = json.loads(f)
        
        
        # "2005-12-19T09:18:25.060+0100"
        datedeb = datetime.datetime.strptime(filtre["datedeb"], "%Y-%m-%dT%H:%M:%S.%f%z")
        print(datedeb)
        #duree = filtre["dure"]
        frequence = filtre["frequence"]
        print(frequence)
        integration = filtre["integration"]
        print(integration)
        
        nrhfile = "nrh" + datedeb.strftime("%d%m%Y")+"_"+str(frequence) + ".mpg" 
        # nrh01092006_3270.mpg
        print(nrhfile)
        
        
        
        """
        fichier à utiliser en fonction de la date
        """
        
        find = True
        """
        for res in sel:
            nrhfile = res["file"][0]
            nrhfrq = res["frq"]
            hdeb = (strict_rfc3339.rfc3339_to_timestamp(res["hdeb"][0]))
            hfin = (strict_rfc3339.rfc3339_to_timestamp(res["hfin"][0]))
            
            unixdatedeb = time.mktime(datedeb.timetuple())
            
            if (unixdatedeb>hdeb and unixdatedeb<hfin):
                find = True
                break
        """
        fname = "/data/data_nrh/rh/"+datedeb.strftime("%Y")+"/"+datedeb.strftime("%m")+"/"+ datedeb.strftime("%d")+"/"+nrhfile
        print(fname)
        if  os.path.isfile(fname):
            
            
           
            print(fname)
            #uid = str(uuid.uuid4())
            """
            idl = pidly.IDL('/usr/local/bin/idl')
            #$IDL = "/home/user/idl83/bin/idl  ".self::$image_rh." -quiet -args  ".$file." ".json_encode($time,JSON_NUMERIC_CHECK)." ".$frequence." ".$polarite." ".$integration." ".self::$uri." ";

            idl.fichier = fname
            print("datedeb",datedeb.utctimetuple()[3:6],int(datedeb.microsecond/10000))
            heurededebut = list(datedeb.utctimetuple()[3:6])
            heurededebut.append(int(datedeb.microsecond/10000))
            idl.h = heurededebut

            idl.kint=int(integration)
            idl.frequence = frequence
            idl('print,frequence')

            idl.npol=0

            idl.url='/media/data/Public/Images/'+uid+'.png'
            
            idl('@rh_common.inc')
            idl('status=RH_OPEN(fichier,/SEL,/MALAX)')
            idl('print, entFI.frq')
            idl('nof =  WHERE(entFI.frq EQ frequence)')
            idl('print,nof')
            idl('ima=fltarr(300,300)')
            idl('rh_hdeb,h, klu, heurlu')
            print(idl.h)
            print(idl.heurlu)
            print(idl.klu)
            print(idl.nof)
            print(idl.kint)
            #idl.pro('IMAGE_RH',fichier,h,frequence,polarite,integration,url)
            #idl('rh_hfin, h, klu, heurlu ')
            print(idl.h)
            print(idl.heurlu)
            print(idl.klu)


            idl.sz=200
            idl.LARG=4
            idl("set_plot,'PS'")
            idl('RH_IM_2DSEL, klu, nof, npol, kint, sz, larg, h, ima')
            idl('loadct,3, /SILENT')
            idl('TVLCT, R, G, B, /GET')
            idl("write_image, url,'PNG',bytscl(ima),R, G, B")
            #IDL procedure with Python argument(s)::
            #idl.pro('IMAGE_RH', range(10), range(10), xstyle=True, ystyle=True)
            print(idl.status)
            
            """
            url='/media/data/Public/Videos/'+nrhfile
            print(url)
            shutil.copyfile(fname, url) 
            print(fname)
            response = {
                "status":"success",
                "message":"Image loaded succesfully.",
                "type":"mpg",
                "date": datedeb,
                "url":"/Public/Videos/"+nrhfile
            }
        else:
            response = {
                "status":"error",
                "message":"Video not found .",
                "type":"mpg",
                "date": datedeb,
                "url":""
            }
            
            
        
        
        return response
        
    def getFits(self,f,sel):
        print("getFits")
        """
        PRO  DPATCHFITSHARM, nomfich, ipolar, itdq, itfq, $
                     dpatch_dir, selec=ch
        ;+ ***********************************************************************
        ; NAME:
        ;	DPATCHFITSHARM
        ; PURPOSE:
        ;	-cree des fichiers FITS 2D d'harmoniques
        ;	sur le repertoire dpatch_dir
        ;       -selectionne les frequences a depatcher
        ; CATEGORY:
        ;	Traitement de fichiers NRH
        ; CALLING SEQUENCE:
        ;	DPATCHFITSHARM, Nomfich, Ipolar, Itdq, Itfq, $
        ;		    Dpatch_dir, SELEC = ch
        ; INPUTS:
        ;	Nomfich : nom du fichier a traiter
        ;	Ipolar  : =1 fichiers Helio avec polar 
        ;		  =0                sans polar
        ;	Itdq    : heure de debut  (h,m,s)
        ;	Itfq    : heure de fin    (h,m,s)
        ;	Dpatch_dir : repertoire d'ecriture des fichiers FITS 2D
        ; KEYWORD PARAMETERS:
        ;	SELEC	: pour la selection des frequences 
        ;		  = ch(10) 'Y' ou 'N'
        ;		  par defaut toutes les frequences sont gardees
        ; COMMON BLOCKS:
        ;	RH	: communique avec les routines de lecture des donnees
        ;		  brutes RH
        ; EXAMPLE:
        ;   ch=strarr(10)
        ;   ch(*)=['Y','Y','Y','Y','Y','N','N','N','N','N']
        ;   DPATCHFITSHARM,'/cdrom/2c970923.01', 0, [10,45,00,00], [11,20,00,00]
        ; ,                '/disque2/scratch/bouteill', selec=ch	
        ; MODIFICATION HISTORY:
        ;	Ecrit par A. Bouteille 
        ;	Modifie le 22-jul-1998 
        ;	- appel des nouvelles routines rh_read (pour ajout antenne NS24)
        ;	- attention a l'ordre des gains modifie dans rh_readch
        ;	- Fevrier 1999 : correction de l'ordre des gains 
        ;	a faire sur les fichiers fits d'harmoniques 2D 
        ;	jusqu'au  18/2/1999  inclus
        ;       lufi.g : EW NS 2D  est different de  bfi.g : 2D EW NS
        ;   dpatchfitsharm ecrivait les gains NS 2D EW au lieu de 2D EW NS
        ;
        ; 2011 jan 27 : BUG : au passage aux 10 frequences l'extension CORREL est tronquee: 
        ;  le header de l'extension 2 se terminait ainsi (au lieu des 109
        ;  champs attendus): 
        ;TFORM98 = '2B      '           / ARRAY, type byte
        ;TTYPE98 = 'CAL_SRC10'          / Calibration source
        ;TFORM99 = '2I      '           / ARRAY, type integer*2
        ;TTYPE99 = 'CAL_TIM10'          / calibration time(h,m)
        ;TFORM** = '57J     '           /
        ;TTYPE** = 'LIBRE   '           /
        ;TDIM**  = '(80,15) '           /
        ;END
        ;  donc pour les fichiers fits harmoniques a 10 freq precedant le 27 janvier
        ;  2011 et envoyes a Tarbes, l'extension 2 ne donne pas directement entfi, 
        ; mrdfits se plante ainsi :
        ;IDL> entfi=mrdfits('nrh2_1509_a00_20100711_082416c03_c.fts',2,h2)
        ;% Attempt to subscript FNAMES with I is out of range.
        ;% Error occurred at: MRD_TABLE 2326 /usr/local/ssw/gen/idl/fits/mrdfits.pro
        ;%             MRDFITS       2721 /usr/local/ssw/gen/idl/fits/mrdfits.pro
        ; prevoir un programme pour remplir et completer entfi dans ce cas
        ;-**************************************************************************
        @rh_common.inc
        """
        filtre = json.loads(f)
        """
        {
            "datedeb":"2007-12-21T09:52:41.010+0100",
            "frequence":1640,
            "integration":10,
            "dure":72000,
            "recepteur":0,
            "sel":
                [
                {
                    "hfin":["2007-12-21T14:35:44.089000+00:00"],
                    "frq":[2280,3270,4080,1509,4320,0,0,0,0,0],
                    "hdeb":["2007-12-21T11:44:13.005000+00:00"],
                    "file":["2d071221.01Z"]
                },{
                    "hdeb":["2007-12-21T14:35:45+00:00"],
                    "file":["2d071221.01ZZ"],
                    "hfin":["2007-12-21T15:18:59.036000+00:00"],
                    "frq":[2280,3270,4080,1509,4320,0,0,0,0,0]
                },{
                    "hdeb":["2007-12-21T08:52:41.010000+00:00"],
                    "file":["2d071221.01"],
                    "hfin":["2007-12-21T11:44:12.094000+00:00"],
                    "frq":[2280,3270,4080,1509,4320,0,0,0,0,0]
                }
                ]
        } 
        javascript rfc3339 "2015-01-20T12:52:29.005000+00:00",
        python "2007-12-21T09:52:41.010+0100"
        """
        datedeb = datetime.datetime.strptime(filtre["datedeb"], "%Y-%m-%dT%H:%M:%S.%f%z")
        
        #duree = filtre["dure"]
        frequence = filtre["frequence"]
        sel = filtre["sel"]
        
        """
        fichier à utiliser en fonction de la date
        """
        
        find = False
        for res in sel:
            nrhfile = res["file"][0]
            nrhfrq = res["frq"]
            hdeb = (strict_rfc3339.rfc3339_to_timestamp(res["hdeb"][0]))
            hfin = (strict_rfc3339.rfc3339_to_timestamp(res["hfin"][0]))
            unixdatedeb = time.mktime(datedeb.timetuple())
            
            if (unixdatedeb>hdeb and unixdatedeb<hfin):
                find = True
                break
       
            
        
        integration = (filtre["integration"])
        #fRH = ["2d" + datedeb.strftime("%y%m%d") +".01", "2d" + datedeb.strftime("%y%m%d") +".01Z", "2d" + datedeb.strftime("%y%m%d") +".01ZZ"]

        if  (find) :
            
            fname = "/data/data_nrh/rh/"+datedeb.strftime("%Y")+"/"+datedeb.strftime("%m")+"/"+ datedeb.strftime("%d")+"/"+nrhfile

            print(fname)
            uid = str(uuid.uuid4())

            idl = pidly.IDL('/usr/local/bin/idl')
            #$IDL = "/home/user/idl83/bin/idl  ".self::$image_rh." -quiet -args  ".$file." ".json_encode($time,JSON_NUMERIC_CHECK)." ".$frequence." ".$polarite." ".$integration." ".self::$uri." ";
            idl('@rh_common.inc')
            idl.fichier = fname

            idl.h = list(datedeb.timetuple()[3:7])

            idl.kint=int(integration)
            idl.frequence=int(frequence)
            #idl.nof=int(sel)

            #idl.npol=0

            #idl.url='/media/data/Public/Images/'+uid+'.png'
            dirUrl = '/media/data/Public/Fits/'+uid
            #/media/data/Public/Fits/
            idl.url= dirUrl
            os.mkdir(dirUrl)
            
            idl('status=RH_OPEN(fichier,/SEL,/MONO)')
            idl('nof =  WHERE(entFI.frq EQ frequence)')
            #idl('ima=fltarr(200,200)')
            #idl('rh_hdeb,h, klu, heurlu')
            #print(idl.h)
            #print(idl.heurlu)
            #print(idl.klu)
            #print(idl.nof)
            #print(idl.kint)
            #idl.pro('IMAGE_RH',fichier,h,frequence,polarite,integration,url)
            #idl('rh_hfin, h, klu, heurlu ')
            #print(idl.h)
            #print(idl.heurlu)
            #print(idl.klu)
            idl('ch=strarr(10)')
            idl("ch=['N','N','N','N','N','N','N','N','N','N']")
            idl("ch[nof]='Y'")
            idl("ch=['Y','N','N','N','N','N','N','N','N','N']")
            print(idl.ch)
            #idl.sz=200
            #idl.LARG=4
            #idl("set_plot,'PS'")
            #idl('hdeb = JSON_PARSE(time,/TOARRAY)')
            #print(idl.hdeb)
            
            #hfin = JSON_PARSE(Result[1],/TOARRAY)
            #idl('RH_IM_2DSEL, klu, nof, npol, kint, sz, larg, h, ima')
            idl('DPATCHFITSHARM, fichier, 1,[00,00,00,00],[24,00,00,00],url,selec=ch')
            #idl('DPATCHFITSHARM, fichier, 1,[00,00,00,00],[24,00,00,00],url')
            #idl('loadct,3, /SILENT')
            #idl('TVLCT, R, G, B, /GET')
            #idl("write_image, url,'PNG',bytscl(ima),R, G, B")
            #IDL procedure with Python argument(s)::
            #idl.pro('IMAGE_RH', range(10), range(10), xstyle=True, ystyle=True)
            print("fin idl",idl.status)
            #os.cd(dirUrl)
            Files = []
            
            Dirs = os.listdir(dirUrl)
            for filename in Dirs:
                print(filename)
                Files.append("/Public/Fits/"+uid+"/"+filename) 
                try:
                    hdulist = fits.open(dirUrl+"/"+filename)
                    print("coucou")
                    print(hdulist.info())
                    for h in hdulist[0].header:
                        print(h)
                    print(hdulist[0].header["DATE_OBS"])
                    print(hdulist[0].header["DATE_END"])
                except:
                    print ("Unexpected error:", sys.exc_info()[0])
            
            print(Files)
               
            
        print("break")
        
        print("break")
        response = {
            "status":"success",
            "message":"Fits done succesfully.",
            "type":"fits",
            
            "date":datedeb
            ,"url":Files[0]
        }
        #idl.close()
        idl('exit')
        print(response)
        return response

    def get1d(self,f,sel):
        print("get1d")
        """
        ; =======================================================================
        ;                               IMAGE_1D
        ; -----------------------------------------------------------------------
        ; INPUTS:
        ; 	path_data:	repertoire contenant les fichiers de donnees
        ; 	path_poub:	repertoire poubelle permettant d'ecrire des
        ;                  	des fichiers temporaires
        ; 	cmd_ps:		commande d'impression PS
        ; 	liste_scratch:  nom du fichier contenant la liste des fichiers
        ;                    	temporaires a detruire a la sortie de 'menu_rh'
        ;
        ; COMMON WIMAGE_1D: 
        ;  	nomfich:	nom du fichier selectionne
        ;	nom_don:	nom 'non polar' ou 'polar'
        ;	t_don:		t_don(i) = 0-non sel, 1-sel 
        ;	tab_don:	liste de selection
        ;	cdon:		nombre de polar selectionnees
        ;	nom_freq:	nom des frequences
        ;	t_freq:		t_freq(i) = 0-non sel, 1-sel
        ;	tab_freq:	numeros des frequences selectionnees
        ;	cfreq:		nombre de frequences selectionnees
        ;	sel_graf:	0-Image, 1-Niveaux, 2-Surface
        ;	s_heure:	ID heure
        ;	heure:		heure courante
        ;	integ:		facteur d'integration
        ;	ng:		nombre courant de graphiques
        ;	sz_ima:		taille d'une image
        ;	ima:		1 image
        ;	lec:		=1 si lecture a faire avant de tracer
        ;	klu:		indice  courant de lecture
        ;	psmode:		0-Paysage, 1-portrait
        ;	cmdps:		commande d'impression PS
        ;	fpoub:		directory poubelle
        ;	fscratch:	fichier contenant la liste des fichiers temporaires
        ;
        ; =======================================================================

        PRO image_1d, path_data, path_poub, cmd_ps, liste_scratch, GROUP=Gp
        
        """
        """
        ; ----------------------------------------------------------------
        ;                             im_1dsel
        ; ----------------------------------------------------------------
        ; Calcule pour la frequence nof l'image EW non polar ou EW polar ou
        ; NS non polar ou NS polar
        ; Image integree kint fois a partir de klu
        ; Le resultat est dans le tableau ima
        ;
        ; INPUTS:
        ;	klu  : numero de l'acquisition a lire	LONG
        ;	       debut du fichier = 0L
        ;	nof  : numero de frequence
        ;	npol : 0-EW Non Polar
        ;              1-EW Polar
        ;              2-NS Non Polar
        ;              3-NS Polar
        ;	kint : facteur d'integration
        ;	sz   : taille de l'image
        ;	h    : Heure de l'image integree (milieu)
        ;	ima  : image integree
        ;
        ; OUTPUTS:
        ;	h    : Heure de l'image integree (milieu)
        ;	ima  : image integree
        ;
        ; COMMON BLOCKS:
        ;       RH
        ;       20-09-2004 :  correction de position des antennes
        ; -----------------------------------------------------------------

        PRO im_1dsel, klu, nof, npol, kint, sz, h, ima
        """

        filtre = json.loads(f)
          
        datedeb = datetime.datetime.strptime(filtre["datedeb"], "%Y-%m-%dT%H:%M:%S.%f%z")
        print(datedeb)
        #duree = filtre["dure"]
        frequence = filtre["frequence"]
        print(frequence)
        integration = (filtre["integration"])
        print(integration)
        fRH = ["2d" + datedeb.strftime("%y%m%d") +".01", "2d" + datedeb.strftime("%y%m%d") +".01Z", "2d" + datedeb.strftime("%y%m%d") +".01ZZ"]
        print(fRH)
        for  file in fRH:
            fname = "/data/data_nrh/rh/"+datedeb.strftime("%Y")+"/"+datedeb.strftime("%m")+"/"+ datedeb.strftime("%d")+"/"+file

            print(fname)
            uid = str(uuid.uuid4())

            idl = pidly.IDL('/usr/local/bin/idl')
            #$IDL = "/home/user/idl83/bin/idl  ".self::$image_rh." -quiet -args  ".$file." ".json_encode($time,JSON_NUMERIC_CHECK)." ".$frequence." ".$polarite." ".$integration." ".self::$uri." ";

            idl.fichier = fname
            idl.frequence = frequence
            idl.h = list(datedeb.timetuple()[3:7])

            idl.kint=int(integration)
            idl('@rh_common.inc')
            idl('status=RH_OPEN(fichier,/SEL,/MONO)')
            idl('print, entFI.frq')
            idl('nof =  WHERE(entFI.frq EQ frequence)')
            

            

            idl.npol=0

            idl.url='/media/data/Public/Images/'+uid+'.png'

            
            #idl('ima = BINDGEN(1,256,256)')
            idl.sz=256
            idl('nbmax = min([entFI.klumax+1,1000])')
            idl('nbmax=entFI.klumax+1')
            idl('ima=fltarr(sz,nbmax)')
            idl('rh_hdeb,h, klu, heurlu')
            print(idl.h)
            print(idl.heurlu)
            print(idl.klu)
            #idl.pro('IMAGE_RH',fichier,h,frequence,polarite,integration,url)
            idl('rh_hfin, h, klu, heurlu ')
            print(idl.h)
            print(idl.heurlu)
            print(idl.klu)


            
            idl.LARG=4
            idl("set_plot,'PS'")
            #  PRO im_1dsel, klu, nof, npol, kint, sz, h, ima
            idl('IM_1DSEL, klu, nof, npol, kint, sz,  h, ima')
            idl('print,ima')
            idl('loadct,3, /SILENT')
            idl('TVLCT, R, G, B, /GET')
            #idl('p = PLOT(ima)')
            #idl('p.Save, url, BORDER=10,  RESOLUTION=300, /TRANSPARENT')
            #idl("write_image, url,'PNG',p,R, G, B")
            #IDL procedure with Python argument(s)::
            #idl.pro('IMAGE_RH', range(10), range(10), xstyle=True, ystyle=True)
            print(idl.status)
            break

        response = {
            "status":"success",
            "message":"Image loaded succesfully.",
            "time":time,
            "date":date,
            "url":"/Public/Images/"+uid+".png",
            "json":idl.ima.tolist()
        }
        #idl.close()
        idl('exit')
        return response


    def getCanvasFromFile(self,f,sel):

        filtre = json.loads(f)
        # 2014-01-01T00:00:00.000Z


        """
        function (doc, meta) {
          var d =  dateToArray(doc.datetime);
          for(var k in doc.frequence){
            emit([d[3],d[4],doc.polarite,parseInt(k)],doc.frequence[k] );
          }
         }
        function(keys, values, rereduce) {

          var length = values.length;
          var reduceValue=0;
          for(var k in values){

           reduceValue = reduceValue + values[k];

          }

          return reduceValue/length;


        }
        """


        # 2014-01-01T00:00:00.000Z

        date = datetime.datetime.strptime(filtre["date"], "%Y-%m-%dT%H:%M:%S.%fZ")
        time = datetime.datetime.strptime(filtre["time"], "%Y-%m-%dT%H:%M:%S.%fZ")
        duree = filtre["dure"]
        frequence = filtre["frequence"]

        """
        cb = Couchbase.connect(bucket='NdaOld')
        r = []

        ViewRow objects are simple named tuples with the following fields:

            vr.key

            The key emitted by the views map function (i.e. first argument to emit.

            vr.value

            The value emitted by the views map function (i.e. second argument to emit ).

            vr.id

            The document ID of this row. The ID can be passed to get and set.

            vr.doc

        cb.query("dev_canvas",
             "default",
             limit=3,
             mapkey_range = ["abbaye", "abbaye" + Query.STRING_RANGE_END],
             descending=True)
        cb.query("dev_canvas", "default", query="limit=3&skip=1&stale=false")


        view = cb.query("dev_canvas",
             "default",
             limit=600
             # mapkey_range = ["abbaye", "abbaye" + Query.STRING_RANGE_END],
             # descending=True
             )


        view = views.iterator.View(cb, "dev_canvas", "default",
            limit=600,
            reduce="false")

         url":[{'values':[{'x':0,'y':35},{"x":1,"y":36...

        for result in view:
            r= r + result.value

        j = {"values":r}
        """

        #pathtable = date.strftime("/an/2/%d/dataset")

        #t = Table.read('/datadam/routine/hdf5/NdaOld.hdf5', path=pathtable)

        #d = pd.HDFStore('/datadam/routine/hdf5/NdaOld.hdf5')
        """
        with h5py.File("/datadam/routine/hdf5/NdaOld3.hdf5", "r") as f:
            print(f)
            dset = f["/an/2/1/dataset"]
            print(dset)
        """

        fdam = "S" + date.strftime("%y%m%d")+".RT1"
        rdam = date.strftime("%Y")
        fname = "/datadam/routine/"+rdam+"/"+fdam

        if os.path.isfile(fname):

            file = open(fname, "rb")
            bytes_read = file.read(405)
            dt = 'c,2S,2S,45S,2S,1S,2S,1S,2S,1S,6S,340S'
            c = np.fromstring(bytes_read, dtype=dt)



            jour = int(c[0][4])


            mois = int(c[0][6])

            an = int(c[0][8])

            if 59 <= an <= 99:
                an =  int(an + 1900)
            else:
                an =  int(an + 2000)

            path = "/"+str(an)+"/"+str(mois)+"/"+str(jour)

            #dt=np.int8
            dt = '405B'

            # dt = np.uint8

            data = np.fromfile(fname, dtype=dt)

            data = np.delete(data, 0, 0)
            data = np.delete(data, [0,1,2,3], axis=1)

            data = np.delete(data, [400], axis=1)

            #  [57469, 405]
            dd = data[0:2400,:]


            # y = np.reshape(dd,(dd.shape[1],dd.shape[0]))
            # z = dd.flatten()
            # ddd = np.zeros((3,z.shape[0]))
            #ddd = np.concatenate((z,z,z,z),axis=1)
            # ddd[0,:]=z
            # ddd[1,:]=z
            # ddd[2,:]=z
            # dddd = np.mat(ddd)
            # e = dddd.getT
            """
            R - The color red (from 0-255)
            G - The color green (from 0-255)
            B - The color blue (from 0-255)
            A - The alpha channel (from 0-255; 0 is transparent and 255 is fully visible)
            """
            r = np.repeat(dd, 4)
            print(r.shape)

            rrr = np.reshape(r,(r.shape[0]/4,4))
            rrr[:,3]=255

            rrr = rrr.flatten()
            print(rrr.shape)
            # dddzzz= np.reshape(ddd,(ddd.shape[1],ddd.shape[0]))
            # zzz = np.concatenate((dd,dd),axis=0)

            # dz = np.zeros((data.shape[0],3))

            # dz[:,0]= an
            # dz[:,1]= mois
            # dz[:,2]= jour

            # l= data.flatten()


            # h = np.append(dz, data, axis=1).astype(int)

            """
            print(f.__contains__(path))
            if not(f.__contains__(path)):
                print("create")
                sb = f.create_group(path)
            else:
                print("exist")
                sb = f.get(path)
            """

            # sb = f.require_group(path)





            #date = datetime.datetime(an,mois,jour,d[0],d[1],d[2],d[3])


        # mm = Image.fromarray(data,"L")

        # zz=ImageOps.colorize(mm, '#000000','#ffffff')
        # zz.show()
        #plt.imshow(data)
        #plt.show()
        #print(img)
        #pil_im = Image.fromarray(np.uint8(l))
        #pil_im.show()
        rr = r.flatten()



        response = {
            "status":"success",
            "message":"Image loaded succesfully.",
            "url":{"values":rrr.tolist()}
        }
        return response

    def find(self,name,obj):

        return obj



    def getCanvas(self,f,sel):

        """
        :param f:
        :param sel:
        :return:
        """

        filtre = json.loads(f)

        # 2014-01-01T00:00:00.000Z

        date = datetime.datetime.strptime(filtre["date"], "%Y-%m-%dT%H:%M:%S.%fZ")
        time = datetime.datetime.strptime(filtre["time"], "%Y-%m-%dT%H:%M:%S.%fZ")
        duree = filtre["dure"]
        frequence = filtre["frequence"]

        # path = date.strftime( "%Y/%m/%d")
        t = date.timetuple()
        path = "/" + str(t[0]) + "/" + str(t[1]) + "/" + str(t[2])



        values = []

        out = np.empty((600,400),dtype=np.uint8)

        with h5py.File("/datadam/routine/hdf5/NdaOldPython2.hdf5", "r") as hdf5:
            #grp = hdf5["/2014/1"]

            #grp.visit(values.append)
            #v = grp.visititems(self.find)
            #v.read_direct(out,np.s_[0:600,1,7:407])
            #print(out)


            print("open")
            #print(hdf5.keys())
            e = False
            if path in hdf5.keys():
                print(path)
                g = hdf5[path]
                e = True
                print(g)
                v = int(g[0:600,1,7:407])

                response = {
                "status":"success",
                "message":"Image loaded succesfully.",
                "url":[{"values":v.tolist()}]
            }

            else:
                print(path)
                g = hdf5[path]
                e = True
                #Image.fromarray(data,"L")


                v = g[:,1,7:407]
                vv = v.astype(np.uint8)
                print(vv.dtype)
                im = cv2.imdecode(vv,0 )
                cv2.imshow("test",vv)
                w = cv2.cvtColor(vv,cv2.COLOR_GRAY2BGR)
                cv2.imwrite('image.png',w)
                response = {
                "status":"error",
                "message":"Not Found",
                "url":ima.tolist()
            }


        return response



    def getViewFromFile(self,f,sel):

        """
        :param f:
        :param sel:
        :return:
        """

        filtre = json.loads(f)

        # 2014-01-01T00:00:00.000Z

        date = datetime.datetime.strptime(filtre["date"], "%Y-%m-%dT%H:%M:%S.%fZ")
        time = datetime.datetime.strptime(filtre["time"], "%Y-%m-%dT%H:%M:%S.%fZ")
        duree = filtre["dure"]
        frequence = filtre["frequence"]



        """
        function (doc, meta) {
          var d =  dateToArray(doc.datetime);
          for(var k in doc.data){
            emit([d[3],d[4],d[5],doc.polarite,parseInt(k)],{'x':parseInt(k),'y':doc.data[k]} );
          }
        }
        """
        fdam = "S" + date.strftime("%y%m%d")+".RT1"
        rdam = date.strftime("%Y")
        fname = "/datadam/routine/"+rdam+"/"+fdam

        if os.path.isfile(fname):

            file = open(fname, "rb")
            bytes_read = file.read(405)
            dt = 'c,2S,2S,45S,2S,1S,2S,1S,2S,1S,6S,340S'
            c = np.fromstring(bytes_read, dtype=dt)



            jour = int(c[0][4])


            mois = int(c[0][6])

            an = int(c[0][8])

            if 59 <= an <= 99:
                an =  int(an + 1900)
            else:
                an =  int(an + 2000)

            path = "/"+str(an)+"/"+str(mois)+"/"+str(jour)

            #dt=np.int8
            dt = '405B'

            # dt = np.uint8

            data = np.fromfile(fname, dtype=dt)

            data = np.delete(data, 0, 0)
            data = np.delete(data, [0,1,2,3], axis=1)

            data = np.delete(data, [400], axis=1)

            #  [57469, 405]
            dd = data[0:600,:]

            line = data[100,:]

        """
        np.set_printoptions(formatter={'all':lambda x: 'x: '+str(x)})

        print(line)
        n = 0

        values = np.array2string(

            dd,formatter={
                "all":lambda x:

                    "{'y': "+str(x)+"},"
            }
        )
        """
        values = []
        it = np.nditer([dd, None])
        for x, y in it:

            values.append({'x':int(x),'y':int(y)})
        #print(it.operands[1])

        """
        all=[]
        m=0
        for v in np.nditer(data):
            values = []
            n = 0
            for y in np.nditer(data[v,:]):
                n +=1
                values.append({'x':int(n),'y':int(y)})
            all.append({"values":values})
        print(all)
        """
        """
        cb = Couchbase.connect(bucket='NdaOld')
        r = []
        view = views.iterator.View(cb, "dev_default", "spectre",
            limit=400,
            reduce="false")

         url":[{'values':[{'x':0,'y':35},{"x":1,"y":36...

        for result in view:
            r.append(result.value)
        j = [{"values":r}]
        """
        response = {
            "status":"success",
            "message":"Image loaded succesfully.",
            "url":[{"values":values}]
        }

        return response

    def setImport(self,f,sel):
        
        logging.basicConfig(filename='/data/var_nrh/orfee.import3.log',level=logging.DEBUG,\
            format='%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s')
        filtre = json.loads(f)
        datedeb = datetime.datetime.strptime(filtre["datedeb"], "%Y-%m-%dT%H:%M:%S.%f%z")
        
        
        
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
        

        es = Elasticsearch(['master-rsdb'])
        #es.indices.create(index='nrh', ignore=400, timeout=30)
        #idl = pidly.IDL('/usr/local/bin/idl',long_delay=0.05)

        print("boucle date debut : ", datedeb)
    
        while counterDay < duree:

            print("Import ",counterDay)
            




            f = frequence
            
            integration = (filtre["integration"])
            # ext = ["01","01Z","01ZZ"]
            ext = ["fts"]
            """
            for i in list(range(1,9,1)):
                
                ext.append("0"+str(i))
                ext.append("0"+str(i)+"Z")
                ext.append("0"+str(i)+"ZZ")
            
            """
                
            # orf20150903_084724.fts
            #fRH = "2d" + date.strftime("%y%m%d") +".01"
            #fname = "/data/data_nrh/rh/"+date.strftime("%Y")+"/"+date.strftime("%m")+"/"+ date.strftime("%d")+"/"+fRH
            dir = "/data/data_nrh/orfees/"+datedeb.strftime("%Y")+"/"+datedeb.strftime("%m")+"/"
           
            os.chdir("/data/data_nrh/orfees/"+datedeb.strftime("%Y")+"/"+datedeb.strftime("%m")+"/")
            n = 0
            for  e in ext:
                file = "orf"+ datedeb.strftime("%Y%m%d") + "_*."+ e
                pattern = '^orf'+datedeb.strftime("%Y%m%d")+'_[0-9]{6}.'+ e
                # file = "2d" + datedeb.strftime("%y%m%d") + "." + e
                # fname = "/data/data_nrh/orfees/"+datedeb.strftime("%Y")+"/"+datedeb.strftime("%m")+"/"+file
                for f in os.listdir('.'):
                    
                    if (re.match(pattern, f)):
                        print(f)
                        fname = f
                        
                
                if os.path.isfile(fname):

                    print("file",fname)

                    
                    #idl.h = list(time.timetuple()[3:7])

                    #idl.kint=int(integration)
                    #idl.nof=int(f)
                    #idl.npol=0
                    
                    hdulist = fits.open(fname)
                    print( hdulist["FREQUENCIES"].header) 
                    hdulist.info()
                    fr = hdulist["FREQUENCIES"].data
                    print( hdulist["SPECTRA"].header)
                    scidata = hdulist["SPECTRA"].data
                    print(scidata.shape)
                    print(scidata.dtype.name)
                    #print (scidata[0])
                    print (scidata.field(0))
                    print( hdulist[2].columns)
                    try:
                        """
                        idl('.Reset_Session')
                        idl('@rh_common.inc')
                        
                        idl.fichier = fname
                        idl('status = RH_OPEN(fichier,/SEL,/MONO)')
                        idl('s = status')

                        if idl.s:
                            print("ok", fname)
                        else:
                            print("PB", fname)
                            break
                        idl('IF  status THEN BEGIN')

                        idl('typ=entFI.typ')

                        idl('dat=entFI.dat')

                        d = datetime.date(idl.dat[2],idl.dat[1], idl.dat[0])
                        idl('hdeb=entFI.hdeb')
                        idl('hfin=entFI.hfin')
                        

                        try:
                           
                            datedeb = datetime.datetime(idl.dat[2],idl.dat[1], idl.dat[0],idl.hdeb[0],idl.hdeb[1], idl.hdeb[2], idl.hdeb[3]*10000, pytz.UTC)
                            #sdeb = ((idl.hdeb[0]*60 + idl.hdeb[1])*60 + idl.hdeb[2])*60 + idl.hdeb[3]/100
                            datefin = datetime.datetime(idl.dat[2],idl.dat[1], idl.dat[0],idl.hfin[0],idl.hfin[1], idl.hfin[2], idl.hfin[3]*10000, pytz.UTC)
                            
                            #sfin = ((idl.hfin[0]*60 + idl.hfin[1])*60 + idl.hfin[2])*60 + idl.hfin[3]/100
                            
                        except ValueError as e:
                            print(e)
                            logging.error('IDL %s: %s %s %s', idl.hdeb.tolist(),idl.hfin.tolist(),file, e)
                            break;
                        except:
                            print("problem break")
                            break;
                        
                        idl('frq=entFI.frq')
                        idl('itg=entFI.itg')
                        idl('dec=entFI.dec')
                        idl('hg=entFI.hg')
                        idl('trj=entFI.trj')
                        idl('comp=entFI.comp')
                        idl('cyclms=entFI.cyclms')
                        idl('d_obs=entFI.d_obs')

                        idl('corel=entFI.corel')
                        """
                        
                        
                        
                        entete={}
                        entete['typ'] = "entFI"
                        entete['typ'] = idl.typ.tolist()
                        entete['frq']= idl.frq.tolist()
                        entete['itg']= idl.itg.tolist()
                        entete['dec']= idl.dec.tolist()
                        entete['hg']= idl.hg.tolist()
                        entete['hdeb']= datedeb
                        #entete['sdeb']= sdeb
                        entete['hfin']= datefin
                        #entete['sfin']= sfin
                        #entete['hdeb']= timedeb.strftime("%H:%M:%S.%f")
                        #entete['hfin']= timefin.strftime("%H:%M:%S.%f")
                        entete['trj']= idl.trj.tolist()
                        entete['comp']= idl.comp.tolist()
                        entete['cyclms']= idl.cyclms.tolist()
                        entete['d_obs']= idl.d_obs.tolist()

                        entete['corel']= idl.corel.tolist()

                        entete['@timestamp'] = datedeb
                        entete['file'] = file
                        entete['ext'] = e
                        entete['ord'] = n
                        #entete['descrip'] = idl.descrip.tolist()
                        #entete['activ'] = idl.activ.tolist()
                        #entete['pannes'] = idl.pannes.tolist()
                        
                        """
                        DATE-OBS : date de début d’observation ; 2014-02-01
                        TIME-OBS : heure de début d’observation ;08 :18 :02.000
                        DATE-END : date de fin d’observation
                        TIME-END : heure de fin d’observation

                        PHYSPARA : ‘I+V’ ; paramètres observés (Stokes)

                        OBSERVATORY : ‘Observatoire de Paris – Nançay’ (a des noms divers selon les fichiers)

                        OBS-TYPE : ‘radio’
                        OBS-SUBTYPE : ‘visibility’
                        TELESCOPE : ‘ radio interferometer’
                        INSTRUMENT : ‘NRH’

                        SOURCE : ‘SUN’, ou GYG,CASS,TAUR, VIRG, HYDR, DAB
                        FREQ-START : fréquence de départ, en MHZ 
                        FREQ-STOP : fréquence de fin, MHZ 
                        BUNITS : ‘SFU’
                        EXP-TIME : sampling time, millisec
                        TIME-STEP : integration time, millisec
                        Entetes fixe
                        
                        """
                        entete['physpara'] = 'I+V'
                        entete['observatory'] = 'Observatoire de Paris – Nançay'
                        entete['obs-type'] = 'radio'
                        entete['obs-subtype'] = 'visibilité'
                        entete['telescope'] = 'radio interferometer'
                        entete['instrument'] = 'nrh'
                        entete['source'] = 'sun'
                        entete['bunits'] = 'sfu'
                        
                        
                        
                        
                        
                        
                        
                        
                        
                    except:
                        idl('exit')
                        print ("Unexpected error:", sys.exc_info()[0])

                    body = {"query":
                                {"bool":
                                    { "must":
                                        [{ "term":
                                            {"@timestamp":datetime.datetime(idl.dat[2],idl.dat[1], idl.dat[0],idl.hdeb[0],idl.hdeb[1], idl.hdeb[2], idl.hdeb[3]*1000, pytz.UTC)}
                                            },
                                         {  "term" :
                                                {"file":file.lower() }
                                        }]
                                    }
                                }
                            }
                    #print(body)
                    #print(entete)
                    """
                    body ={"query": { "term":{
                    '@timestamp':datedeb}}}

                    GET _search
                    {"bool": {
                      "must": [{
                          "term": {
                            "timestamp": "2013/12/06T08:12:38"
                            }
                          }
                       , {
                         "term": {
                           "file": "2d131206.01"

                         }
                      }
                     ]
                    }}
                    """
                    #res = es.search(index="nrh", doc_type='entFI', body={"query": { "term":{
                    #'@timestamp':datedeb}}},fields='_id')
                    res = es.search(index="nrh",
                        doc_type='entFI',
                        body=body,fields='_id')
                    print(res)

                    if res['hits']['total'] > 0:
                        print("update")
                        _id =res['hits']['hits'][0]['_id']
                        res = es.index(index="nrh", doc_type='entFI', id = _id, body=entete, timeout=30 )
                        print(res)
                    else:

                        print("new")
                        res = es.index(index="nrh", doc_type='entFI', body=entete, timeout=30 )

                    n += 1

            counterDay += 1
            
            datedeb = datedeb + oneday

        response = {
            "status":"success",
            "message":"Import succesful",

            "time":t,
            "date":d

        }
        #idl.close()
        idl('exit')
        return response
        
    def setVideo(self,f,sel):
        logging.basicConfig(filename='/var/www/backend/nrh.import2.log',level=logging.DEBUG,\
            format='%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s')
        filtre = json.loads(f)
        datedeb = datetime.datetime.strptime(filtre["datedeb"], "%Y-%m-%dT%H:%M:%S.%fZ")
        #datefin = datetime.datetime.strptime(filtre["datefin"], "%Y-%m-%dT%H:%M:%S.%fZ")
        duree = int(filtre["dure"])
        frequence = filtre["frequence"]
        recepteur = filtre["recepteur"]

        oneday = datetime.timedelta(days=1)
        counterDay = 0
        DU = datetime.timedelta(days=duree)
        DD = date
        DF = date + DU

        oneday = datetime.timedelta(days=1)

        counterDay = 1


        es = Elasticsearch(['master-rsdb'])
        es.indices.create(index='nrh', ignore=400, timeout=30)
        idl = pidly.IDL('/usr/local/bin/idl',long_delay=0.05)

        print("boucle date debut : ", date)

        while counterDay < duree:

            print("Import ",counterDay)




            f = frequence
            integration = (filtre["integration"])
            ext = ["1640"]
            #fRH = "2d" + date.strftime("%y%m%d") +".01"
            #fname = "/data/data_nrh/rh/"+date.strftime("%Y")+"/"+date.strftime("%m")+"/"+ date.strftime("%d")+"/"+fRH
            #nrh09092006_1640.mpg
            n = 0
            for  e in ext:
                file = "nrh" + date.strftime("%d%m%Y") +"_"+e+".mpg"
           
                # file = "2d" + date.strftime("%y%m%d") + "." + e
                fname = "/data/data_nrh/rh/"+date.strftime("%Y")+"/"+date.strftime("%m")+"/"+ date.strftime("%d")+"/"+file


                if os.path.isfile(fname):




                    #idl.h = list(time.timetuple()[3:7])

                    #idl.kint=int(integration)
                    #idl.nof=int(f)
                    #idl.npol=0
                    try:
                        idl('.Reset_Session')
                        idl('@rh_common.inc')
                        idl.fichier = fname
                        idl('status = RH_OPEN(fichier,/SEL,/MONO)')
                        idl('s = status')

                        if idl.s:
                            print("ok", fname)
                        else:
                            print("PB", fname)
                            break
                        idl('IF  status THEN BEGIN')

                        idl('typ=entFI.typ')

                        idl('dat=entFI.dat')

                        d = datetime.date(idl.dat[2],idl.dat[1], idl.dat[0])
                        idl('hdeb=entFI.hdeb')
                        idl('hfin=entFI.hfin')


                        try:
                           
                            datedeb = datetime.datetime(idl.dat[2],idl.dat[1], idl.dat[0],idl.hdeb[0],idl.hdeb[1], idl.hdeb[2], idl.hdeb[3]*1000, pytz.UTC)
                            sdeb = ((idl.hdeb[0]*60 + idl.hdeb[1])*60 + idl.hdeb[2])*60 + idl.hdeb[3]/100
                            datefin = datetime.datetime(idl.dat[2],idl.dat[1], idl.dat[0],idl.hfin[0],idl.hfin[1], idl.hfin[2], idl.hfin[3]*1000, pytz.UTC)
                            
                            sfin = ((idl.hfin[0]*60 + idl.hfin[1])*60 + idl.hfin[2])*60 + idl.hfin[3]/100
                            
                        except ValueError as e:
                            print(e)
                            logging.error('IDL %s: %s %s %s', idl.hdeb.tolist(),idl.hfin.tolist(),file, e)
                            break;
                        except:
                            print("problem break")
                            break;
                        idl('frq=entFI.frq')
                        idl('itg=entFI.itg')
                        idl('dec=entFI.dec')
                        idl('hg=entFI.hg')
                        idl('trj=entFI.trj')
                        idl('comp=entFI.comp')
                        idl('cyclms=entFI.cyclms')
                        idl('d_obs=entFI.d_obs')

                        idl('corel=entFI.corel')

                        entete={}
                        entete['typ'] = "entFI"
                        entete['typ'] = idl.typ.tolist()
                        entete['frq']= idl.frq.tolist()
                        entete['itg']= idl.itg.tolist()
                        entete['dec']= idl.dec.tolist()
                        entete['hg']= idl.hg.tolist()
                        entete['hdeb']= datedeb
                        entete['sdeb']= sdeb
                        entete['hfin']= datefin
                        entete['sfin']= sfin
                        #entete['hdeb']= timedeb.strftime("%H:%M:%S.%f")
                        #entete['hfin']= timefin.strftime("%H:%M:%S.%f")
                        entete['trj']= idl.trj.tolist()
                        entete['comp']= idl.comp.tolist()
                        entete['cyclms']= idl.cyclms.tolist()
                        entete['d_obs']= idl.d_obs.tolist()

                        entete['corel']= idl.corel.tolist()

                        entete['@timestamp'] = datedeb
                        entete['file'] = file
                        entete['ext'] = e
                        entete['ord'] = n
                    except:
                        idl('exit')
                        print ("Unexpected error:", sys.exc_info()[0])

                    body = {"query":
                                {"bool":
                                    { "must":
                                        [{ "term":
                                            {"@timestamp":datedeb}
                                            },
                                         {  "term" :
                                                {"file":file.lower() }
                                        }]
                                    }
                                }
                            }
                    #print(body)
                    #print(entete)
                    """
                    body ={"query": { "term":{
                    '@timestamp':datedeb}}}

                    GET _search
                    {"bool": {
                      "must": [{
                          "term": {
                            "timestamp": "2013/12/06T08:12:38"
                            }
                          }
                       , {
                         "term": {
                           "file": "2d131206.01"

                         }
                      }
                     ]
                    }}
                    """
                    #res = es.search(index="nrh", doc_type='entFI', body={"query": { "term":{
                    #'@timestamp':datedeb}}},fields='_id')
                    res = es.search(index="nrh",
                        doc_type='entFI',
                        body=body,fields='_id')
                    print(res)

                    if res['hits']['total'] > 0:
                        print("update")
                        _id =res['hits']['hits'][0]['_id']
                        res = es.index(index="nrh", doc_type='entFI', id = _id, body=entete, timeout=30 )
                        #print(res)
                    else:

                        #print("new")
                        res = es.index(index="nrh", doc_type='entFI', body=entete, timeout=30 )

                    n += 1

            counterDay += 1

            date = date + oneday

        response = {
            "status":"success",
            "message":"Import succesful",

            "time":t,
            "date":d

        }
        #idl.close()
        idl('exit')
        return response
