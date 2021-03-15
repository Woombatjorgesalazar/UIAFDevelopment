# encoding=UTF-8
#-*- coding: UTF-8 -*-
from chardet.universaldetector import UniversalDetector
import common as cm

def detectar_codec(Archivo):
    detector = UniversalDetector()
    file = open(Archivo, 'rb')
    if  cm.GLOBAL_CONFIG.get("local"):
        S=100
    else:
        S=10000

    i =0
    for line in file:
        detector.feed(line)
        if detector.done: break
        i += 1

        if i>S: break
    result =detector.close()
    file.close()
    return result


def find (list, value):
    try:
        list.index(value)
        return True
    except:
        return False