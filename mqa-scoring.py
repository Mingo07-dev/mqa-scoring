'''
YODA (Your Open DAta)
EU CEF Action 2019-ES-IA-0121
University of Cantabria
Developer: Johnny Choque (jchoque@tlmat.unican.es)

Fork:
BEOPEN 2023
Developer: Marco Sajeva (sajeva.marco01@gmail.com)
'''
import requests
import json
from rdflib import Graph
import argparse
import mqaMetrics as mqa
from fastapi import FastAPI, File, UploadFile, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import os
import uvicorn
from pydantic import BaseModel
import logging 
from typing import List
import xml.etree.ElementTree as ET

URL_EDP = 'https://data.europa.eu/api/mqa/shacl/validation/report'
HEADERS = {'content-type': 'application/rdf+xml'}
MACH_READ_FILE = os.path.join('edp-vocabularies', 'edp-machine-readable-format.rdf')
NON_PROP_FILE = os.path.join('edp-vocabularies', 'edp-non-proprietary-format.rdf')

def otherCases(pred, objs, g):
  for obj in objs:
    met = str_metric(obj, g)
    if met == None:
      print('   Result: WARN. Not included in MQA - '+ str_metric(pred, g))
    else:
      print('   Result: WARN. Not included in MQA - '+ str(met))

def str_metric(val, g):
  valStr=str(val)
  for prefix, ns in g.namespaces():
    if val.find(ns) != -1:
      metStr = valStr.replace(ns,prefix+":")
      return metStr

def load_edp_vocabulary(file):
  g = Graph()
  g.parse(file, format="application/rdf+xml")
  voc = []
  for sub, pred, obj in g:
    voc.append(str(sub))
  return voc

def edp_validator(file, weight):
  # print('* SHACL validation')
  try:
    rdfFile = open(file, "r")
  except Exception as e:
    raise SystemExit(e)
  with rdfFile:
    try:
      payload = rdfFile.read().replace("\n", " ")
      r_edp = requests.post(URL_EDP, data=payload.encode('utf-8'), headers=HEADERS)
      r_edp.raise_for_status()
    except requests.exceptions.HTTPError as err:
      raise SystemExit(err)
    report = json.loads(r_edp.text)
    if valResult(report):
      # print('   Result: OK. The metadata has successfully passed the EDP validator. Weight assigned 30')
      weight = weight + 30
    # else:
    #   print('   Result: ERROR. DCAT-AP errors found in metadata')
  return weight

def valResult(d):
  if 'sh:conforms' in d:
    return d['sh:conforms']
  for k in d:
    if isinstance(d[k], list):
      for i in d[k]:
        if 'sh:conforms' in i:
          return i['sh:conforms']

def get_metrics(g):
  metrics = {}
  for sub, pred, obj in g:
    if pred not in metrics.keys():
      metrics[pred] = None
  for pred in metrics.keys():
    obj_list=[]
    for obj in g.objects(predicate=pred):
      obj_list.append(obj)
    metrics[pred] = obj_list
  return metrics

def get_urls(g):
  voc = []
  for sub, pred, obj in g:
    voc.append(str(sub))
  return voc

#to avoid some properties on json response are missing
def prepareResponse():
  class Object(object):
    pass
  response = Object()
  response.accessURL = 0
  response.downloadURL = 0
  response.downloadURLResponseCode = 0
  response.keyword = 0
  response.theme = 0
  response.spatial = 0
  response.temporal = 0
  response.format = 0
  response.formatMachineReadable = 0
  response.formatNonProprietary = 0
  response.license = 0
  response.licenseVocabulary = 0
  response.contactPoint = 0
  response.mediaType = 0
  response.publisher = 0
  response.accessRights = 0
  response.accessRightsVocabulary = 0
  response.issued = 0
  response.modified = 0
  response.rights = 0
  response.byteSize = 0
  return response

def main(file):
  mach_read_voc = []
  non_prop_voc = []

  # to get file in input by command line
  # parser = argparse.ArgumentParser(description='Calculates the score obtained by a metadata according to the MQA methodology specified by data.europa.eu')
  # parser.add_argument('-f', '--file', type=str, required=True, help='RDF file to be validated')
  # args = parser.parse_args()

  response = prepareResponse()

  g = Graph()
  g.parse(data = file)
  try:
    mach_read_voc = load_edp_vocabulary(MACH_READ_FILE)
    non_prop_voc = load_edp_vocabulary(NON_PROP_FILE)
  except:
    mach_read_voc = '-1'
    non_prop_voc = '-1'
  try:
    weight = edp_validator(file, weight)
    response.shacl_validation = weight
  except:
    weight = 0
    response.shacl_validation = 0
  
  # print('   Current weight =',weight)

  metrics = get_metrics(g)
  urls = get_urls(g)
  f_res = {}
  f_res = f_res.fromkeys(['result', 'url', 'response'])
  m_res = {}
  m_res = m_res.fromkeys(['result', 'response'])

  for pred in metrics.keys():
    met = str_metric(pred, g)
    objs = metrics[pred]
    # print('*',met)
    if met == "dcat:accessURL":
      res = mqa.accessURL(objs, response)
      response = res
      weight = weight + res.accessURL
    elif met == "dcat:downloadURL":
      res = mqa.downloadURL(objs, response)
      response = res
      weight = weight + res.downloadURL
      if hasattr(res, "downloadURLResponseCode"):
        weight += res.downloadURLResponseCode
    elif met == "dcat:keyword":
      res = mqa.keyword(response)
      response = res
      weight = weight + res.keyword
    elif met == "dcat:theme":
      res = mqa.theme(response)
      response = res
      weight = weight + res.theme
    elif met == "dct:spatial":
      res = mqa.spatial(response)
      response = res
      weight = weight + res.spatial
    elif met == "dct:temporal":
      res = mqa.temporal(response)
      response = res
      weight = weight + res.temporal
    elif met == "dct:format":
      f_res = mqa.format(objs, mach_read_voc, non_prop_voc, response)
      res = f_res['response']
      response = res
      weight = weight + res.format
      if hasattr(res, "formatMachineReadable"):
        weight += res.formatMachineReadable
      if hasattr(res, "formatNonProprietary"):
        weight += res.formatNonProprietary
    if met == "dct:license":
      res = mqa.license(objs, response)
      response = res
      weight = weight + res.license
      if hasattr(res, "licenseVocabulary"):
        weight += res.licenseVocabulary
    elif met == "dcat:contactPoint":
      res = mqa.contactpoint(response)
      response = res
      weight = weight + res.contactPoint
    elif met == "dcat:mediaType":
      m_res = mqa.mediatype(objs, response)
      res = m_res['response']
      response = res
      weight = weight + res.mediaType
    elif met == "dct:publisher":
      res = mqa.publisher(response)
      response = res
      weight = weight + res.publisher
    elif met == "dct:accessRights":
      res = mqa.accessrights(objs, response)
      response = res
      weight = weight + res.accessRights
      if hasattr(res, "accessRightsVocabulary"):
        weight += res.accessRightsVocabulary
    elif met == "dct:issued":
      res = mqa.issued(response)
      response = res
      weight = weight + res.issued
    elif met == "dct:modified":
      res = mqa.modified(response)
      response = res
      weight = weight + res.modified
    elif met == "dct:rights":
      res = mqa.rights(response)
      response = res
      weight = weight + res.rights
    elif met == "dcat:byteSize":
      res = mqa.byteSize(response)
      response = res
      weight = weight + res.byteSize
    # else:
    #   otherCases(pred, objs, g)
    # print('   Current weight =',weight)

  # print('* dct:format & dcat:mediaType')
  if f_res['result'] and m_res['result']:
    response.dctFormat_dcatMediaType = 10
    weight = weight + 10
    # print('   Result: OK. The properties belong to a controlled vocabulary. Weight assigned 10')
    # print('   Current weight=',weight)
  else:
    response.dctFormat_dcatMediaType = 0
    # print('   Result: WARN. The properties do not belong to a controlled vocabulary')

  # print('\n')
  # print('Overall MQA scoring:', str(weight))
  response.overall = weight
  return response


app = FastAPI(title="BeOpen mqa-scoring")
logger = logging.getLogger(__name__)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Base model
class Options(BaseModel):
    xml: str


@app.post("/mqa")
async def useCaseConfigurator(options: Options):
# async def useCaseConfigurator():
    try:
        configuration_inputs = options
    except Exception as e:
        raise HTTPException(status_code=400, detail="Inputs not valid")
    try:
        if configuration_inputs.xml != None:
          global xml
          xml = configuration_inputs.xml

        return main(xml)

    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal Server Error" + str(e))


# if __name__ == "__main__":
#   main()

appPort = os.getenv("PORT", 8000)
if __name__ == "__main__":
    uvicorn.run(app, host='0.0.0.0', port=appPort)
    # uvicorn.run(app, host='localhost', port=appPort)