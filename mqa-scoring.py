'''
YODA (Your Open DAta)
EU CEF Action 2019-ES-IA-0121
University of Cantabria
Developer: Johnny Choque (jchoque@tlmat.unican.es)

Fork:
BEOPEN 2023
Developer: Marco Sajeva (sajeva.marco01@gmail.com)
'''

import math
import csv
import re
import traceback
import requests
import json
from rdflib import Graph
from fastapi import FastAPI, File, UploadFile, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import os
import uvicorn
from pydantic import BaseModel
import logging 
from typing import Optional

URL_EDP = 'https://data.europa.eu/api/mqa/shacl/validation/report'
HEADERS = {'content-type': 'application/rdf+xml'}
MACH_READ_FILE = os.path.join('edp-vocabularies', 'edp-machine-readable-format.rdf')
NON_PROP_FILE = os.path.join('edp-vocabularies', 'edp-non-proprietary-format.rdf')
LICENSE_FILE = os.path.join('edp-vocabularies', 'edp-licences-skos.rdf')
ACCESSRIGHTS_FILE = os.path.join('edp-vocabularies', 'access-right-skos.rdf')
MEDIATYPE_FILE_APPLICATION = os.path.join('edp-vocabularies', 'edp-mediatype-application.csv')
MEDIATYPE_FILE_AUDIO = os.path.join('edp-vocabularies', 'edp-mediatype-audio.csv')
MEDIATYPE_FILE_FONT = os.path.join('edp-vocabularies', 'edp-mediatype-font.csv')
MEDIATYPE_FILE_IMAGE = os.path.join('edp-vocabularies', 'edp-mediatype-image.csv')
MEDIATYPE_FILE_MESSAGE = os.path.join('edp-vocabularies', 'edp-mediatype-message.csv')
MEDIATYPE_FILE_MODEL = os.path.join('edp-vocabularies', 'edp-mediatype-model.csv')
MEDIATYPE_FILE_MULTIPART = os.path.join('edp-vocabularies', 'edp-mediatype-multipart.csv')
MEDIATYPE_FILE_TEXT = os.path.join('edp-vocabularies', 'edp-mediatype-text.csv')
MEDIATYPE_FILE_VIDEO = os.path.join('edp-vocabularies', 'edp-mediatype-video.csv')
MEDIATYPE_FILE_VIDEO = os.path.join('edp-vocabularies', 'edp-mediatype-video.csv')

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

def load_edp_vocabulary_csv(file):
  g = Graph()
  g.parse(file, format="text/csv")
  voc = []
  for sub, pred, obj in g:
    voc.append(str(sub))
  return voc

def edp_validator(file: str):
  check = False
  try:
    r_edp = requests.post(URL_EDP, data=bytes(file, 'utf-8'), headers=HEADERS)
    r_edp.raise_for_status()
  except requests.exceptions.HTTPError as err:
    print(traceback.format_exc())
    raise SystemExit(err)
  report = json.loads(r_edp.text)
  if valResult(report):
    check = True
  return check

def valResult(d):
  if 'shacl:conforms' in d:
    return d['shacl:conforms']
  for k in d:
    if isinstance(d[k], list):
      for i in d[k]:
        if 'shacl:conforms' in i:
          return i['shacl:conforms']

#to avoid some properties on json response are missing
def prepareResponse():
  class Object(object):
    pass
  response = Object()
  response.accessURL = 400
  response.downloadURL = False
  response.downloadURLResponseCode = 400
  response.format = False
  response.dctFormat_dcatMediaType = False
  response.formatMachineReadable = False
  response.formatNonProprietary = False
  response.license = False
  response.licenseVocabulary = False
  response.mediaType = False
  response.issued = False
  response.modified = False
  response.rights = False
  response.byteSize = False
  return response


def distribution_calc(str):
  mach_read_voc = []
  non_prop_voc = []
  license_voc = []
  
  response = prepareResponse()

  g = Graph()
  g.parse(data = str)
  
  try:
    mach_read_voc = load_edp_vocabulary(MACH_READ_FILE)
    non_prop_voc = load_edp_vocabulary(NON_PROP_FILE)
    license_voc = load_edp_vocabulary(LICENSE_FILE)
  except:
    print(traceback.format_exc())
    mach_read_voc = '-1'
    non_prop_voc = '-1'
    license_voc = '-1'

  accessURL_List = []
  downloadURLResponseCode_List = []
  dctFormat_dcatMediaType_List = []

  for sub, pred, obj in g:
    met = str_metric(pred, g)
    if met == "dcat:accessURL":
      try:
        res = requests.get(obj)
        accessURL_List.append(res.status_code)
      except:
        print(traceback.format_exc())
        accessURL_List.append(1100)

    elif met == "dcat:downloadURL":
      response.downloadURL = True
      try:
        res = requests.get(obj)
        downloadURLResponseCode_List.append(res.status_code)
      except:
        print(traceback.format_exc())
        downloadURLResponseCode_List.append(1100)

    elif (met == "dct:format" and obj != '' and obj != None) or met == "dct:MediaTypeOrExtent":
      response.format = True
      try:
        if (obj) in mach_read_voc:
          response.formatMachineReadable = True
        else:
          response.formatMachineReadable = False
        if (obj) in non_prop_voc:
          response.formatNonProprietary = True
        else:
          response.formatNonProprietary = False
      except:
        print(traceback.format_exc())
        response.formatMachineReadable = False
        response.formatNonProprietary = False
      try:
        g2 = Graph()
        g2.parse(obj, format="application/rdf+xml")
        if (obj, None, None) in g2: 
          dctFormat_dcatMediaType_List.append(True)
        else:
          dctFormat_dcatMediaType_List.append(False)
      except:
        print(traceback.format_exc())
        dctFormat_dcatMediaType_List.append(False)

    elif met == "dct:license":
      response.license = True
      try:
        if (obj) in license_voc:
          response.licenseVocabulary = True
        else:
          response.licenseVocabulary = False
      except:
        print(traceback.format_exc())
        response.licenseVocabulary = False

    elif met == "dcat:mediaType":
      response.mediaType = True
      try:
        mediatype = obj.replace('http://www.iana.org/assignments/media-types/','')
        mediatype = mediatype.replace('https://www.iana.org/assignments/media-types/','')
        found = False
        try:
          vocabularies = [MEDIATYPE_FILE_APPLICATION, MEDIATYPE_FILE_AUDIO, MEDIATYPE_FILE_FONT, MEDIATYPE_FILE_IMAGE, MEDIATYPE_FILE_MESSAGE, MEDIATYPE_FILE_MODEL, MEDIATYPE_FILE_MULTIPART, MEDIATYPE_FILE_TEXT, MEDIATYPE_FILE_VIDEO]
          for voc in vocabularies:
            with open(voc, 'rt') as f:
              reader = csv.reader(f, delimiter=',')
              for row in reader: 
                for field in row:
                  if field ==  mediatype:
                    found = True
                    break
              if found == True:
                break
          if found == True:
            dctFormat_dcatMediaType_List.append(True)
        except:
          print(traceback.format_exc())
          dctFormat_dcatMediaType_List.append(False)
      except:
        print(traceback.format_exc())
        dctFormat_dcatMediaType_List.append(False)

    elif met == "dct:issued":
      response.issued = True

    elif met == "dct:modified":
      response.modified = True

    elif met == "dct:rights":
      response.rights = True

    elif met == "dcat:byteSize":
      response.byteSize = True

  response.accessURL = most_frequent(accessURL_List)
  response.downloadURLResponseCode = most_frequent(downloadURLResponseCode_List)
  temp = True
  for el in dctFormat_dcatMediaType_List:
    if el == False:
      temp = False
      break
  response.dctFormat_dcatMediaType = temp
  return response

def most_frequent(List):
    counter = 0
    if(len(List) == 0):
      return 400
    num = List[0]
    for i in List:
        curr_frequency = List.count(i)
        if(curr_frequency> counter):
            counter = curr_frequency
            num = i
 
    return num

def dataset_calc(dataset_str, pre):

  class Object(object):
    pass
  response = Object()
  response.distributions = []

  accessRights_voc = []
  dt_copy = dataset_str
  distribution_start = [m.start() for m in re.finditer('(?=<dcat:distribution>)', dataset_str)]
  distribution_finish = [m.start() for m in re.finditer('(?=</dcat:distribution>)', dataset_str)]
  if len(distribution_start) == len(distribution_finish):
    for index, item in enumerate(distribution_start):
      distr_tag = dataset_str[distribution_start[index]:distribution_finish[index]+20]
      dt_copy = dt_copy.replace(distr_tag, '')
      distribution = pre + '<dcat:Dataset>' + distr_tag + '</dcat:Dataset>' +'</rdf:RDF>'
      response.distributions.append(distribution_calc(distribution))

    dt_copy = dt_copy.replace(dt_copy[dt_copy.rfind('<adms:identifier>'):dt_copy.rfind('</adms:identifier>')+18], '')
    g = Graph()
    g.parse(data = dt_copy)

    response.issued = 0
    response.modified = False
    response.keyword = False
    response.issuedDataset = False
    response.modifiedDataset = False
    response.theme = False
    response.spatial = False
    response.temporal = False
    response.contactPoint = False
    response.publisher = False
    response.accessRights = False
    response.accessRightsVocabulary = False
    response.accessURL = []
    response.downloadURL = 0
    response.downloadURLResponseCode = []
    response.format = 0
    response.dctFormat_dcatMediaType = 0
    response.formatMachineReadable = 0
    response.formatNonProprietary = 0
    response.license = 0
    response.licenseVocabulary = 0
    response.mediaType = 0
    response.rights = 0
    response.byteSize = 0

    try:
      accessRights_voc = load_edp_vocabulary(ACCESSRIGHTS_FILE)
    except:
      print(traceback.format_exc())
      accessRights_voc = '-1'
      
    try:
      res = edp_validator(dataset_str)
      if res == True:
        response.shacl_validation = True
      else:
        response.shacl_validation = False
    except:
      print(traceback.format_exc())
      response.shacl_validation = 0

    for sub, pred, obj in g:
      met = str_metric(pred, g)

      if met == "dct:issued":
        response.issued += 1
        response.issuedDataset = True

      elif met == "dct:modified":
        response.modified = True
        response.modifiedDataset = True

      elif met == "dcat:keyword":
        response.keyword = True

      elif met == "dcat:theme":
        response.theme = True

      elif met == "dct:spatial":
        response.spatial = True

      elif met == "dct:temporal":
        response.temporal = True

      elif met == "dcat:contactPoint":
        response.contactPoint = True

      elif met == "dct:publisher":
        response.publisher = True

      elif met == "dct:accessRights":
        response.accessRights = True
        try:
          if str(obj) in accessRights_voc:
            response.accessRightsVocabulary = True
          else:
            response.accessRightsVocabulary = False
        except:
            print(traceback.format_exc())
            response.accessRightsVocabulary = False
  
    tempArrayDownloadUrl = []
    tempArrayAccessUrl = []
    for distr in response.distributions:
      if distr.issued == True:
        response.issued += 1
      if distr.downloadURL == True:
        response.downloadURL += 1
      tempArrayDownloadUrl.append(distr.downloadURLResponseCode)
      tempArrayAccessUrl.append(distr.accessURL)
      if distr.license == True:
        response.license += 1
      if distr.licenseVocabulary == True:
        response.licenseVocabulary += 1
      if distr.byteSize == True:
        response.byteSize += 1
      if distr.rights == True:
        response.rights += 1
      if distr.format == True:
        response.format += 1
      if distr.formatMachineReadable == True:
        response.formatMachineReadable += 1
      if distr.formatNonProprietary == True:
        response.formatNonProprietary += 1
      if distr.mediaType == True:
        response.mediaType += 1
      if distr.dctFormat_dcatMediaType == True:
        response.dctFormat_dcatMediaType += 1
    response.issued = round(response.issued / (len(response.distributions)+ 1) * 100)
    response.downloadURL = round(response.downloadURL / len(response.distributions) * 100)
    list_unique = (list(set(tempArrayDownloadUrl)))
    for el in list_unique:
      response.downloadURLResponseCode.append({"code": el, "percentage": round(tempArrayDownloadUrl.count(el) / len(response.distributions) * 100)})
    list_unique = (list(set(tempArrayAccessUrl)))
    for el in list_unique:
      response.accessURL.append({"code": el, "percentage": round(tempArrayAccessUrl.count(el) / len(response.distributions) * 100)})
    response.license = round(response.license / len(response.distributions) * 100)
    response.licenseVocabulary = round(response.licenseVocabulary / len(response.distributions) * 100)
    response.byteSize = round(response.byteSize / len(response.distributions) * 100)
    response.rights = round(response.rights / len(response.distributions) * 100)
    response.format = round(response.format / len(response.distributions) * 100)
    response.formatMachineReadable = round(response.formatMachineReadable / len(response.distributions) * 100)
    response.formatNonProprietary = round(response.formatNonProprietary / len(response.distributions) * 100)
    response.mediaType = round(response.mediaType / len(response.distributions) * 100)
    response.dctFormat_dcatMediaType = round(response.dctFormat_dcatMediaType / (len(response.distributions)*2) * 100)

    if(response.modified == False):
      for distr in response.distributions:
        if distr.modified == True:
          response.modified = True
          break
    
  else:
    return -1
  return response

def find_nth(haystack: str, needle: str, n: int) -> int:
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start


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
    try:
        configuration_inputs = options
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=400, detail="Inputs not valid")
    try:
      xml = configuration_inputs.xml
      class Object(object):
        pass
      response = Object()
      response.datasets = []
      
      closing_index = 2
      if xml.rfind('<?xml', None, 10) == -1:
        closing_index = 1

      pre = xml[:find_nth(xml,'>',closing_index) ] + '>'
      dataset_start = [m.start() for m in re.finditer('(?=<dcat:Dataset)', xml)]
      dataset_finish = [m.start() for m in re.finditer('(?=</dcat:Dataset>)', xml)]
      if len(dataset_start) == len(dataset_finish):
        for index, item in enumerate(dataset_start):
          dataset = pre + xml[dataset_start[index]:dataset_finish[index]+15] + '</rdf:RDF>'
          result = dataset_calc(dataset, pre)
          if result == -1:
            raise HTTPException(status_code=400, detail="Could not sort distributions")
          else:
            response.datasets.append(result)
      else:
        raise HTTPException(status_code=400, detail="Could not sort datasets")
      
      if xml.rfind('<dcat:Catalog ') != -1:
        
        response.issued = 0
        response.modified = 0
        response.keyword = 0
        response.theme = 0
        response.spatial = 0
        response.temporal = 0
        response.contactPoint = 0
        response.publisher = 0
        response.accessRights = 0
        response.accessRightsVocabulary = 0
        response.accessURL = []
        response.accessURL_Perc = 0
        response.downloadURL = 0
        response.downloadURLResponseCode = []
        response.downloadURLResponseCode_Perc = 0
        response.format = 0
        response.dctFormat_dcatMediaType = 0
        response.formatMachineReadable = 0
        response.formatNonProprietary = 0
        response.license = 0
        response.licenseVocabulary = 0
        response.mediaType = 0
        response.rights = 0
        response.byteSize = 0
        response.shacl_validation = 0

        countDataset = 0
        countDistr = 0
        tempArrayDownloadUrl = []
        tempArrayAccessUrl = []
        for dataset in response.datasets:
          countDataset += 1
          if dataset.issuedDataset == True:
            response.issued += 1
          del dataset.issuedDataset
          if dataset.modifiedDataset == True:
            response.modified += 1
          del dataset.modifiedDataset
          if dataset.accessRights == True:
            response.accessRights += 1
          if dataset.accessRightsVocabulary == True:
            response.accessRightsVocabulary += 1
          if dataset.contactPoint == True:
            response.contactPoint += 1
          if dataset.publisher == True:
            response.publisher += 1
          if dataset.keyword == True:
            response.keyword += 1
          if dataset.theme == True:
            response.theme += 1
          if dataset.spatial == True:
            response.spatial += 1
          if dataset.temporal == True:
            response.temporal += 1
          if dataset.shacl_validation == True:
            response.shacl_validation += 1
          for distr in dataset.distributions:
            countDistr += 1
            if distr.issued == True:
              response.issued += 1
            if distr.modified == True:
              response.modified += 1
            if distr.byteSize == True:
              response.byteSize += 1
            if distr.rights == True:
              response.rights += 1
            if distr.license == True:
              response.license += 1
            if distr.licenseVocabulary == True:
              response.licenseVocabulary += 1
            if distr.downloadURL == True:
              response.downloadURL += 1
            tempArrayDownloadUrl.append(distr.downloadURLResponseCode)
            tempArrayAccessUrl.append(distr.accessURL)
            if distr.format == True:
              response.format += 1
            if distr.formatMachineReadable == True:
              response.formatMachineReadable += 1
            if distr.formatNonProprietary == True:
              response.formatNonProprietary += 1
            if distr.mediaType == True:
              response.mediaType += 1
            if distr.dctFormat_dcatMediaType == True:
              response.dctFormat_dcatMediaType += 1

        # distribution level percentages
        response.issued = round(response.issued / (countDataset + countDistr) * 100)
        response.modified = round(response.modified / (countDataset + countDistr) * 100)
        response.byteSize = round(response.byteSize / countDistr * 100)
        response.rights = round(response.rights / countDistr * 100)
        response.licenseVocabulary = round(response.licenseVocabulary / response.license * 100)
        response.license = round(response.license / countDistr * 100)
        response.downloadURL = round(response.downloadURL / countDistr * 100)
        list_unique = (list(set(tempArrayDownloadUrl)))
        for el in list_unique:
          if el in range(200, 399):
            response.downloadURLResponseCode_Perc += round(tempArrayDownloadUrl.count(el) / countDistr * 100)
          response.downloadURLResponseCode.append({"code": el, "percentage": round(tempArrayDownloadUrl.count(el) / countDistr * 100)})
        list_unique = (list(set(tempArrayAccessUrl)))
        for el in list_unique:
          if el in range(200, 399):
            response.accessURL_Perc += round(tempArrayAccessUrl.count(el) / countDistr * 100)
          response.accessURL.append({"code": el, "percentage": round(tempArrayAccessUrl.count(el) / countDistr * 100)})
        # response.downloadURLResponseCode = round(most_frequent(tempArrayDownloadUrl))
        # response.downloadURLResponseCode_Perc = round(tempArrayDownloadUrl.count(response.downloadURLResponseCode) / countDistr * 100)
        # response.accessURL = round(most_frequent(tempArrayAccessUrl))
        # response.accessURL_Perc = round(tempArrayAccessUrl.count(response.accessURL) / countDistr * 100)
        response.format = round(response.format / countDistr * 100)
        response.formatMachineReadable = round(response.formatMachineReadable / countDistr * 100)
        response.formatNonProprietary = round(response.formatNonProprietary / countDistr * 100)
        response.mediaType = round(response.mediaType / countDistr * 100)
        response.dctFormat_dcatMediaType = round(response.dctFormat_dcatMediaType / (countDistr*2) * 100)

        # dataset level percentages
        response.accessRightsVocabulary = round(response.accessRightsVocabulary / response.accessRights * 100)
        response.accessRights = round(response.accessRights / countDataset * 100)
        response.contactPoint = round(response.contactPoint / countDataset * 100)
        response.publisher = round(response.publisher / countDataset * 100)
        response.keyword = round(response.keyword / countDataset * 100)
        response.theme = round(response.theme / countDataset * 100)
        response.spatial = round(response.spatial / countDataset * 100)
        response.temporal = round(response.temporal / countDataset * 100)
        response.shacl_validation = round(response.shacl_validation / countDataset * 100)

        # weights
        response.keyword_Weight = math.ceil(30 / 100 * response.keyword)
        response.theme_Weight = math.ceil(30 / 100 * response.theme)
        response.spatial_Weight = math.ceil(20 / 100 * response.spatial)
        response.temporal_Weight = math.ceil(20 / 100 * response.temporal)
        response.contactPoint_Weight = math.ceil(20 / 100 * response.contactPoint)
        response.publisher_Weight = math.ceil(10 / 100 * response.publisher)
        response.accessRights_Weight = math.ceil(10 / 100 * response.accessRights)
        response.accessRightsVocabulary_Weight = math.ceil(5 / 100 * response.accessRightsVocabulary)
        response.accessURL_Weight = math.ceil(50 / 100 * response.accessURL_Perc)
        response.downloadURL_Weight = math.ceil(20 / 100 * response.downloadURL)
        response.downloadURLResponseCode_Weight = math.ceil(30 / 100 * response.downloadURLResponseCode_Perc)
        response.format_Weight = math.ceil(20 / 100 * response.format)
        response.dctFormat_dcatMediaType_Weight = math.ceil(10 / 100 * response.dctFormat_dcatMediaType)
        response.formatMachineReadable_Weight = math.ceil(20 / 100 * response.formatMachineReadable)
        response.formatNonProprietary_Weight = math.ceil(20 / 100 * response.formatNonProprietary)
        response.license_Weight = math.ceil(20 / 100 * response.license)
        response.licenseVocabulary_Weight = math.ceil(10 / 100 * response.licenseVocabulary)
        response.mediaType_Weight = math.ceil(10 / 100 * response.mediaType)
        response.rights_Weight = math.ceil(5 / 100 * response.rights)
        response.byteSize_Weight = math.ceil(5 / 100 * response.byteSize)
        response.issued_Weight = math.ceil(5 / 100 * response.issued)
        response.modified_Weight = math.ceil(5 / 100 * response.modified)
        response.shacl_validation_Weight = math.ceil(30 / 100 * response.shacl_validation)

        response.findability = response.keyword_Weight + response.theme_Weight + response.spatial_Weight + response.temporal_Weight
        response.accessibility = response.accessURL_Weight + response.downloadURL_Weight + response.downloadURLResponseCode_Weight
        response.interoperability = response.format_Weight + response.dctFormat_dcatMediaType_Weight + response.formatMachineReadable_Weight + response.formatNonProprietary_Weight + response.mediaType_Weight + response.shacl_validation_Weight
        response.reusability = response.license_Weight + response.licenseVocabulary_Weight + response.contactPoint_Weight + response.publisher_Weight + response.accessRights_Weight + response.accessRightsVocabulary_Weight 
        response.contextuality = response.rights_Weight + response.byteSize_Weight + response.issued_Weight + response.modified_Weight

        response.overall = response.findability + response.accessibility + response.interoperability + response.reusability + response.contextuality

      return response
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal Server Error" + str(e))
    
@app.post("/mqa/file")
async def useCaseConfigurator(file: UploadFile = File(...)):
  try:
    xml = file.file.read()
    file.file.close()
    return dataset_calc(xml)
  except Exception:
      print(traceback.format_exc())
      return {"message": "There was an error uploading the file"}

# if __name__ == "__main__":
#   main()

appPort = os.getenv("PORT", 8000)
if __name__ == "__main__":
    uvicorn.run(app, host='0.0.0.0', port=appPort)
    # uvicorn.run(app, host='localhost', port=appPort)