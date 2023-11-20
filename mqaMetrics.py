'''
YODA (Your Open DAta)
EU CEF Action 2019-ES-IA-0121
University of Cantabria
Developer: Johnny Choque (jchoque@tlmat.unican.es)
'''
import requests
from rdflib import Graph, URIRef
import json

def accessURL(urls, response):
  checked = True
  for url in urls:
    try:
      res = requests.get(url)
      if res.status_code in range(200, 399):
        checked = checked and True
      else:
        checked = checked and False
    except:
      checked = checked and False
  if checked:
    response.accessURL = 50
    print('   Result: OK. Weight assigned 50')
  else:
    print('   Result: ERROR - Responded status code of HTTP HEAD request is not in the 200 or 300 range')
  return response

def downloadURL(urls, response):
  weight = 20
  checked = True
  print('   Result: OK. The property is set. Weight assigned 20')
  for url in urls:
    try:
      res = requests.get(url)
      if res.status_code in range(200, 399):
        checked = checked and True
      else:
        checked = checked and False
    except:
      checked = checked and False
  if checked:
    weight += 30
    print('   Result: OK. Weight assigned 30')
  else:
    print('   Result: ERROR - Responded status code of HTTP HEAD request is not in the 200 or 300 range')
  response.downloadURL = weight
  return response

def keyword(response):
  response.keyword = 30
  print('   Result: OK. The property is set. Weight assigned 30')
  return response

def theme(response):
  response.theme = 30
  print('   Result: OK. The property is set. Weight assigned 30')
  return response

def spatial(response):
  response.spatial = 20
  print('   Result: OK. The property is set. Weight assigned 20')
  return response

def temporal(response):
  response.temporal = 20
  print('   Result: OK. The property is set. Weight assigned 20')
  return response

def format(urls, mach_read_voc, non_prop_voc, response):
  mach_read_checked = True
  non_prop_checked = True
  found_checked = True
  print('   Result: OK. The property is set. Weight assigned 20')
  response.format = 20
  for url in urls:
    if str(url) in mach_read_voc:
      mach_read_checked = mach_read_checked and True
    else:
      mach_read_checked = mach_read_checked and False
    if str(url) in non_prop_voc:
      non_prop_checked = non_prop_checked and True
    else:
      non_prop_checked = non_prop_checked and False
    g = Graph()
    try:
      g.parse(url, format="application/rdf+xml")
      if (url, None, None) in g:
        found_checked = found_checked and True
      else:
        found_checked = found_checked and False
    except:
        found_checked = found_checked and False
  if mach_read_checked:
    print('   Result: OK. The property is machine-readable. Weight assigned 20')
    response.formatMachineReadable = 20
  else:
    print('   Result: ERROR. The property is not machine-readable')
  if non_prop_checked:
    print('   Result: OK. The property is non-propietary. Weight assigned 20')
    response.formatNonProprietary = 20
  else:
    print('   Result: ERROR. The property is not non-propietary')
  if found_checked:
    result = True
  else:
    result = False
  return {'result': result, 'url':str(url), 'response': response}

def license(urls, response):
  checked = True
  weight = 20
  print('   Result: OK. The property is set. Weight assigned 20')
  for url in urls:
    g = Graph()
    try:
      g.parse(url, format="application/rdf+xml")
      if (url, None, None) in g:
        checked = checked and True
      else:
        checked = checked and False
    except:
        checked = checked and False
  if checked:
    weight += 10
    print('   Result: OK. The property provides the correct license information. Weight assigned 10')
  else:
    print('   Result: ERROR. The license is incorrect -',str(url))
  response.license = weight
  return response


def contactpoint(response):
  response.contactPoint = 20
  print('   Result: OK. The property is set. Weight assigned 20')
  return response

def mediatype(urls, response):
  checked = True
  response.mediaType = 10
  print('   Result: OK. The property is set. Weight assigned 10')
  for url in urls:
    try:
      res = requests.get(str(url))
      if res.status_code != 404:
        checked = checked and True
      else:
        checked = checked and False
    except:
      checked = checked and False
  if checked:
    result = True
  else:
    result = False
  return {'result': result, 'response': response}

def publisher(response):
  response.publisher = 10
  print('   Result: OK. The property is set. Weight assigned 10')
  return response

def accessrights(urls, response):
  uri = URIRef('')
  checked = True
  isURL = True
  weight = 10
  print('   Result: OK. The property is set. Weight assigned 10')
  for url in urls:
    g = Graph()
    if type(url) != type(uri):
      isURL = False
      continue
    try:
      g.parse(url, format="application/rdf+xml")
      if (url, None, None) in g:
        checked = checked and True
      else:
        checked = checked and False
    except:
      checked = checked and False
  if isURL:
    if checked:
      weight = weight + 5
      print('   Result: OK. The property uses a controlled vocabulary. Weight assigned 5')
    else:
      print('   Result: ERROR. The license is incorrect -', str(url))
  else:
    print('   Result: ERROR. The property does not use a valid URL. No additional weight assigned')
  response.accessRights = weight
  return response

def issued(response):
  response.issued = 5
  print('   Result: OK. The property is set. Weight assigned 5')
  return response

def modified(response):
  response.modified = 5
  print('   Result: OK. The property is set. Weight assigned 5')
  return response

def rights(response):
  response.rights = 5
  print('   Result: OK. The property is set. Weight assigned 5')
  return response

def byteSize(response):
  response.byteSize = 5
  print('   Result: OK. The property is set. Weight assigned 5')
  return response

