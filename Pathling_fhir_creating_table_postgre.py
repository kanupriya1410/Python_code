# -*- coding: utf-8 -*-
"""
Created on Sun Jul  9 08:30:54 2023

@author: kanupriyag
"""

# valueBoolean changes
# for the second option trial of creating a table and config file via code,
# inserting as much as possible from fhir ,then through pathling code

import requests
import json
import psycopg2
from pathling import PathlingContext
from tempfile import NamedTemporaryFile
import os

# resourceArray=["Observation","Claim","Encounter","ExplanationOfBenefit",
#                "DiagnosticReport","Immunization","SearchParameter","Condition","MedicationAdministration","ImagingStudy",
#                "CarePlan","Medication","Goal","Patient","CareTeam","Practitioner","AllergyIntolerance","Consent",
#                "Composition","Device","Questionnaire","QuestionnaireResponse","Procedure","MedicationRequest","Organization"]
resourceArray=["Observation"]

# conn = psycopg2.connect(database="fhir_resource_mgmt", user="postgres", password="root", host="localhost",
#                             port="5432")
# conn = psycopg2.connect(database="fhir_realtime_export", user="postgres", password="root", host="localhost",
#                             port="5432")
#conn = psycopg2.connect(database="fhir_realtime_export", user="postgres", password="root", host="localhost",
 #                           port="5432")

conn = psycopg2.connect(database="realtime_export_fhirtest3", user = "eform_user", password = "eform@123$", host = "3.22.85.62", port = "5432")
print("Opened database successfully")
print("length of resource array",len(resourceArray))
cur = conn.cursor()

for resource in resourceArray:
    resourceName = resource

    # Get a Bundle of 500 Patient resources from the HAPI test server.
    # url = 'http://hapi.fhir.org/baseR4/Patient?_count=50'
    # url = "http://hapi.fhir.org/baseR4/" + resourceName + "?_count=50"
    # url = "http://hapi.fhir.org/baseR4/" + resourceName
    #http://3.144.97.27/
    url = "http://3.144.97.27:8000/fhir/" + resourceName
    headers = {'Accept': 'application/fhir+json'}
    response = requests.get(url, headers=headers)
    # Save the response to a temporary file.
    # f = open("file", "w")
    file='c:\\python_data\\org1.json'
    f = open("file", "w", encoding="utf-8")
    f.write(response.text)
    f.close()

    # Read the Bundle into Spark.
    pc = PathlingContext.create()
    bundle = pc.spark.read.text("file", wholetext=True)

    # Encode it using Pathling.
    # patients = pc.encode_bundle(bundle, "Patient")
    EncodedBundle = pc.encode_bundle(bundle, resourceName)

    pt = EncodedBundle.schema
    data1 = EncodedBundle.schema.json()

    jsonData = data1.replace("true", "True")
    jsondata = json.loads(data1)
    fhirResourceDescp = "Resource type is"
    resNameDesc = fhirResourceDescp + resourceName
    finalListForIndividualResource = []

    # table in database is created based on finalDictionary
    finalDictionary = {}

    # columns of config file transformers is queryFile
    queryFile = []

    # config file consist of data in queryFileDictionary
    queryFileDictionary = {}
    excess = []
    # finalDictionary["version"] = "INT"
    # hardcodefhirPath=resourceName+".meta.versionId"
    # queryFile.append({"columnName": "version","fhirPath":hardcodefhirPath,"columnType": "INT"})

    data = jsondata
    Valstr = ""
    Valtype = ""
    ConcatStr = ""
    fhirPathValstr=""
    ConcatStr1=""

    def func1(data, Valtype, Valstr, ConcatStr,fhirPathValstr,ConcatStr1):
        for key, value in data.items():
            if isinstance(value, dict):
                # print (str(key)+'->'+str(value)+ "*")
                if (str(key) == 'type'):
                    Valtype = 'conct'
                else:
                    Valtype = 'skip'
                func1(value, Valtype, Valstr, ConcatStr,fhirPathValstr,ConcatStr1)
            elif isinstance(value, list):
                if (str(key) == 'fields'):
                    ConcatStr = Valstr
                    ConcatStr1 = fhirPathValstr
                else:
                    ConcatStr = ''
                    ConcatStr1 = ''
                for val in value:
                    typeOfVal = ""
                    try:
                        # print("val====>", val['name'], "val type====>", val["type"]["type"])
                        trial = val["type"]["type"]
                        typeOfVal = 'json'
                        trial = 'BLOB'
                    except:
                        # print("val====>", val['name'], "val type====>", val["type"])
                        # trial = val["type"]
                        if (val["type"] == 'string'):
                            typeOfVal = 'varchar'
                            trial='STRING'
                        elif (val["type"] == 'array'):
                            typeOfVal = 'ARRAY'
                            trial = 'BLOB'
                        elif (val["type"] == 'boolean'):
                            typeOfVal = 'bool'
                            trial = 'BOOLEAN'
                        elif (val["type"] == 'struct'):
                            typeOfVal = 'json'
                            trial = 'BLOB'
                        elif (val["type"] == 'integer'):
                            typeOfVal = 'int'
                            trial = 'INT'
                        else:
                            typeOfVal = 'varchar'
                            trial = 'STRING'
                    if isinstance(val, str):
                        pass
                    elif isinstance(val, list):
                        pass
                    else:

                        if (Valtype == 'conct'):
                            if (ConcatStr == ""):
                                Valstr = val['name']
                                fhirPathValstr = val['name']
                            else:
                                Valstr = ConcatStr + "_" + val['name']
                                fhirPathValstr = ConcatStr1 + "." + val['name']
                        else:
                            if (ConcatStr == ""):
                                Valstr = val['name']
                                fhirPathValstr = val['name']
                            else:
                                Valstr = ConcatStr + "_" + val['name']
                                fhirPathValstr = ConcatStr1 + "." + val['name']

                        newfhirPathValstr = resourceName+"." + fhirPathValstr
                        finalListForIndividualResource.append(Valstr)
                        finalDictionary[str(Valstr)] = typeOfVal

                        # for toinsert dictionary
                        toinsert = {"columnName": "", "fhirPath": "", "columnType": ""}

                        toinsert["columnName"]=Valstr
                        toinsert["fhirPath"]=newfhirPathValstr
                        # toinsert["columnType"]=typeOfVal
                        toinsert["columnType"] = trial
                        # print("toinsert==>",toinsert)
                        if(typeOfVal == 'json'):
                            toinsert["columnName"] = Valstr
                            toinsert["fhirPath"] = newfhirPathValstr + ".first()"
                            toinsert["columnType"] = "STRING"
                            print("toinsert==>", toinsert)
                            queryFile.append(toinsert)

                        if (trial != 'BLOB'):
                            queryFile.append(toinsert)
                        # print(Valstr)
                        func1(val, Valtype, Valstr, ConcatStr,fhirPathValstr,ConcatStr1)

    func1(data, Valtype, Valstr, ConcatStr,fhirPathValstr,ConcatStr1)
    print("resource====>",resourceName)
    # print("finalListForIndividualResource=====>",finalListForIndividualResource)
    print("finalDictionary===>",finalDictionary)
    # print("queryFile==>",queryFile)
#%%

    if(resourceName=="Observation"):
        for pervalue in queryFile:
            if pervalue["columnName"]=="valueBoolean":
              pervalue["fhirPath"]="Observation.value"
        #%%


    query1 = f"CREATE TABLE {resourceName}()"
#%%
    try:
        cur.execute(query1)
    except Exception as e:
        print(resourceName," already exists")
    for j in finalDictionary:
        check=len(j)
        if(check<=90):
            try:
                query2 = f"ALTER TABLE {resourceName} ADD COLUMN {j} {finalDictionary[j]}"
                cur.execute(query2)
            except Exception as e:
                # print("err==>",e)
                pass
        else:
            # excess.append(j)
            jnew = j[0:90]
            try:
                query2 = f"ALTER TABLE {resourceName} ADD COLUMN {jnew} {finalDictionary[j]}"
                cur.execute(query2)
            except Exception as e:
                # print("err==>",e)
                pass
            for index in range(len(queryFile)-1):
                if queryFile[index]['columnName']==j:
                    # del queryFile[index]
                    jnew = j[0:90]
                    queryFile[index]['columnName'] = jnew
                    # print(len(inew), inew)
                    excess.append(jnew)

    conn.commit()
#%%
#%%
    print("len queryFile==>", len(queryFile))
    queryFileDictionary["columns"]=queryFile
    print("excess",excess)
    print(queryFileDictionary)
    with open('c:\\python_data\\en_data.json', 'w') as outfile:
        json.dump(queryFileDictionary, outfile)
#%%
    # # Serializing json
    # json_object = json.dumps(queryFileDictionary, indent=4)
    # file_path = r'c:\python_data\consent.json'
    
    # with open(file_path, 'w') as outfile:
    #     json.dump(json_object, outfile)

#%%
    # Writing to sample.json
    
    json_object = json.dumps(queryFileDictionary, indent=4)
    file_path = r'c:\python_data\en_data.json'
    
    with open(file_path, 'w') as outfile:
        outfile.write(json_object)

