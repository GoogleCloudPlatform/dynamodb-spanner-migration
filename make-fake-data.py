#
# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import boto3
from faker import Faker
import json
import random
import sys


# write fake recrods to DynamoDB table to be used to demonstrate migration

genFake = Faker()

#dynamodb max batch size
MAXBATCHSIZE = 25

client = boto3.client('dynamodb')




def writeItems(recordCount,tableName):
  
  fullbatches = int(recordCount / MAXBATCHSIZE)
  partialbatch = recordCount % MAXBATCHSIZE

  for _ in range(fullbatches):
    try:
      response = client.batch_write_item(RequestItems={tableName: makeFakeBatch()})
      
      unprocessed_records = len(response['UnprocessedItems'].get(tableName,[]))
      
      if unprocessed_records > 0:
          print('Unprocessed Items: %d' % len(response['UnprocessedItems'].get(tableName,[])))
      
    except Exception as e:
      print(e)

  if partialbatch >= 1:
    try:
      response = client.batch_write_item(RequestItems={tableName: makeFakeBatch(partialbatch)})
      
      unprocessed_records = len(response['UnprocessedItems'].get(tableName,[]))
      
      if unprocessed_records > 0:
          print('Unprocessed Items: %d' % len(response['UnprocessedItems'].get(tableName,[])))
      
    except Exception as e:
      print(e)






def makeFakeBatch(numrecords=MAXBATCHSIZE):

  fake_data = []

  for _ in range(numrecords):
    fake_record = {
      "Username": {"S" : genFake.user_name()+str(random.randrange(0,10000))},
      "Zipcode": {"N" : genFake.zipcode()},
      "Subscribed": {"BOOL" : True},
      "ReminderDate": {"S" : str(genFake.past_date())},
      "PointsEarned": {"N" : str(genFake.random_int())}
    }
  
    fake_data.append({"PutRequest":{"Item": fake_record}})
  
  return fake_data



def countRecords(tblName,scan_count=0,starting_key=None):
  
  if starting_key is None:
	  response = client.scan(TableName=tblName, Select='COUNT', ConsistentRead=True)
  else:
      response = client.scan(TableName=tblName, ConsistentRead=True, Select='COUNT', ExclusiveStartKey=starting_key)
    
    
  if 'LastEvaluatedKey' in response:
      scan_count += response['Count']
      return countRecords(tblName,scan_count,response['LastEvaluatedKey'])
  else:
      scan_count += response['Count']
      return scan_count






def main(argv):
  
  parser = argparse.ArgumentParser()
  parser.add_argument("--items", help="number of items to create in table", default=25000, required=False)
  parser.add_argument("--table", help="name of table", default="Migration", required=False)
  args = parser.parse_args()

  recordCount = int(args.items)
  tableName = args.table
  
  writeItems(recordCount,tableName)
  
  print('Items in Table: %d'% countRecords(tableName))




if __name__ == "__main__":
  main(sys.argv[1:])


