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

from __future__ import print_function

import site
import os
import json
import base64
from google.oauth2 import service_account

site.addsitedir(os.getcwd())

from google.cloud import pubsub_v1

print('Loading function')

def lambda_handler(event, context):
    credsjson = json.loads(base64.b64decode(os.environ['SVCACCT']))
    credentials = service_account.Credentials.from_service_account_info(credsjson)
    scoped_creds = credentials.with_scopes(['https://www.googleapis.com/auth/pubsub'])
    publisher = pubsub_v1.PublisherClient(credentials=scoped_creds)
    topic_path = publisher.topic_path(os.environ['PROJECT'], os.environ['TOPIC'])
    #process incoming event
    for record in event['Records']:
        print(record['eventID'])
        print(record['eventName'])
        print("DynamoDB Record: " + json.dumps(record['dynamodb'], indent=2))
        future = publisher.publish(topic_path, data=json.dumps(record['dynamodb']))
        print("Pub/Sub message_id: %s" % future.result())
    return 'Successfully processed {} records.'.format(len(event['Records']))
