import json
import logging
from google.cloud import bigquery
import base64

BQ_DATASET = 'mydataset'
BQ_TABLE = 'device_messages'
BQ = bigquery.Client()

def load_data(event, context):
  """Triggered from a message on a Cloud Pub/Sub topic.
  Args:
       event (dict): Event payload.
       context (google.cloud.functions.Context): Metadata for the event.
  """
  pubsub_message = base64.b64decode(event['data']).decode('utf-8')
  

  table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
  
  # rows_to_insert = [
  #   {"msg": pubsub_message},
  # ]
  print(" ".join(["-----", pubsub_message, "-----"]))
  
  errors = BQ.insert_rows_json(table,
                     json_rows=[json.loads(pubsub_message)],
                     )

  if errors != []:
      raise BigQueryError(errors)
      
  print('Successfully executed.')
      
      
class BigQueryError(Exception):
  '''Exception raised whenever a BigQuery error happened''' 

  def __init__(self, errors):
    super().__init__(self._format(errors))
    self.errors = errors

  def _format(self, errors):
    err = []
    for error in errors:
        err.extend(error['errors'])
    return json.dumps(err)
