import sys
import json
from time import sleep
import splunklib.results as results

from splunklib.client import connect



service = connect(username='admin',password='',host='localhost',port='8089',owner='admin',app='search')


collection_name = "task_collection"

searchquery_normal = "search * | table JSESSIONID productId product_name price sale_price | where isnotnull(productId)"
job = service.jobs.create(searchquery_normal)

print("Fetching...")

while True:
    while not job.is_ready():
        pass
    if job["isDone"] == "1":
        sys.stdout.write("Done!\n")
        break
    sleep(2)

# Get the results and display them
res = results.JSONResultsReader(job.results(output_mode='json'))

job.cancel()

# Create Collection
collection = service.kvstore[collection_name]


if collection_name in service.kvstore:
    print("Collection %s found!" % collection_name)
else:
    service.kvstore.create(collection_name)


for row in res:
    collection.data.insert(json.dumps(row))

print("Collection data: %s" % json.dumps(collection.data.query(), indent=1))