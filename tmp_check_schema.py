import sys, json
data = json.load(sys.stdin)
schema = json.loads(data['extraMetadata']['schema'])
for f in schema['fields']:
    print(f['name'])
print('---')
for p in data['partitionToWriteStats'].keys():
    print(p)
