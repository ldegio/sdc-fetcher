import fetcher
import sys

#
# The metrics to query.
# Note that this includes segmentation.
# For example, this query will download memory and cpu for each of the kubernetes
# deployments.
#
query = [
    {
        'id': 'kubernetes.deployment.name'
    },
    {
        'id': 'memory.bytes.used',
        'aggregations': 
        {
            'group': 'avg',
            'time': 'avg'
        }
    },
    {
        'id': 'cpu.used.percent',
        'aggregations': 
        {
            'group': 'avg',
            'time': 'avg'
        }
    }
]

#
# The additional query parameters.
# filter: allows to restric the downloaded data. For example, in this case we
#         exclude timelines that don't belong to any kubernetes deployment.
#         See http://python-sdc-client.readthedocs.io/en/latest/#sdcclient.SdSecureClient.get_data
#         for details. 
# source_type: the aggregation source unit. can be 'host' or 'container'.
# time_range: can be '1h', '1d', '1w', '2w', '4w'
#
info = {
    'filter': 'kubernetes.deployment.name != null', 
    'source_type': 'container', 
    'time_range': '2w'}

if len(sys.argv) != 2:
    print 'usage: %s <api_key>' % sys.argv[0]
    sys.exit(0)

#
# Get the data as a pandas datatable
#
ftch = fetcher.Fetcher(sys.argv[1])
df = ftch.fetch_as_datatable(info, query)
if df is not None:
    #
    # Use pandas to save the data to disk as csv
    #
    df.to_csv('data.csv')
