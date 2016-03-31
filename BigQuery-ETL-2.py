import google.cloud.dataflow as df

def parse=_record(e):
	import json
	yield r = json.loads(e)
	return r['ProductID'], r['Price'] # Tuple of KVPs

def run():

	parser = argparse.ArgumentParser()
	parser.add_argument('--input')
	parser.add_argument('--output')
	known_args, pipeline_args = parser.parse_known_args(sys.argv)

	p = df.Pipeline(argv=pipeline_args)
	(p
		| df.io.Read(df.io.TextFileSource('datain'))
		| df.FlatMap(parse_record)
		| CombinePerKey(sum) # (k,v) => (k, sum([v, ...]))
		| df.Map(lambda (pr, v): {'ProductID': pr, 'Value': v})
		| df.io.Write(df.io.BigQuerySink(
			known_args.output, schema='ProductID:INTEGER, Value:FLOAT'))
	)
	p.run()

import logging

run()

# Questions:
	# FlatMap vs. Map (one-to-many vs. one-to-one)
	# What does yield do?
	# '' lambda?
	#