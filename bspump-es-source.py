#!/usr/bin/env python3
import logging
import bspump
import bspump.elasticsearch
import bspump.file
import bspump.common
import bspump.trigger

###

L = logging.getLogger(__name__)

###


class SamplePipeline(bspump.Pipeline):

	def __init__(self, app, pipeline_id):
		super().__init__(app, pipeline_id)

		request_body = {
			"query": {
				"bool": {
					"must": {
						"match_all": {}
					}
				}
			},
			"sort": [
				{"creation_date": {"order": "desc"}}
			]
		}

		self.build(
			bspump.elasticsearch.ElasticSearchSource(app, self, "ESConnection1", request_body=request_body).on(bspump.trigger.PubSubTrigger(app, "go!", pubsub=self.PubSub)),
			bspump.common.PPrintSink(app, self)
		)


if __name__ == '__main__':
	app = bspump.BSPumpApplication()

	svc = app.get_service("bspump.PumpService")
	svc.add_connection(
		bspump.elasticsearch.ElasticSearchConnection(app, "ESConnection1")
	)

	# Construct and register Pipeline
	pl = SamplePipeline(app, 'SamplePipeline')
	svc.add_pipeline(pl)

	pl.PubSub.publish("go!")

	app.run()
