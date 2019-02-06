import abc
import requests

from ..abc.lookup import MappingLookup

class ElasticSearchLookup(MappingLookup):

	'''
The lookup that is linked with a ES.
It provides a mapping (dictionary-like) interface to pipelines.
It feeds lookup data from ES using a query.
It also has a simple cache to reduce a number of datbase hits.

Example:

class ProjectLookup(bspump.elasticsearch.ElasticSearchLookup):

	async def count(self, database):
		return await database['projects'].count_documents({})

	def find_one(self, database, key):
		return database['projects'].find_one({'_id':key})

	'''

	ConfigDefaults = {
		'index': '', # Specify an index
		'field':'' # Specify field name to match
		'scroll_timeout': '1m',
	}

	def __init__(self, app, lookup_id, es_connection, config=None):
		super().__init__(app, lookup_id=lookup_id, config=config)
		self.Connection = es_connection

		self.Index = self.Config['index']
		self.ScrollTimeout = self.Config['scroll_timeout']

		self.Count = -1
		self.Cache = {}

		metrics_service = app.get_service('asab.MetricsService')
		self.CacheCounter = metrics_service.create_counter("es.lookup", tags={}, init_values={'hit': 0, 'miss': 0})


	
	def _find_one(self, key):
		request = {
			"query": {
				"match" : {
					self.Config['field'] : key
				}
			}
		}
		
		response = requests.post(url+"/_search", json=request)
		data = json.loads(response.text)
		hit = None
		try:
			hit = data['hits']['hits'][0]
		except KeyError():
			pass

		return hit

	
	async def _count(self):
		prefix = "_count"
		request = {
			"query": {
				"match_all":{}
			}
		}

		url = self.Connection.get_url() + '{}/_search'.format(self.Index)
		async with self.Connection.get_session() as session:
			async with session.post(
				url,
				json=request,
				headers={'Content-Type': 'application/json'}
			) as response:

				if response.status != 200:
					data = await response.text() #!
					L.error("Failed to fetch data from ElasticSearch: {} from {}\n{}".format(response.status, url, data))
					break

				msg = await response.json()

		return msg["count"]


	async def load(self):
		self.Count = await self._count()


	def __len__(self):
		return self.Count


	def __getitem__(self, key):
		try:
			value = self.Cache[key]
			self.CacheCounter.add('hit', 1)
			return value
		except KeyError:
			v = self._find_one(key)
			self.Cache[key] = v
			self.CacheCounter.add('miss', 1)
			return v


	def __iter__(self):
		# TODO: bug!
		# scroll_id = None
		# request = {
		# 	"query": {
		# 		"match_all":{}
		# 	}
		# }

		# all_hits = {}
		# while True:
		# 	if scroll_id is None:
		# 		path = '{}/_search?scroll={}'.format(self.Index, self.ScrollTimeout)
		# 		request_body = request
		# 	else:
		# 		path = "_search/scroll"
		# 		request_body = {"scroll": self.ScrollTimeout, "scroll_id": scroll_id}

		# 	url = self.Connection.get_url() + path
		# 	response = requests.post(url+"/_search", json=request)
		# 	if response.status != 200:
		# 		data = response.text()
		# 		L.error("Failed to fetch data from ElasticSearch: {} from {}\n{}".format(response.status, url, data))
		# 		break
		# 	data = json.loads(response.text)

		# 	scroll_id = msg.get('_scroll_id')
			
		# 	if scroll_id is None:
		# 		break

		# 	hits = msg['hits']['hits']
			
		# 	if len(hits) == 0:
		# 		break
			
		# 	all_hits.update(hits)


		# return all_hits.__iter__()
