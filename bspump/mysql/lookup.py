import abc

from ..abc.lookup import MappingLookup

class MySQLLookup(MappingLookup):

	'''
The lookup that is linked with a MySQL.
It provides a mapping (dictionary-like) interface to pipelines.
It feeds lookup data from MongoDB using a query.
It also has a simple cache to reduce a number of datbase hits.

Example:

class ProjectLookup(bspump.mysql.MySQLLookup):

	async def count(self, database):
		return await database['projects'].count_documents({})

	def find_one(self, database, key):
		return database['projects'].find_one({'_id':key})

	'''

	ConfigDefaults = {
		'database': '', # Specify a database if you want to overload the connection setting
		'key':'' # Specify key name used for search
	}

	def __init__(self, app, lookup_id, mysql_connection, config=None):
		super().__init__(app, lookup_id=lookup_id, config=config)
		self.Connection = mysql_connection

		self.Database = self.Config['database']
		self.Key = self.Config['key']

		if len(self.Database) == 0:
			self.Database = self.Connection.Database

		self.Count = -1
		self.Cache = {}

		self.Cursor = self.Connection.acquire().cursor()

		metrics_service = app.get_service('asab.MetricsService')
		self.CacheCounter = metrics_service.create_counter("mysql.lookup", tags={}, init_values={'hit': 0, 'miss': 0})


	def _find_one(self, database, key):
		# sync, easy
		# query = "SELECT * "
		return database[self.Config['collection']].find_one({self.Config['key']:key})

	
	async def _count(self, database):

		query = """SELECT COUNT(*) as "Number of Rows" FROM {};""".format(self.Database)
		await cur.execute(query)
		count = await cur.fetchone()[0] #??????S
		return count


	async def load(self):
		self.Count = await self._count(self.Connection.Client[self.Database])


	def __len__(self):
		return self.Count


	def __getitem__(self, key):
		try:
			value = self.Cache[key]
			self.CacheCounter.add('hit', 1)
			return key
		except KeyError:
			database = self.Connection.Client[self.Database].delegate
			v = self._find_one(database, key)
			self.Cache[key] = v
			self.CacheCounter.add('miss', 1)
			return v


	def __iter__(self):
		database = self.Connection.Client[self.Database].delegate
		collection = self.Config['collection']
		return database[collection].find().__iter__()
