import logging
import asyncio
import asab

from .abc.connection import Connection
from .abc.lookup import Lookup

#

L = logging.getLogger(__file__)

#

class BSPumpService(asab.Service):

	def __init__(self, app, service_name="bspump.PumpService"):
		super().__init__(app, service_name)

		self.Pipelines = dict()
		self.Connections = dict()
		self.Lookups = dict()


	def locate(self, address):
		if '.' in address:
			p, t = address.split('.', 1)
		else:
			p = address
			t = None
		pipeline = self.Pipelines.get(p)
		if pipeline is None:
			return None
		elif t is None:
			return pipeline

		if t[:1] == '*':
			for source in pipeline.Sources:
				if source.Id == t[1:]:
					return source
		else:
			for processor in pipeline.iter_processors():
				if processor.Id == t:
					return processor

		return None


	# Pipelines

	def add_pipeline(self, pipeline):
		if pipeline.Id in self.Pipelines:
			raise RuntimeError("Pipeline with id '{}' is already registered".format(pipeline.Id))
		self.Pipelines[pipeline.Id] = pipeline

	def add_pipelines(self, *pipelines):
		for pipeline in pipelines:
			self.add_pipeline(pipeline)


	# Connections

	def add_connection(self, connection):
		if connection.Id in self.Connections:
			raise RuntimeError("Connection '{}' already created".format(connection.Id))
		self.Connections[connection.Id] = connection
		return connection

	def add_connections(self, *connections):
		for connection in connections:
			self.add_connection(connection)

	def locate_connection(self, connection_id):
		if isinstance(connection_id, Connection): return connection_id
		try:
			return self.Connections[connection_id]
		except KeyError:
			raise KeyError("Cannot find connection id '{}' (did you call add_connection() ?)".format(connection_id))


	# Lookups

	def add_lookup(self, lookup):
		if lookup.Id in self.Lookups:
			raise RuntimeError("Lookup '{}' already created".format(lookup_id))		
		self.Lookups[lookup.Id] = lookup
		return lookup

	def add_lookups(self, *lookups):
		for lookup in lookups:
			self.add_lookup(lookup)

	def locate_lookup(self, lookup_id):
		if isinstance(lookup_id, Lookup): return lookup_id
		try:
			return self.Lookups[lookup_id]
		except KeyError:
			raise KeyError("Cannot find lookup id '{}' (did you call add_lookup() ?)".format(lookup_id))

	#

	async def initialize(self, app):
		# Await all lookups
		lookup_update_tasks = [lookup.ensure_future_update(app.Loop) for lookup in self.Lookups.values()]
		if len(lookup_update_tasks) > 0:
			done, pending = await asyncio.wait(lookup_update_tasks, loop=app.Loop)

		# Start all pipelines
		for pipeline in self.Pipelines.values():
			pipeline.start()


	async def finalize(self, app):
		# Stop all started pipelines
		if len(self.Pipelines) > 0:
			await asyncio.gather(*[pipeline.stop() for pipeline in self.Pipelines.values()], loop=app.Loop)
