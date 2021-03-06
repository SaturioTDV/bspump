import logging
from .fileabcsource import FileABCSource

#

L = logging.getLogger(__file__)

#

class FileBlockSource(FileABCSource):


	def __init__(self, app, pipeline, id=None, config=None):
		super().__init__(app, pipeline, id=id, config=config)
		self.ProactorService = app.get_service("asab.ProactorService")


	async def read(self, filename, f):
		await self.Pipeline.ready()
		# Load the file in a worker thread (to prevent blockage of the main loop)
		event = await self.ProactorService.run(f.read)
		await self.process(event, {
			"filename": filename
		})
