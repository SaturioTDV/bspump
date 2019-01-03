from bspump.abc.sink import Sink
import jupyter_client


class JupyterSink(Sink):
	"""
	Basic implementation of JupyterSink

	usage:
	You can access current event as variable 'event' inside your jupyter notebook

	If bspump and jupyter notebook are not running at the same server
	you need to copy notebook's connection file from jupyter's server to bspump's server
	and change ip address inside connection file to jupyter's server address.

	You can find location of  connection file of running jupyter instance
	by running these 2 lines in python:

	from jupyter_client import find_connection_file
	find_connection_file()

	If they are running on the same machine and
	there is just one instance of IPython you don't need to specify connection_file at all
	"""

	def __init__(self, app, pipeline, id=None, config=None):
		super().__init__(app, pipeline, id=id, config=config)

		self.connection_file = self.Config.get('connection_file')
		if self.connection_file is not None:
			self.connection_file = jupyter_client.find_connection_file(config['connection_file'])
		else:
			self.connection_file = jupyter_client.find_connection_file()

		self.jupyter_client = jupyter_client.BlockingKernelClient(connection_file=self.connection_file)
		self.jupyter_client.load_connection_file()
		print(self.connection_file)

	def process(self, context, event):
		self.jupyter_client.execute('event = {}'.format(str(event)))
