from bspump.abc.sink import Sink
import jupyter_client


class JupyterSink(Sink):
	""""
	Basic implementation of JupyterSink

	usage:
	You can access current event as variable 'event' inside your jupyter notebook

	If you're using Anaconda and OS X, the connection_file is stored at:
	/Users/[username]/Library/Jupyter/runtime/

	For Windows users:
	c:\Users[username]\AppData\Roaming\jupyter\runtime\

	If bspump and jupyter notebook are not running at the same server
	you need to copy jupyter
	"""

	ConfigDefaults = {
		'connection_file': None,
	}

	def __init__(self, app, pipeline, id=None, config=None):
		super().__init__(app, pipeline, id=id, config=config)

		self.connection_file = jupyter_client.find_connection_file(config['connection_file'])
		self.jupyter_client = jupyter_client.BlockingKernelClient(connection_file=self.connection_file)
		self.jupyter_client.load_connection_file()
		print(self.connection_file)

	def process(self, context, event):
		self.jupyter_client.execute('event = {}'.format(str(event)))
