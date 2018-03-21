import logging
import copy
from ..abcproc import Processor
from .internal import InternalSource

#

L = logging.getLogger(__name__)

#

class TeeProcessor(Processor):


	ConfigDefaults = {
	}


	def __init__(self, app, pipeline, target, id=None, config=None):
		super().__init__(app, pipeline, id=id, config=config)

		self.Source = None

		self._target = target
		self._svc = app.get_service("bspump.PumpService")


	def start(self):
		if self.Source is None:
			source = self._svc.locate(self._target)
			if source is None:
				L.warning("TeeProcessor '{}' cannot find source '{}'".format(self.Id, self._target))
				return

			if not isinstance(source, InternalSource):
				L.warning("TeeProcessor '{}' require InternalSource as target, not '{}'".format(self.Id, self._target))
				return

			self.Source = source


	def process(self, event):
		event_copy = copy.deepcopy(event)
		self.Source.put_nowait(event_copy)
		return event