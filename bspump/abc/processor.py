import abc
import asab

class ProcessorBase(abc.ABC, asab.ConfigObject):


	def __init__(self, app, pipeline, id=None, config=None):
		super().__init__("pipeline:{}:{}".format(pipeline.Id, id if id is not None else self.__class__.__name__), config=config)

		self.Id = id if id is not None else self.__class__.__name__
		self.Pipeline = pipeline


	@abc.abstractmethod
	def process(self, context, event):
		raise NotImplemented()


	def locate_address(self):
		return "{}.{}".format(self.Pipeline.Id, self.Id)


	def rest_get(self):
		return {
			"Id": self.Id,
			"Class": self.__class__.__name__,
			"PipelineId": self.Pipeline.Id,
		}


	def __repr__(self):
		return '%s(%r)' % (self.__class__.__name__, self.locate_address())


class Processor(ProcessorBase):
	pass
