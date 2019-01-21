import time
import logging


import asab

from .analyzer import Analyzer

###

L = logging.getLogger(__name__)

###


class SessionAnalyzer(Analyzer):

	def __init__(self, app, pipeline, id=None, config=None):

		super().__init__(app, pipeline, id, config)
		self.Sessions = {}


	def add_session(self, session_id):
		self.Sessions[session_id] = {
			"start": time.time(),
			"members": {}
		}

	def delete_session(self, session_id):
		self.Sessions.pop(session_id)

	def get_session(self, session_id):
		self.Sessions.get(session_id)
	
	
	def add_to_session(self, session_id, member_id, member):
		if session_id not in self.Sessions:
			return False

		self.Sessions[session_id]['members'][member_id] = member
		return True

	

