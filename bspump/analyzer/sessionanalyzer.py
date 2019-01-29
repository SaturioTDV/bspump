import time
import logging
import numpy as np
import copy

import asab

from .analyzer import Analyzer

###

L = logging.getLogger(__name__)

###


class SessionAnalyzer(Analyzer):
	'''
	TODO: add lookup for formats and documentation
	'''


	def __init__(self, app, pipeline, cell_formats, cell_names, id=None, config=None):
		
		super().__init__(app, pipeline, id, config)
		self.CellNames = cell_names
		self.CellFormats = cell_formats
		if len(self.CellNames) != len(self.CellFormats):
			raise RuntimeError("Cell names and cell formats should be the same length!")

		self.CellNames.append("@timestamp")
		self.CellFormats.append('u') #unsigned integer for timestamp
		self._initialize_sessions()
		

	def _initialize_sessions(self):
		self.Sessions = np.zeros(0, dtype={'names': self.CellNames, 'formats': self.CellFormats})
		self.RowMap = {}
		self.RevRowMap = {}
		self.DeactivatedRows = set()


	def add_session(self, session_id, start_time):
		row = np.zeros(1, dtype={'names': self.CellNames, 'formats': self.CellFormats})
		row["@timestamp"] = start_time
		self.Sessions = np.append(self.Sessions, row) #discussable, we can preassign big matrix and then fill it untill end and then restructure
		row_counter = len(self.RowMap)
		self.RowMap[session_id] = row_counter
		self.RevRowMap[row_counter] = session_id


	def deactivate_session(self, session_id):
		row_counter = self.RowMap.get(session_id)
		
		if row_counter is None:
			return
		else:
			self.DeactivatedRows.add(row_counter)


	def rebuild_sessions(self, mode):
		
		if mode == "full":
			self._initialize_sessions()
		elif mode == "partial":
			new_row_map = {}
			new_rev_row_map = {}
			saved_indexes = []
			for key in self.RowMap.keys():
				value = self.RowMap[key]
				if value not in self.DeactivatedRows:
					new_row_map[key] = value
					new_rev_row_map[value] = key
					saved_indexes.append(value)

			new_sessions = self.Sessions[saved_indexes]
			self.Sessions = new_sessions
			self.RowMap = new_row_map
			self.RevRowMap = new_rev_row_map
			self.DeactivatedRows = set()

		else:
			L.warn("Unknown mode")


	def get_session(self, session_id):
		row = self.RowMap.get(session_id)
		if row is None:
			return None
		else:
			return self.Sessions[row]
	
	
	





	

