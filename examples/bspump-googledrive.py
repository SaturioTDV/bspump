#!/usr/bin/env python3
import aiohttp
import logging
import asyncio
import asab
import asab.web
import asab.web.session
import bspump
import bspump.file
import bspump.common
import bspump.trigger
import os

import google.oauth2.credentials
import google_auth_oauthlib.flow
import googleapiclient.discovery

###

L = logging.getLogger(__name__)

###


class SamplePipeline(bspump.Pipeline):

	def __init__(self, app, pipeline_id):
		super().__init__(app, pipeline_id)

		self.Sink = bspump.file.FileCSVSink(app, self, config={'path': 'out.csv'})

		self.build(
			bspump.file.FileLineSource(app, self, config={'path': 'sample.csv', 'delimiter': ';'}),
			bspump.common.PPrintProcessor(app, self),
			self.Sink
		)

		self.PubSub.subscribe("bspump.pipeline.cycle_end!", self.on_cycle_end)


	def on_cycle_end(self, event_name, pipeline):
		'''
		This ensures that at the end of the file scan, the target file is closed
		'''
		self.sink.rotate()



class Application(bspump.BSPumpApplication):

	AUTH_URL_PATH = "/auth"
	OAUTH2CALLBACK_URL_PATH = "/oauth2callback"
	CLIENT_SECRETS_FILE = "./client_secret.json"
	SCOPES = 'https://www.googleapis.com/auth/drive.metadata.readonly'

	def __init__(self):
		super().__init__()
		self.add_module(asab.web.Module)

		pump_svc = self.get_service("bspump.PumpService")
		web_svc = self.get_service("asab.WebService")
		asab.web.session.ServiceWebSession(self, "asab.ServiceWebSession", web_svc)

		# Construct and register Pipeline
		pl = SamplePipeline(self, 'SamplePipeline')
		pump_svc.add_pipeline(pl)

		# Auth endpoints
		web_svc.WebApp.router.add_get("/", self.get_root)
		web_svc.WebApp.router.add_get("/s", self.get_session)
		web_svc.WebApp.router.add_get(self.AUTH_URL_PATH, self.get_auth)
		web_svc.WebApp.router.add_get(self.OAUTH2CALLBACK_URL_PATH, self.get_oauth2callback)


	async def get_session(self, req):
		session = req['Session']
		return aiohttp.web.Response(text='Your session is {}\n'.format(session))


	async def get_root(self, req):
		if 'credentials' not in req['Session']:
			raise aiohttp.web.HTTPTemporaryRedirect(self.AUTH_URL_PATH)

		credentials = google.oauth2.credentials.Credentials(
			**req['Session']['credentials']
		)

		drive = googleapiclient.discovery.build(
			"drive", "v3",
			credentials=credentials,
			cache_discovery=False)

		files = drive.files().list(q="'Archiv' in parents").execute()

		req['Session']['credentials'] = self.credentials_to_dict(credentials)

		return aiohttp.web.Response(text='{}\n'.format(files))


	async def get_auth(self, req):
		# Create flow instance to manage the OAuth 2.0 Authorization Grant Flow steps.
		flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
			self.CLIENT_SECRETS_FILE, scopes=self.SCOPES
		)

		# TODO: make this work with base url for deployment to a subpath
		flow.redirect_uri = req.scheme+"://"+req.host+self.OAUTH2CALLBACK_URL_PATH

		authorization_url, state = flow.authorization_url(
			access_type='offline',
			include_granted_scopes='true'
		)

		# Store the state so the callback can verify the auth server response.
		req['Session']["state"] = state
		raise aiohttp.web.HTTPTemporaryRedirect(authorization_url)


	async def get_oauth2callback(self, req):
		state = req['Session'].get('state')

		flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
			self.CLIENT_SECRETS_FILE, scopes=self.SCOPES, state=state
		)

		# TODO: make this work with base url for deployment to a subpath
		flow.redirect_uri = req.scheme+"://"+req.host+self.OAUTH2CALLBACK_URL_PATH

		# Use the authorization server's response to fetch the OAuth 2.0 tokens.
		authorization_response = str(req.url)
		flow.fetch_token(authorization_response=authorization_response)

		# Store credentials in the session.
		# ACTION ITEM: In a production app, you likely want to save these
		#              credentials in a persistent database instead.
		credentials = flow.credentials
		req['Session']['credentials'] = self.credentials_to_dict(credentials)

		raise aiohttp.web.HTTPTemporaryRedirect("/")


	def credentials_to_dict(self, credentials):
		return {'token': credentials.token,
			'refresh_token': credentials.refresh_token,
			'token_uri': credentials.token_uri,
			'client_id': credentials.client_id,
			'client_secret': credentials.client_secret,
			'scopes': credentials.scopes}


if __name__ == '__main__':
	os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
	app = Application()
	app.run()
