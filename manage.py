# management layer for connecting to central server
import random
import socket
import sys
from mynet import ListenSocket, DgramSocket, StreamSocket, Timer, Event

class Manager:
	def __init__(self, client, host, server):
		self.client = client
		self.host = host
		self.server = server

		self.options = client.options()
		self.socket = StreamSocket(socket.socket(), self)
		self.socket.connect(*self.server.split(':'))
		self.socket.write(['HELLO', self.host])

	def on_connect(self, socket):
		pass # nothing to do

	def on_error(self, socket):
		sys.exit('lost connection to server')

	def on_data(self, socket, data):
		pos = data.find('\n')
		if pos < 0: # need to wait for new line
			return 0
		elif pos == 0:
			return 1 # just a keep-alive

		args = data[0:pos].split(' ')

		# possible messages:
		# KILL -- terminate self
		# START bootstrap -- initialize using bootstrap
		# STOP -- stop server, but keep control connection active
		if args[0] == 'KILL':
			sys.exit('killed by server')
		elif args[0] == 'START':
			self.do_start(args[1])
		elif args[0] == 'STOP':
			self.do_stop()
		else:
			print 'unknown message:', ' '.join(args)

		return pos + 1

	def do_start(self, bootstrap):
		port = random.randrange(10000, 65536)
		opts = {}

		# set up datagram socket
		if 'dgram_socket' in self.options:
			dgram_socket = DgramSocket(port, self.client)
			opts['dgram_socket'] = dgram_socket

		# set up server socket
		if 'listen_sock' in self.options:
			server_socket = ListenSocket(port, self.client)
			opts['listen_sock'] = server_socket

		if 'boot_peer' in self.options and bootstrap != 'none':
			opts['boot_peer'] = bootstrap

		opts['listen_addr'] = '%s:%d' % (self.host, port)

		self.client.start(opts)

		self.socket.write(['STARTED', self.host, str(port)])

	def do_stop(self):
		self.client.stop()
		self.socket.write(['STOPPED', self.host])

	def run(self):
		Event.dispatch()
