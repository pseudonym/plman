#!/usr/bin/env python
# central server that keeps track of all peers in the system

import sys
from subprocess import Popen
import random
from mynet import ListenSocket, Event

class Peer:
	# status flags
	DEAD = 0
	STOPPED = 1
	STARTED = 2

	def __init__(self, host):
		self.host = host
		self.state = Peer.DEAD
		self.port = 0
		self.socket = None

	def disconnected(self):
		self.state = Peer.DEAD
		self.port = 0
		self.socket = None

	def hello(self, socket):
		self.state = Peer.STOPPED
		self.socket = socket

	def started(self, port):
		self.state = Peer.STARTED
		self.port = int(port)

	def stopped(self):
		self.state = Peer.STOPPED
		self.port = 0

	def get_state(self):
		def s2str(s):
			if s == Peer.DEAD:
				return 'DEAD'
			elif s == Peer.STOPPED:
				return 'STOPPED'
			elif s == Peer.STARTED:
				return 'STARTED'

		return ['STATE', self.host, s2str(self.state)]

class Daemon:
	def __init__(self, host, port, peers):
		self.host = host
		self.port = int(port)
		self.peers = {} # dictionary of Peer objects
		self.clients = set() # set of client sockets

		for i in peers:
			self.peers[i] = Peer(i)

		self.listen_sock = ListenSocket(self.port, self)

		self.keepalive_timer = Timer(15, self.keepalive_timer_cb)
		self.keepalive_timer.add()

		self.revive_timer = Timer(60, self.revive_timer_cb)
		self.revive_timer.add()

	def run(self):
		Event.dispatch()

	def on_connect(self, socket):
		pass

	def on_error(self, socket):
		# see if it's a peer; if it's a client, just ignore it
		for p in self.peers.values():
			if p.socket == socket:
				p.disconnected()
		self.clients.discard(socket) # just in case

	def on_data(self, socket, data):
		pos = data.find('\n')
		if pos < 0: # need to wait for new line
			return 0
		elif pos == 0:
			return 1 # just a keep-alive

		args = data[0:pos].split(' ')

		# possible messages (peer)
		# HELLO host -- peer is now active and ready to be started
		# STARTED host port -- peer is active and listening on given port
		# STOPPED host -- peer is stopped
		if args[0] == 'HELLO' and len(args) == 2:
			host = args[1]
			peer = self.peers[host]
			peer.hello(socket)
			self.broadcast(peer.get_state())
		elif args[0] == 'STARTED' and len(args) == 3:
			host = args[1]
			port = args[2]
			peer = self.peers[host]
			peer.started(port)
			self.broadcast(peer.get_state())
		elif args[0] == 'STOPPED' and len(args) == 2:
			host = args[1]
			peer = self.peers[host]
			peer.stopped()
			self.broadcast(peer.get_state())
		# possible messages (client)
		# CHELLO -- client is connected and would like status
		# CSTART host -- request to start host
		# CSTOP host -- request to stop host
		# CKILL host -- request to kill host
		elif args[0] == 'CHELLO' and len(args) == 1:
			for i in self.peers:
				socket.write(i.get_state())
			self.clients.add(socket)
		elif args[0] == 'CSTART' and len(args) == 2:
			self.do_start(args[1])
		elif args[0] == 'CSTOP' and len(args) == 2:
			self.do_stop(args[1])
		elif args[0] == 'CKILL' and len(args) == 2:
			self.do_kill(args[1])
		else:
			print 'unknown message:', ' '.join(args)

		return pos + 1

	def do_start(self, host):
		peer = self.peers[host]
		try:
			bootstrap = random.choice(['%s:%d' % (i.host,i.port) for i in self.peers.values() if i.state == Peer.STARTED])
		except IndexError:
			bootstrap = 'none'
		if peer.state == Peer.STOPPED:
			peer.socket.write(['START', bootstrap])

	def do_stop(self, host):
		peer = self.peers[host]
		if peer.state == Peer.STARTED:
			peer.socket.write(['STOP'])

	def do_kill(self, host):
		peer = self.peers[host]
		if peer.state == Peer.STARTED or peer.state == Peer.STOPPED:
			peer.socket.write(['KILL'])

	def broadcast(self, msg):
		for i in self.clients:
			i.write(msg)

	# send out a newline sometimes
	def keepalive_timer_cb(self):
		# re-add
		self.keepalive_timer = Timer(15, self.keepalive_timer_cb)
		self.keepalive_timer.add()

		for i in self.peers.values():
			if i.socket:
				i.socket.write([]) # send keepalive

	def revive_timer_cb(self):
		# re-add
		self.revive_timer = Timer(60, self.revive_timer_cb)
		self.revive_timer.add()

		for i in self.peers.values():
			if i.state == Peer.DEAD:
				self.do_spawn(i.host)

	def do_spawn(self, host):
		# spawn shell script that delivers payload
		Popen(['./deliver.sh', host, '%s:%d' % (self.host, self.port)])

if __name__ == '__main__':
	if len(sys.argv) < 4:
		sys.exit('Usage: python %s addr port peer...' % sys.argv[0])
	host = sys.argv[1]
	port = int(sys.argv[2])
	peers = sys.argv[3:]

	d = Daemon(host, port, peers)
	d.run()
