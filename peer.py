#!/usr/bin/env python
import base64
import hashlib
import random
import socket
import sys
from mynet import Event, StreamSocket, DgramSocket, Timer, ListenSocket
from manage import Manager

class Trans:
	# transaction types (i.e., reasons for making DHT requests)
	FINGER = 0 # finding finger peers for us (index = finger table index)
	BACKUP = 1 # finding redundant successors in case real one dies
	PRUNE = 2 # are we still in charge of this file? if not, drop it
	GET = 10 # finding place for client to get data (client = client socket)
	PUT = 11 # finding place for client to put data (client = client socket, data = file data))
	SHOW = 12 # transaction is asking for peer list (client = client socket, timer = 30sec timer)

	next = 0 # incrementing count of transactions

	def __init__(self, type, main, arg1=None, arg2=None):
		self.type = type
		self.main = main

		def make_trans():
			num = Trans.next
			Trans.next += 1
			return '%s-%d' % (main.myname, num)
		self.id = make_trans()

		if self.type == Trans.FINGER:
			self.index = arg1
		elif self.type == Trans.BACKUP:
			pass # nothing special to do
		elif self.type == Trans.PRUNE:
			pass # nothing special to do
		elif self.type == Trans.GET:
			self.client = arg1
		elif self.type == Trans.PUT:
			self.client = arg1
			self.data = arg2
		elif self.type == Trans.SHOW:
			self.client = arg1
			# create a timer; allow 30 seconds for a show to complete
			def cb():
				self.remove()
				# close client connection; we won't receive any more peer responses
				self.client.close_when_done()
			# we use a timeout to determine the roll call being done because
			# getting the SHOW message back doesn't mean we've seen all PEER messages
			self.timer = Timer(10, cb)
			self.timer.add()

	def add(self):
		self.main.trans[self.id] = self

	def remove(self):
		if self.type == Trans.SHOW:
			self.timer.remove()
		del self.main.trans[self.id]

class Main:
	def __init__(self):
		self.DEBUG = False

	# start node
	def start(self, options):
		self.myname = options['listen_addr']
		self.prev = None # previous peer in DHT
		self.succsucc = None # successor of my successor; in case successor fails
		self.finger = [None] * 160 # list of finger connections, in order of distance; finger[0] is next node

		# number of consecutive ping rounds a peer has not responded
		# after 2 rounds, we consider it dead
		self.ping_fail = {}

		self.items = {} # items stored at this node
		self.trans = {} # list of active transactions: {id: Trans}

		self.timers = {} # timers, so that we can remove them when stopping
		self.sockets = set() # set of sockets so we can shut all of them down

		self.listen_sock = options['listen_sock']
		self.dgram_socket = options['dgram_socket']

		# set up timers
		tl = []
		tl.append(('ping', self.ping_timer_cb))
		tl.append(('backup', self.backup_timer_cb))
		tl.append(('finger', self.finger_timer_cb))
		tl.append(('stabilize', self.stabilize_timer_cb))
		#tl.append(('prune', self.prune_timer_cb)) # disabled because it's kind of broken
		for (name, fn) in tl:
			self.timers[name] = Timer(random.randrange(5, 10), fn)
			self.timers[name].add()

		# start bootstrap process by finding successor
		if 'boot_peer' in options:
			bootpeer = options['boot_peer']
			t = Trans(Trans.FINGER, self, 0)
			t.add()
			self.dgram_socket.send(bootpeer, ['FIND', make_id(self.myname), self.myname, t.id]) # find our successor
		else:
			self.finger[0] = self.myname
			self.prev = self.myname

		print 'started', make_id(self.myname), self.myname

	# stop node
	def stop(self):
		self.listen_sock.close()
		self.dgram_socket.close()

		# delete timers
		for t in self.timers.values():
			t.remove()
		self.timers = {}

		# delete sockets
		for i in self.sockets:
			i.close()
		self.sockets = set()

	# the optional features we want enabled
	def options(self):
		return set(['listen_sock', 'listen_addr', 'dgram_socket', 'boot_peer'])

	def on_connect(self, socket):
		self.sockets.add(socket)

	def on_error(self, socket):
		self.sockets.discard(socket)

		# client disconnected.... whatever, just remove its transactions
		for i in self.trans.values():
			def is_client_type(t):
				return t == Trans.GET or t == Trans.PUT or t == Trans.SHOW
			if is_client_type(i.type) and i.client == socket:
				print '%s disconnected, purging transaction %s' % (socket, i.id)
				t.remove()

	# called when data received from server (UDP) port
	def on_dgram(self, socket, data):
		args = data.split(' ')
		if self.DEBUG: print 'inU: %s\n' % ' '.join(args)

		# server->server UDP commands:
		# FIND hash ip:port transid -- server at ip:port wants to know who's responsible for the given hash
		# FOUND hash ip:port transid -- server at ip:port is responsible for your requested hash
		# GETP ip:port -- ip:port wants your predecessor
		# NOTIFY ip:port -- set predecessor to ip:port (suggestion)
		# PRED ip:port -- predecessor is ip:port
		# SHOW ip:port transid -- send ip:port information about yourself (sent around ring)
		# PEER ip:port transid -- response to SHOW
		# PING ip:port -- ping request from ip:port
		# PONG ip:port -- ping reply from ip:port
		if args[0] == 'FIND':
			(hash, peer, transid) = args[1:]
			self.find_forward(hash, peer, transid)
		elif args[0] == 'FOUND':
			(hash, peer, transid) = args[1:]
			self.handle_found(hash, peer, transid)
		elif args[0] == 'GETP':
			peer = args[1]
			if self.prev:
				self.dgram_socket.send(peer, ['PRED', self.prev])
		elif args[0] == 'NOTIFY':
			peer = args[1]
			my_id = make_id(self.myname)
			prev_id = self.prev and make_id(self.prev) or None
			peer_id = make_id(peer)
			if not self.prev or id_distance(peer_id, my_id) < id_distance(prev_id, my_id):
				print 'updating prev:', peer
				self.prev = peer
		elif args[0] == 'PRED':
			peer = args[1]
			my_id = make_id(self.myname)
			succ_id = make_id(self.finger[0])
			peer_id = make_id(peer)
			if id_distance(my_id, peer_id) < id_distance(my_id, succ_id):
				print 'updating finger[0] from succ.pred:', peer
				self.finger[0] = peer
		elif args[0] == 'SHOW':
			(peer, transid) = args[1:]
			if peer == self.myname:
				# we started it, so stop forwarding
				pass
			else:
				self.dgram_socket.send(self.finger[0], args)
				self.dgram_socket.send(peer, ['PEER', self.myname, transid])
		elif args[0] == 'PEER':
			(peer, transid) = args[1:]
			t = self.trans[transid]
			t.client.write(['CPEER', make_id(peer), peer])
		elif args[0] == 'PING':
			# reply to ping
			peer = args[1]
			self.dgram_socket.send(args[1], ['PONG', self.myname])
		elif args[0] == 'PONG':
			peer = args[1]
			self.ping_fail[peer] = 0
		else:
			print 'unknown message:', ' '.join(args)

	def handle_found(self, hash, peer, transid):
		if transid not in self.trans:
			print 'received message for bad trans %s: FOUND %s %s' % (transid, hash, peer)
			return

		t = self.trans[transid]
		if t.type == Trans.GET:
			s = self.connect(peer)
			s.write(['GET', hash, transid])
		elif t.type == Trans.PUT:
			s = self.connect(peer)
			s.write(['PUT', base64.b64encode(t.data), transid])
		elif t.type == Trans.FINGER:
			# don't add ourself to the finger table, we'll get used as fallback anyway
			if peer == self.myname:
				return
			# we found the finger node for a specific index
			if peer != self.finger[t.index]:
				print 'updating finger %d: %s -> %s' % (t.index, self.finger[t.index], peer)
			# if we just found a new successor, grab data from them that we're supposed to have
			if t.index == 0:
				s = self.connect(peer)
				# cute trick: we don't need to know our predecessor, we just ask for
				# everything but the space between us and our successor!
				s.write(['RETR', make_id(peer), make_id(self.myname)])
			self.finger[t.index] = peer
			t.remove()
		elif t.type == Trans.BACKUP:
			# update our successor's successor (for fault tolerance)
			if self.succsucc != peer:
				print 'updating succsucc to %s' % peer
			self.succsucc = peer
			t.remove()
		elif t.type == Trans.PRUNE:
			# if we're no longer responsible for this, remove it
			if peer != self.myname and hash in items:
				print 'pruning %s' % hash
				del self.items[hash]
			t.remove()

	# called when data received from client (TCP) port
	def on_data(self, socket, data):
		pos = data.find('\n')
		if pos < 0: # need to wait for new line
			return 0
		elif pos == 0:
			return 1 # just a keep-alive

		args = data[0:pos].split(' ')
		if self.DEBUG: print 'inT:', ' '.join(args)

		# client->server commands:
		# CGET hash -- get value with specified hash
		# CPUT data -- put data into hash table (base64-encoded)
		# CSHOW -- request a listing of all nodes
		# server->client commands:
		# CERROR msg -- there was some kind of error
		# CDATA data -- data that was stored (base64-encoded)
		# COK hash -- insert succeeded
		# CPEER hash ip:port -- peer in system
		if args[0] == 'CGET':
			hash = args[1]
			t = Trans(Trans.GET, self, socket)
			t.add()
			self.find(hash, t.id)
		elif args[0] == 'CPUT':
			to_add = base64.b64decode(args[1])
			hash = make_file_id(to_add)
			t = Trans(Trans.PUT, self, socket, to_add)
			t.add()
			self.find(hash, t.id)
		elif args[0] == 'CSHOW':
			t = Trans(Trans.SHOW, self, socket)
			t.add()
			if self.finger[0] != self.myname:
				# we can have trouble here if a node just joined and our successor
				# has not been updated to reflect that
				self.dgram_socket.send(self.finger[0], ['SHOW', self.myname, t.id])
			socket.write(['CPEER', make_id(self.myname), self.myname])
		# get/put operations done over TCP because data could be larger than 1 packet
		# GET hash transid -- request for data
		# DATA data transid -- hash and its data (sent in response to GET)
		# ERROR msg transid -- there was an error
		# PUT data transid -- data to insert (hash calculated at inserting node)
		# OK hash transid -- insert succeeded
		elif args[0] == 'GET':
			(hash, transid) = args[1:]
			if hash in self.items:
				socket.write(['DATA', base64.b64encode(self.items[hash]), transid])
			else:
				socket.write(['ERROR', 'data.not.found', transid])
			socket.close_when_done()
		elif args[0] == 'DATA':
			(data, transid) = args[1:]
			t = self.trans[transid]
			t.client.write(['CDATA', data])
			t.client.close_when_done()
			t.remove()
		elif args[0] == 'ERROR':
			(msg, transid) = args[1:]
			t = self.trans[transid]
			t.client.write(['CERROR', msg])
			t.client.close_when_done()
			t.remove()
		elif args[0] == 'PUT':
			(data, transid) = args[1:]
			data = base64.b64decode(data)
			hash = make_file_id(data)
			print 'adding %s' % hash
			self.items[hash] = data
			socket.write(['OK', hash, transid])
			socket.close_when_done()
		elif args[0] == 'OK':
			(hash, transid) = args[1:]
			t = self.trans[transid]
			t.client.write(['COK', hash])
			t.client.close_when_done()
			t.remove()
		# value transfers are done over TCP as well
		# RETR low high -- ask for data in range (low, high]
		# XFER hash data -- response to RETR (transferring data to new node)
		elif args[0] == 'RETR':
			(low, high) = args[1:]
			for i in self.items.iterkeys():
				if id_distance(low, i) < id_distance(low, high):
					print 'transferring %s to peer' % i
					socket.write(['XFER', i, base64.b64encode(self.items[i])])
			socket.close_when_done()
		elif args[0] == 'XFER':
			(hash, data) = args[1:]
			# add to database
			self.items[hash] = base64.b64decode(data)
		else:
			print 'unknown message:', ' '.join(args)

		return pos + 1

	def find(self, hash, transid):
		self.find_forward(hash, self.myname, transid)

	# peerid is looking for hash with transaction transid
	def find_forward(self, hash, peerid, transid):
		my_id = make_id(self.myname)
		for i in reversed(self.finger):
			if not i:
				continue
			other_id = make_id(i)
			if id_distance(my_id, hash) > id_distance(my_id, other_id):
				self.dgram_socket.send(i, ['FIND', hash, peerid, transid])
				return
		# if we can't find a node less than the key,
		# then our successor (or just us) must be its owner
		if self.finger[0]:
			self.dgram_socket.send(peerid, ['FOUND', hash, self.finger[0], transid])
		else:
			self.dgram_socket.send(peerid, ['FOUND', hash, self.myname, transid])

	def connect(self, peername):
		sock = socket.socket()
		s = StreamSocket(sock, self)
		(host, port) = peername.split(':')
		s.connect(host, port)
		return s

	#
	# timers
	#
	def reschedule(self, name, func, time):
		self.timers[name] = Timer(10, func)
		self.timers[name].add()

	def ping_timer_cb(self):
		# reschedule
		self.reschedule('ping', self.ping_timer_cb, 10)

		# look for dead nodes
		# prev
		if self.prev and self.prev in self.ping_fail and self.ping_fail[self.prev] >= 2:
			print 'prev node %s failed, setting to none' % self.prev
			self.prev = None
		# finger[0]
		if self.finger[0] and self.finger[0] in self.ping_fail and self.ping_fail[self.finger[0]] >= 2:
			print 'finger[0] (%s) died, updating with succsucc' % self.finger[0]
			self.finger[0] = self.succsucc
			self.succsucc = None
		# fingers
		for i in range(1, len(self.finger)):
			if self.finger[i] and self.finger[i] in self.ping_fail and self.ping_fail[self.finger[i]] >= 2:
				print 'finger %d (%s) failed, setting to none' % (i, self.finger[i])
				self.finger[i] = None
		# don't need to check deadness of succsucc; it gets refreshed automatically

		# send heartbeat (dead nodes detected by timeout)
		new_ping_fail = {}

		for x in (self.finger + [self.prev]):
			if not x:
				continue # if x is none, just ignore
			elif x in new_ping_fail:
				pass # peer is in multiple finger slots; only ping once
			else:
				new_ping_fail[x] = self.ping_fail.get(x, 0) + 1
				self.dgram_socket.send(x, ['PING', self.myname])

		self.ping_fail = new_ping_fail

	# periodically refresh succsucc
	def backup_timer_cb(self):
		self.reschedule('backup', self.backup_timer_cb, 10)

		if not self.finger[0] and self.succsucc:
			print 'succ is null, copying succsucc %s to finger[0]' % self.succsucc
			self.finger[0] = self.succsucc
			self.succsucc = None
		elif not self.finger[0] and not self.succsucc:
			print 'lost successor and successor\'s successor! dying...'
			sys.exit(1)

		# query for succsucc again
		t = Trans(Trans.BACKUP, self)
		t.add()
		self.find(add_to_id(make_id(self.finger[0]), 1), t.id)

	def finger_timer_cb(self):
		self.reschedule('finger', self.finger_timer_cb, 15)

		# pick a random finger index and update it
		# only use top 8 bits of hash; we don't have anywhere close to 2^160 nodes
		index = random.randrange(len(self.finger)-8, len(self.finger))
		t = Trans(Trans.FINGER, self, index)
		t.add()
		self.find(add_to_id(make_id(self.myname), 2 ** index), t.id)

	def stabilize_timer_cb(self):
		self.reschedule('stabilize', self.stabilize_timer_cb, 10)

		self.dgram_socket.send(self.finger[0], ['GETP', self.myname])

		# according to paper, we should wait to notify until after we update
		# our successor from succ.prev, but we may not get a reply if they have
		# no previous node set. therefore, notify unconditionally
		self.dgram_socket.send(self.finger[0], ['NOTIFY', self.myname])

	# NOTE: this causes problems because things can get pruned from the correct
	# node while things are still converging on stable state. to use, uncomment
	# the line that adds the timer in the start method
	def prune_timer_cb(self):
		self.reschedule('prune', self.prune_timer_cb, 20)

		# we periodically search for one of our files,
		# to make sure we're still in charge of it
		if len(self.items) == 0:
			return # no item to check

		t = Trans(Trans.PRUNE, self)
		t.add()
		file = random.choice(self.items.keys())
		self.find(file, t.id)

# utility functions (these are all pure functions, so we make them freestanding)
def make_id(addr):
	h = hashlib.sha1()
	h.update('\x00')
	h.update(addr)
	return h.hexdigest()
def make_file_id(addr): # make file ids differently to avoid collisions
	h = hashlib.sha1()
	h.update('\x01')
	h.update(addr)
	return h.hexdigest()

# returns hash distance from id1 to id2
def id_distance(id1, id2):
	if id1 == id2:
		# special case: if we're the only node, we need to have the distance
		# to our successor be maximum for forwarding to work correctly;
		# so if they're the same, the distance is 2^160
		return 2 ** 160
	l1 = long(id1, 16)
	l2 = long(id2, 16)
	if l1 > l2:
		return 2 ** 160 + l2 - l1
	else:
		return l2 - l1

def add_to_id(id, n):
	n = long(n)
	id = long(id, 16)
	sum = (id + n) & (2**160 - 1)
	return '%040x' % (id + n)

if __name__ == '__main__':
	if len(sys.argv) != 3:
		sys.exit("Usage: %s myhost server" % sys.argv[0])
	host = sys.argv[1]
	server = sys.argv[2]
	m = Manager(Main(), host, server)
	m.run()
