import base64
import hashlib
import random
import socket
import sys
from mynet import Event, StreamSocket, DgramSocket, Timer, ListenSocket

myname = '' # host:port of this node; DHT id is the sha-1 of this string
prev = None # previous peer in DHT
succsucc = None # successor of my successor; in case successor fails
finger = [None] * 160 # list of finger connections, in order of distance; finger[0] is next node
ping_fail = {} # number of consecutive ping rounds a peer has not responded
               # after 2 rounds, we consider it dead

items = {} # items stored at this node

DEBUG = False

class Trans:
	# transaction types (i.e., reasons for making DHT requests)
	FINGER = 0 # finding finger peers for us (index = finger table index)
	BACKUP = 1 # finding redundant successors in case real one dies
	PRUNE = 2 # are we still in charge of this file? if not, drop it
	GET = 10 # finding place for client to get data (client = client socket)
	PUT = 11 # finding place for client to put data (client = client socket, data = file data))
	SHOW = 12 # transaction is asking for peer list (client = client socket, timer = 30sec timer)

	active = {} # list of active transactions: {id: Trans}
	next = 0 # incrementing count of transactions

	def __init__(self, type, arg1=None, arg2=None):
		self.type = type

		def make_trans():
			num = Trans.next
			Trans.next += 1
			return '%s-%d' % (myname, num)
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
		Trans.active[self.id] = self

	def remove(self):
		if self.type == Trans.SHOW:
			self.timer.remove()
		del Trans.active[self.id]

def error_cb(socket):
	# client disconnected.... whatever, just remove its transactions
	for i in Trans.active.values():
		def is_client_type(t):
			return t == Trans.GET or t == Trans.PUT or t == Trans.SHOW
		if is_client_type(i.type) and i.client == socket:
			print '%s disconnected, purging transaction %s' % (socket, i.id)
			t.remove()

# called when data received from server (UDP) port
def packet_cb(data):
	global prev
	args = data.split(' ')
	if DEBUG: print 'inU: %s\n' % ' '.join(args)

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
		find_forward(hash, peer, transid)
	elif args[0] == 'FOUND':
		(hash, peer, transid) = args[1:]
		handle_found(hash, peer, transid)
	elif args[0] == 'GETP':
		peer = args[1]
		if prev:
			dgram_socket.send(peer, ['PRED', prev])
	elif args[0] == 'NOTIFY':
		peer = args[1]
		my_id = make_id(myname)
		prev_id = prev and make_id(prev) or None
		peer_id = make_id(peer)
		if not prev or id_distance(peer_id, my_id) < id_distance(prev_id, my_id):
			print 'updating prev:', peer
			prev = peer
	elif args[0] == 'PRED':
		peer = args[1]
		my_id = make_id(myname)
		succ_id = make_id(finger[0])
		peer_id = make_id(peer)
		if id_distance(my_id, peer_id) < id_distance(my_id, succ_id):
			print 'updating finger[0] from succ.pred:', peer
			finger[0] = peer
	elif args[0] == 'SHOW':
		(peer, transid) = args[1:]
		if peer == myname:
			# we started it, so stop forwarding
			pass
		else:
			dgram_socket.send(finger[0], args)
			dgram_socket.send(peer, ['PEER', myname, transid])
	elif args[0] == 'PEER':
		(peer, transid) = args[1:]
		t = Trans.active[transid]
		t.client.write(['CPEER', make_id(peer), peer])
	elif args[0] == 'PING':
		# reply to ping
		peer = args[1]
		dgram_socket.send(args[1], ['PONG', myname])
	elif args[0] == 'PONG':
		peer = args[1]
		ping_fail[peer] = 0
	else:
		print 'unknown message:', ' '.join(args)

def handle_found(hash, peer, transid):
	global succsucc

	if transid not in Trans.active:
		print 'received message for bad trans %s: FOUND %s %s' % (transid, hash, peer)
		return

	t = Trans.active[transid]
	if t.type == Trans.GET:
		s = connect(peer)
		s.write(['GET', hash, transid])
	elif t.type == Trans.PUT:
		s = connect(peer)
		s.write(['PUT', base64.b64encode(t.data), transid])
	elif t.type == Trans.FINGER:
		# don't add ourself to the finger table, we'll get used as fallback anyway
		if peer == myname:
			return
		# we found the finger node for a specific index
		if peer != finger[t.index]:
			print 'updating finger %d: %s -> %s' % (t.index, finger[t.index], peer)
		# if we just found a new successor, grab data from them that we're supposed to have
		if t.index == 0:
			s = connect(peer)
			# cute trick: we don't need to know our predecessor, we just ask for
			# everything but the space between us and our successor!
			s.write(['RETR', make_id(peer), make_id(myname)])
		finger[t.index] = peer
		t.remove()
	elif t.type == Trans.BACKUP:
		# update our successor's successor (for fault tolerance)
		if succsucc != peer:
			print 'updating succsucc to %s' % peer
		succsucc = peer
		t.remove()
	elif t.type == Trans.PRUNE:
		# if we're no longer responsible for this, remove it
		if peer != myname and hash in items:
			print 'pruning %s' % hash
			del items[hash]
		t.remove()


# called when data received from client (TCP) port
def data_cb(socket, data):
	pos = data.find('\n')
	if pos < 0: # need to wait for new line
		return 0
	elif pos == 0:
		return 1 # just a keep-alive

	args = data[0:pos].split(' ')
	if DEBUG: print 'inT:', ' '.join(args)

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
		t = Trans(Trans.GET, socket)
		t.add()
		find(hash, t.id)
	elif args[0] == 'CPUT':
		to_add = base64.b64decode(args[1])
		hash = make_file_id(to_add)
		t = Trans(Trans.PUT, socket, to_add)
		t.add()
		find(hash, t.id)
	elif args[0] == 'CSHOW':
		t = Trans(Trans.SHOW, socket)
		t.add()
		if finger[0] != myname:
			# we can have trouble here if a node just joined and our successor
			# has not been updated to reflect that
			dgram_socket.send(finger[0], ['SHOW', myname, t.id])
		socket.write(['CPEER', make_id(myname), myname])
	# get/put operations done over TCP because data could be larger than 1 packet
	# GET hash transid -- request for data
	# DATA data transid -- hash and its data (sent in response to GET)
	# ERROR msg transid -- there was an error
	# PUT data transid -- data to insert (hash calculated at inserting node)
	# OK hash transid -- insert succeeded
	elif args[0] == 'GET':
		(hash, transid) = args[1:]
		if hash in items:
			socket.write(['DATA', base64.b64encode(items[hash]), transid])
		else:
			socket.write(['ERROR', 'data.not.found', transid])
		socket.close_when_done()
	elif args[0] == 'DATA':
		(data, transid) = args[1:]
		t = Trans.active[transid]
		t.client.write(['CDATA', data])
		t.client.close_when_done()
		t.remove()
	elif args[0] == 'ERROR':
		(msg, transid) = args[1:]
		t = Trans.active[transid]
		t.client.write(['CERROR', msg])
		t.client.close_when_done()
		t.remove()
	elif args[0] == 'PUT':
		(data, transid) = args[1:]
		data = base64.b64decode(data)
		hash = make_file_id(data)
		print 'adding %s' % hash
		items[hash] = data
		socket.write(['OK', hash, transid])
		socket.close_when_done()
	elif args[0] == 'OK':
		(hash, transid) = args[1:]
		t = Trans.active[transid]
		t.client.write(['COK', hash])
		t.client.close_when_done()
		t.remove()
	# value transfers are done over TCP as well
	# RETR low high -- ask for data in range (low, high]
	# XFER hash data -- response to RETR (transferring data to new node)
	elif args[0] == 'RETR':
		(low, high) = args[1:]
		for i in items.iterkeys():
			if id_distance(low, i) < id_distance(low, high):
				print 'transferring %s to peer' % i
				socket.write(['XFER', i, base64.b64encode(items[i])])
		socket.close_when_done()
	elif args[0] == 'XFER':
		(hash, data) = args[1:]
		# add to database
		items[hash] = base64.b64decode(data)
	else:
		print 'unknown message:', ' '.join(args)

	return pos + 1

def connect(peername):
	sock = socket.socket()
	s = StreamSocket(sock, data_cb, error_cb)
	(host, port) = peername.split(':')
	s.connect(host, port)
	return s

#
# timers
#
def ping_timer_cb():
	Timer(10, ping_timer_cb).add() # reschedule
	global ping_fail # silly python
	global prev
	global succsucc

	# look for dead nodes
	# prev
	if prev and prev in ping_fail and ping_fail[prev] >= 2:
		print 'prev node %s failed, setting to none' % prev
		prev = None
	# finger[0]
	if finger[0] and finger[0] in ping_fail and ping_fail[finger[0]] >= 2:
		print 'finger[0] (%s) died, updating with succsucc' % finger[0]
		finger[0] = succsucc
		succsucc = None
	# fingers
	for i in range(1, len(finger)):
		if finger[i] and finger[i] in ping_fail and ping_fail[finger[i]] >= 2:
			print 'finger %d (%s) failed, setting to none' % (i, finger[i])
			finger[i] = None
	# don't need to check deadness of succsucc; it gets refreshed automatically

	# send heartbeat (dead nodes detected by timeout)
	new_ping_fail = {}

	for x in (finger + [prev]):
		if not x:
			continue # if x is none, just ignore
		elif x in new_ping_fail:
			pass # peer is in multiple finger slots; only ping once
		else:
			new_ping_fail[x] = ping_fail.get(x, 0) + 1
			dgram_socket.send(x, ['PING', myname])

	ping_fail = new_ping_fail

def backup_timer_cb():
	Timer(10, backup_timer_cb).add()
	# periodically refresh succsucc
	global succsucc

	if not finger[0] and succsucc:
		print 'succ is null, copying succsucc %s to finger[0]' % succsucc
		finger[0] = succsucc
		succsucc = None
	elif not finger[0] and not succsucc:
		print 'lost successor and successor\'s successor! dying...'
		sys.exit(1)

	# query for succsucc again
	t = Trans(Trans.BACKUP)
	t.add()
	find(add_to_id(make_id(finger[0]), 1), t.id)

def finger_timer_cb():
	Timer(15, finger_timer_cb).add()

	# pick a random finger index and update it
	# only use top 8 bits of hash; we don't have anywhere close to 2^160 nodes
	index = random.randrange(len(finger)-8, len(finger))
	t = Trans(Trans.FINGER, index)
	t.add()
	find(add_to_id(make_id(myname), 2 ** index), t.id)

def stabilize_timer_cb():
	Timer(10, stabilize_timer_cb).add()
	dgram_socket.send(finger[0], ['GETP', myname])

	# according to paper, we should wait to notify until after we update
	# our successor from succ.prev, but we may not get a reply if they have
	# no previous node set. therefore, notify unconditionally
	dgram_socket.send(finger[0], ['NOTIFY', myname])

# NOTE: this causes problems because things can get pruned from the correct
# node while things are still converging on stable state. to use, add to
# the list of timers in the __main__ part below
def prune_timer_cb():
	Timer(20, prune_timer_cb).add()
	# we periodically search for one of our files,
	# to make sure we're still in charge of it
	if len(items) == 0:
		return # no item to check

	t = Trans(Trans.PRUNE)
	t.add()
	file = random.choice(items.keys())
	find(file, t.id)

# utility functions
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

def find(hash, transid):
	find_forward(hash, myname, transid)

# peerid is looking for hash with transaction transid
def find_forward(hash, peerid, transid):
	my_id = make_id(myname)
	for i in reversed(finger):
		if not i:
			continue
		other_id = make_id(i)
		if id_distance(my_id, hash) > id_distance(my_id, other_id):
			dgram_socket.send(i, ['FIND', hash, peerid, transid])
			return
	# if we can't find a node less than the key,
	# then our successor (or just us) must be its owner
	if finger[0]:
		dgram_socket.send(peerid, ['FOUND', hash, finger[0], transid])
	else:
		dgram_socket.send(peerid, ['FOUND', hash, myname, transid])


if __name__ == '__main__':
	# first is our port, second is bootstrap host:port
	if len(sys.argv) != 3:
		sys.exit('Usage: python %s myhost:port bootstraphost:port' % sys.argv[0])

	myname = sys.argv[1]
	myport = int(myname.split(':')[1])
	bootpeer = sys.argv[2]

	# set up datagram socket
	dgram_socket = DgramSocket(myport, packet_cb)

	# set up server socket
	server_socket = ListenSocket(myport, data_cb, error_cb)

	for i in [ping_timer_cb, backup_timer_cb, finger_timer_cb, stabilize_timer_cb]:
		Timer(random.randrange(5, 10), i).add() # start timer

	# start bootstrap process by finding successor
	if bootpeer != 'none':
		t = Trans(Trans.FINGER, 0)
		t.add()
		dgram_socket.send(bootpeer, ['FIND', make_id(myname), myname, t.id]) # find our successor
	else:
		finger[0] = myname
		prev = myname

	print 'started', make_id(myname), myname

	Event.dispatch()
