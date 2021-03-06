# classes for providing event handling and socket buffering abstractions
# I've already written the same thing in C++ and Java for classes I already took;
# why isn't this part of the standard library??
import socket
import sys
from select import select
from time import time

DEBUG = False

class Event:
	active = {} # a dict from (fd, ev_type) to Event
	timers = [] # list of Timer objects
	READ = 1
	WRITE = 2

	@staticmethod
	def dispatch():
		while True:
			# exit if we have no events to process
			if len(Event.active) == 0 and len(Event.timers) == 0:
				break

			rlist = []
			wlist = []

			for (fd, ev) in Event.active.iterkeys():
				if ev == Event.READ:
					rlist.append(fd)
				elif ev == Event.WRITE:
					wlist.append(fd)

			currtime = time()

			if len(Event.timers) == 0:
				timeout = None
			elif Event.timers[0].timeout > currtime:
				timeout = Event.timers[0].timeout - currtime
			else:
				timeout = 0

			(rlist, wlist, _) = select(rlist, wlist, [], timeout)

			# process timers first
			while len(Event.timers) != 0 and Event.timers[0].timeout < time():
				t = Event.timers[0]
				Event.timers = Event.timers[1:]
				t.callback() # call callback

			for fd in rlist:
				if (fd, Event.READ) in Event.active:
					e = Event.active[(fd,Event.READ)]
					e.callback()
			for fd in wlist:
				if (fd, Event.WRITE) in Event.active:
					e = Event.active[(fd,Event.WRITE)]
					e.callback()

	@staticmethod
	def sort_timers():
		Event.timers.sort(cmp = lambda a,b: cmp(a.timeout, b.timeout))

	def __init__(self, fd, ev, callback):
		assert ev == Event.READ or ev == Event.WRITE
		self.fd = fd
		self.ev = ev
		self.callback = callback

	def enable(self):
		if (self.fd, self.ev) not in Event.active:
			Event.active[(self.fd, self.ev)] = self

	def disable(self):
		if (self.fd, self.ev) in Event.active:
			del Event.active[(self.fd,self.ev)]

# a TCP socket. calls the following methods on the client object:
# on_connect(sock) -- when socket is created
# on_error(sock) -- when socket is disconnected
# on_data(sock, data) -- when data arrives. return number of bytes consumed
class StreamSocket:
	def __init__(self, sock, client):
		self.client = client
		self.socket = sock
		self.socket.setblocking(0)

		# callbacks
		self.wev = Event(self.socket.fileno(), Event.WRITE, self.write_cb)
		self.rev = Event(self.socket.fileno(), Event.READ, self.read_cb)
		self.rev.enable()

		# set up buffers
		self.rbuf = ""
		self.wbuf = ""

		self.name = ""
		self.send_eof = False

		self.client.on_connect(self) # call callback

	def __str__(self):
		return '%x' % id(self)

	def connect(self, host, port):
		self.socket.connect_ex((host, int(port)))

	def read_cb(self):
		# ok, socket is ready for reading
		ret = self.socket.recv(4096)
		if len(ret) == 0: # disconnected
			self.client.on_error(self)
			self.close()
			return

		self.rbuf += ret

		def helper(self):
			ret = self.client.on_data(self, self.rbuf)
			self.rbuf = self.rbuf[ret:]
			return ret

		while helper(self) > 0:
			pass

	def write_cb(self):
		# socket ready for writing
		ret = self.socket.send(self.wbuf)
		self.wbuf = self.wbuf[ret:] # remove sent data
		if len(self.wbuf) == 0 and self.send_eof:
			self.close()
		elif len(self.wbuf) == 0:
			self.wev.disable()

	def write(self, data): # data is a list; things are joined by spaces
		self.write_raw(' '.join(data) + '\n')

	def write_raw(self, data):
		self.wbuf += data
		if DEBUG: print 'out %s: %s' % (self, data)
		self.wev.enable()

	def close_when_done(self):
		if len(self.wbuf) == 0:
			self.close()
		else:
			self.send_eof = True
			self.rev.disable()

	def close(self):
		self.wev.disable()
		self.rev.disable()
		self.socket.close()

# a listening TCP socket. doesn't do any callbacks, but
# the StreamSocket it creates does
class ListenSocket:
	def __init__(self, bindport, client):
		self.client = client
		self.socket = socket.socket()
		self.socket.setblocking(0)
		self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.socket.bind(('', bindport))
		self.socket.listen(5)

		self.ev = Event(self.socket.fileno(), Event.READ, self.accept_cb)
		self.ev.enable()

	def accept_cb(self):
		(ret, addr) = self.socket.accept()
		StreamSocket(ret, self.client) # will add itself and do the right thing

	def close(self):
		self.ev.disable()
		self.socket.close()

# a UDP socket. callback is:
# on_dgram(socket, data)
class DgramSocket:
	def __init__(self, bindport, client):
		self.client = client

		self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.socket.setblocking(0)
		self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		if bindport:
			self.socket.bind(('0.0.0.0', bindport))
			self.rev = Event(self.socket.fileno(), Event.READ, self.read_cb)
			self.rev.enable()
		else:
			self.rev = None

		self.wev = Event(self.socket.fileno(), Event.WRITE, self.write_cb)

		self.sendq = [] # list of (data, addr) tuples

	def read_cb(self):
		data = self.socket.recv(4096)
		self.client.on_dgram(self, data)

	def write_cb(self):
		(data, addr) = self.sendq[0]
		self.sendq = self.sendq[1:]
		self.socket.sendto(data, 0, addr)
		if len(self.sendq) == 0:
			self.wev.disable()

	def send(self, addr, data):
		if not addr:
			# peer doesn't exist, return
			return
		(host, port) = addr.split(':')
		self.send_raw((host, int(port)), ' '.join(data))

	def send_raw(self, addr, data):
		self.sendq += [(data, addr)]
		if DEBUG: print 'out %s: %s' % (addr, data)
		self.wev.enable()

	def close(self):
		if self.rev:
			self.rev.disable()
		self.wev.disable()
		self.socket.close()

class Timer:
	def __init__(self, t, callback):
		self.timeout = t + time()
		self.callback = callback

	def add(self):
		# if already added, do nothing
		if Event.timers.count(self) == 0:
			Event.timers.append(self)
		Event.sort_timers()

	def remove(self):
		# remove all instances (shouldn't be more than one, but...)
		while Event.timers.count(self) > 0:
			Event.timers.remove(self)
		Event.sort_timers()

	def __str__(self):
		return '%d:%s' % (self.timeout, self.callback)
