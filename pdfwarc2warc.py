#!/usr/bin/env python3
import os
import sys
import argparse
import traceback
import threading
import ctypes
import json
from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders
from shutil import copyfileobj
from tempfile import NamedTemporaryFile
from io import BytesIO
from contextlib import contextmanager, redirect_stdout
from pd3f.export import run_parsr, Export
from queue import SimpleQueue


def current_thread_id():
	current_thread = threading.current_thread()

	if hasattr(current_thread, '_thread_id'):
		return current_thread._thread_id

	for thread_id, thread in threading._active.items():
		if thread is current_thread:
			return thread_id

	raise RuntimeError('Could not get thread id')


class TimeoutException(Exception):
	"""Raised by Timeout inside the thread that timed out"""
	pass


class Timeout:
	"""Context that can be used inside a thread to raise a TimeoutException after
	a certain number of seconds.

	Usage (inside a thread!):
	  try:
	    with Timeout(10):
	      do_blocking_thing()
	  except TimeoutException:
	    print("Timeout")
	"""

	def __init__(self, timeout: int):
		self.thread_id = current_thread_id()
		self.timer = threading.Timer(timeout, self.trigger)

	def __enter__(self):
		self.start()
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		self.cancel()

	def __del__(self):
		self.timer.cancel()

	def start(self):
		self.timer.start()

	def cancel(self):
		self.timer.cancel()

	def trigger(self):
		res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(self.thread_id), ctypes.py_object(TimeoutException))
		if res == 0:
			raise Valuerror('Invalid thread id')
		elif res != 1:
			ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(self.thread_id), None)
			raise SystemError('Exception raise failure')


class Task:
	def __init__(self, record):
		self.record = record
		self.tempfile = NamedTemporaryFile().__enter__()
		copyfileobj(record.content_stream(), self.tempfile)
		self.tempfile.flush()

	def __del__(self):
		self.tempfile.__exit__(None, None, None)


class Stats:
	read = 0
	written = 0

	def __str__(self):
		return f'{self.read} records read, {self.written} written'


class ParsrFilter:
	def __init__(self, fh):
		self.fh = fh

	def write(self, data):
		written = 0
		for line in data.split('\n'):
			if line.startswith('> ') or line.startswith('>> '):
				continue
			elif len(line) > 0:
				written += self.fh.write(line + '\n')
		return written

	def flush(self):
		self.fh.flush()



def read(options, stats, queue):
	for fh in options.warcs:
		for n, record in enumerate(ArchiveIterator(fh)):
			if record.rec_type != 'response' and record.rec_type != 'resource':
				continue
			queue.put(Task(record))
			stats.read += 1


def process(options, in_queue, out_queue):
	while True:
		task = in_queue.get()
		if task is None:
			break

		input_json = None # reset it so it does show the previous json when run_parsr errors out

		try:
			with Timeout(options.timeout):
				# Oh crap extract calls parsr_client which pollutes stdout (not stderr!)
				# with progress info without any way to disable that functionality.
				# See https://github.com/axa-group/Parsr/blob/develop/clients/python-client/parsr_client/parsr_client.py#L138
				# Also, why do they call parsr.send_document with silent=False :(
				# Here: https://github.com/jelmervdl/pd3f-core/blob/master/pd3f/parsr_wrapper.py#L80
				input_json, _ = run_parsr(
					task.tempfile.name,
					check_tables=False,
					parsr_location=options.parsr_location,
					fast=options.fast,
					config={},
					adjust_cleaner_config=[])

			# Common error: empty document
			if len(input_json.get('pages', [])) == 0:
				continue

			# Common error: document without text
			if len(input_json.get('fonts', [])) == 0:
				continue

			export = Export(input_json,
				seperate_header_footer=True,
				footnotes_last=True,
				remove_page_number=True,
				lang='multi',
				fast=options.fast)

			task.record.raw_stream = BytesIO(export.text().encode())
			task.record.length = None
			out_queue.put(task.record)
		except Exception as e:
			print(f"Error while processing record {task.record.rec_headers.get_header('WARC-Record-ID')} ({task.record.rec_headers.get_header('WARC-Target-URI')}):", file=sys.stderr)
			traceback.print_exc(file=sys.stderr)
			
			if options.dump_errors:
				basename = os.path.join(options.dump_errors, task.record.rec_headers.get_header('WARC-Record-ID'))

				with open(f'{basename}.pdf', 'wb') as fh:
					task.tempfile.seek(0)
					copyfileobj(task.tempfile, fh)

				with open(f'{basename}.log', 'w') as fh:
					print(f"Error while processing record {task.record.rec_headers.get_header('WARC-Record-ID')} ({task.record.rec_headers.get_header('WARC-Target-URI')}):", file=fh)
					traceback.print_exc(file=fh)

				if input_json:
					with open(f'{basename}.json', 'w') as fh:
						json.dump(input_json, fh, indent=2)

			if options.pedantic:
				sys.stderr.flush()
				os._exit(1) # TODO Rather aggressive, but I don't have a better alternative right now


def write(options, stats, queue):
	writer = WARCWriter(options.output, gzip=True)
	while True:
		record = queue.get()
		if not record:
			break
		writer.write_record(record)
		stats.written += 1
		

def main(argv):
	parser = argparse.ArgumentParser(description='Converts a warc with pdfs into a warc with the contents of these pdfs.')
	parser.add_argument('warcs', nargs='+', type=argparse.FileType('rb'), help='one or more warcs with pdfs')
	parser.add_argument('--threads', '-j', type=int, default=8, help='number of workers (default: 8)')
	parser.add_argument('--dump-errors', type=str, help='dump pdfs causing errors into this directory')
	parser.add_argument('--pedantic', action='store_true', help='stop at the first sign of trouble')
	parser.add_argument('--fast', action='store_true', help='skip certain steps in parsr')
	parser.add_argument('--parsr-location', type=str, default='localhost:3001', help='host:port of Parsr api (default: localhost:3001)')
	parser.add_argument('--timeout', type=int, default=300, help='time limit in seconds for processing per pdf (default 300)')
	parser.add_argument('--output', '-o', type=argparse.FileType('wb'), default=sys.stdout.buffer)
	
	options = parser.parse_args(argv[1:])
	stats = Stats()
	
	in_queue = SimpleQueue()
	out_queue = SimpleQueue()

	writer = threading.Thread(target=write, args=(options, stats, out_queue))
	writer.start()

	workers = [threading.Thread(target=process, args=(options, in_queue, out_queue)) for worker in range(options.threads)]
	# For the duration of workers doing things, redirect stdout to stderr because
	# of pd3f_extract. Would like to do this inside the worker itself, but python
	# and threading ... redirect would not be thread local!
	with redirect_stdout(ParsrFilter(sys.stderr)):
		for worker in workers:
			worker.start()

		# Read all input (could be doing this in a thread, but why...)
		read(options, stats, in_queue)

		# Tell workers their shift is over
		for _ in workers:
			in_queue.put(None)
		# Wait for workers to finish
		for worker in workers:
			worker.join()

	out_queue.put(None)
	writer.join()

	print(stats, file=sys.stderr)

if __name__ == '__main__':
	main(sys.argv)