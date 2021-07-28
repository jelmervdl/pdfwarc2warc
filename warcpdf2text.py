#!/usr/bin/env python3
import sys
import argparse
import traceback
from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders
from shutil import copyfileobj
from tempfile import NamedTemporaryFile
from io import BytesIO
from contextlib import contextmanager, redirect_stdout
from pd3f.export import extract
from queue import SimpleQueue
from threading import Thread


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



def read(options, stats, queue):
	for fh in options.warcs:
		for n, record in enumerate(ArchiveIterator(fh)):
			if record.rec_type != 'response' and record.rec_type != 'resource':
				continue
			queue.put(Task(record))
			stats.read += 1


def process(in_queue, out_queue):
	while True:
		task = in_queue.get()
		if task is None:
			break
		try:
			# Oh crap extract calls parsr_client which pollutes stdout (not stderr!)
			# with progress info without any way to disable that functionality.
			# See https://github.com/axa-group/Parsr/blob/develop/clients/python-client/parsr_client/parsr_client.py#L138
			# Also, why do they call parsr.send_document with silent=False :(
			# Here: https://github.com/jelmervdl/pd3f-core/blob/master/pd3f/parsr_wrapper.py#L80
			text, tables = extract(task.tempfile.name, experimental=True)
			task.record.raw_stream = BytesIO(text.encode())
			task.record.length = None
			out_queue.put(task.record)
		except Exception as e:
			print(f"Error while processing record {task.record.rec_headers.get_header('WARC-Target-URI')}:", file=sys.stderr)
			traceback.print_exc(file=sys.stderr)


def write(options, stats, queue):
	writer = WARCWriter(options.output, gzip=True)
	while True:
		record = queue.get()
		if not record:
			break
		writer.write_record(record)
		stats.written += 1
		

def main(argv):
	parser = argparse.ArgumentParser()
	parser.add_argument('warcs', nargs='+', type=argparse.FileType('rb'))
	parser.add_argument('--output', '-o', type=argparse.FileType('wb'), default=sys.stdout.buffer)
	
	options = parser.parse_args(argv[1:])
	stats = Stats()
	
	in_queue = SimpleQueue()
	out_queue = SimpleQueue()

	writer = Thread(target=write, args=(options, stats, out_queue))
	writer.start()

	workers = [Thread(target=process, args=(in_queue, out_queue)) for worker in range(8)]
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