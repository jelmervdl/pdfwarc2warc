# pdfwarc2warc
Convert a warc filled with pdf records to a warc with plain text records.

# Installation
This project only depends on warcio and pd3f (currently on our own branch with a couple of bugfixes).

```sh
pip3 install -r requirements.txt
```

# Usage
```sh
docker-compose up -d
./pdfwarc2warc.py pdf.warc.gz > text.warc.gz
```

Make sure [Parsr](https://github.com/axa-group/Parsr) is running and accessible through http (default localhost:3001 but changeable through the `--parsr-location` option.).

The main thread reads records into a queue, which workers (controllable through `-j` or `--threads`) pick records from. Each worker then grabs the pdf from the record, submits it to *parsr*, processes the response with *pd3f*, and puts the text back into the record payload. The record is then put on another queue to be written to stdout (or whatever file is specified with `--output`.)

# Error handling
A lot of things can go wrong when processing PDF files. Generally, records that produce errors are skipped over, unless an error is unrecoverable (e.g. parsr cannot be contacted, or a filesystem error).

To aid debugging the PDF processing (mainly done by pd3f) there is a `--dump-errors path/to/folder` option that dumps the exception traceback, the pdf, and any response from parsr (if there was any) for unknown errors to a folder, keyed by the warc record id.

Some errors are already pro-actively caught before we pass on the parsr output to pd3f, mainly around empty documents, or documents without any text.
