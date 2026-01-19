# ``Arrow``

Apache Arrow: Columnar format for fast data interchange and in-memory analytics.

## Overview

Arrow provides a universal columnar data format optimized for efficient analytic operations. This Swift implementation enables you to:

- Read and write Arrow IPC file and streaming formats
- Build columnar data structures with typed arrays
- Work with record batches and tables
- Encode and decode Swift types to Arrow format
- Interoperate with other Arrow implementations via the C Data Interface

## Topics

### Reading and Writing Data

- ``ArrowReader``
- ``ArrowWriter``

### Core Data Structures

- ``ArrowTable``
- ``RecordBatch``
- ``ArrowColumn``
- ``ChunkedArray``

### Arrays

- ``ArrowArray``
- ``ArrowArrayBuilder``
- ``ArrowArrayHolder``

### Schema and Types

- ``ArrowSchema``
- ``ArrowField``
- ``ArrowType``

### Encoding and Decoding

- ``ArrowEncoder``
- ``ArrowDecoder``

### C Data Interface

- ``ArrowCExporter``
- ``ArrowCImporter``
