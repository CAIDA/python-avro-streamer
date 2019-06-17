# Avro Streamer python module

A python module designed to support development of custom code for
in-line modification of streamed Avro data, such as when an Avro
data file is being downloaded via HTTP.

Existing Avro parsing libraries for Python tend to expect the entire
Avro file to be available immediately and will throw an exception when
there is no more data available, especially if they are part-way through
decoding a record. The Avro streamer module anticipates this and will
simply keep trying to read until it gets a complete data unit.

At this stage, the streamer module has been written primarily to support
the removal of certain fields from Avro records but I anticipate that it
should be not too difficult to add in some hooks to adding new fields if
that was something that someone required.


## Installing

The module requires the python-snappy and future packages. These should be
installed automatically by the setup.py script.

```
git clone git@github.com:caida/python-avro-streamer
cd python-avro-streamer/
python setup.py install
```

## Usage

At the time of writing, the streamer module provides two classes:
`GenericStreamingAvroParser` and `GenericStrippingAvroParser`.

You can import both classes into your own code using:

```
from avro_streamer.avro_streamer import GenericStreamingAvroParser, GenericStrippingAvroParser
```

The `GenericStreamingAvroParser` class is intended as an abstract class that
you could use as a base class for your own code. If used directly without
modification or overriding any methods, this class will simply read Avro
from a source generator (which is the first argument passed in when creating
an instance of this class) and then return it back unmodified through its
own `next` method.

The `GenericStrippingAvroParser` class is an example of how you can sub-class
`GenericStreamingAvroParser` to read, modify and then return an Avro data
stream. The stripping parser takes an additional argument, which is a list
of names of fields that should be removed from the Avro data stream. If all
you want to do is remove some fields from streamed Avro data records, this
class is probably enough on its own.
