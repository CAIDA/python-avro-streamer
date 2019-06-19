# Copyright 2019 The Regents of the University of California.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# Original version written by Shane Alcock <salcock@waikato.ac.nz>
#
# Major TODOs:
#   * support for deflate codec
#   * support additional data types (float etc)
#   * make other operations easier to perform, e.g. adding new fields

import json, snappy, binascii

from struct import pack

AVRO_HEADER_MAGIC = 1
AVRO_HEADER_FILE_METADATA = 2
AVRO_SYNC_MARKER = 3
AVRO_START_DATABLOCK = 4

class AvroInsufficientDataException(Exception):
    pass

class AvroParsingFailureException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class GenericStreamingAvroParser(object):
    """
    A generator class that can read Avro from another generator (say,
    a file object or an HTTP response iterable) and parse the contents
    of the Avro data. Intended for use as a base class to write code
    that modifies the streamed Avro in some way. Example applications
    might be to insert or remove fields from the Avro records.

    After the Avro content has been parsed, it will then be re-encoded
    and yielded to the user just as if they had used the original
    generator directly.

    Requires: python-snappy, future (python2)
    """

    def __init__(self, source, initbody):
        """
        source is the generator that will be providing the raw Avro data
        stream (e.g. a file object or some other sort of iterable).

        initbody is any initial data that may have already been read by
        the source generator (e.g. some HTTP response generators will
        include the first 'read' in a body field and the associated generator
        is solely used for reading additional data beyond that first blob.
        """
        self.source = source
        self.buffered = ""
        self.blen = 0
        self.bused = 0
        self.schema = {}
        self.codec = None
        self.state = AVRO_HEADER_MAGIC
        self.syncmarker = None

        self.buffered += initbody
        self.blen += len(initbody)

    def _encode_long(self, toenc):
        encoded = ""

        toenc = (toenc << 1) ^ (toenc >> 63)
        while (toenc & ~0x7F) != 0:
            encoded += pack('B', (toenc & 0x7f) | 0x80)
            toenc >>= 7
        encoded += pack('B', toenc)
        return encoded

    def _read_long(self, buffered, blen):
        if blen == 0:
            raise AvroInsufficientDataException()

        c = buffered[0]
        ind = 1
        b = ord(c)
        n = b & 0x7F
        shift = 7

        while (b & 0x80) != 0:
            if blen <= ind:
                raise AvroInsufficientDataException()
            b = ord(buffered[ind])
            ind += 1
            n |= (b & 0x7F) << shift
            shift += 7

        return (n >> 1) ^ -(n & 1), ind

    def _read_bytes(self, buffered, blen):

        slen, bused = self._read_long(buffered, blen)

        if blen < slen + bused:
            raise AvroInsufficientDataException()

        readstr = buffered[bused:bused+slen]

        return readstr, slen + bused

    def _read_avro_codec(self):
        codec, used = self._read_bytes(self.buffered[self.bused:],
                self.blen - self.bused)

        self.codec = codec

        self.saved += self.buffered[self.bused:self.bused + used]
        self.bused += used

    def _parse_schema_fields(self, fields):
        """
        If you wish to modify the set of fields present in the schema,
        override this function.
        """
        return fields

    def _read_avro_schema(self):
        fullschema, used = self._read_bytes(self.buffered[self.bused:],
                self.blen - self.bused)

        self.bused += used

        jsonschema = json.loads(fullschema)

        # Save the original schema so that we can interpret the
        # upcoming records
        self.schema = dict(jsonschema)

        if 'fields' in jsonschema:
            newfields = self._parse_schema_fields(jsonschema['fields'])
            jsonschema['fields'] = newfields

        newschema = json.dumps(jsonschema)
        self.saved += self._encode_long(len(newschema))
        self.saved += newschema


    def _parse_header_magic(self):
        self.bused = 0

        if self.blen - self.bused < 4:
            raise AvroInsufficientDataException()

        if self.buffered[self.bused:4 + self.bused] != b"Obj\x01":
            raise AvroParsingFailureException("Invalid Header Magic")

        self.state = AVRO_HEADER_FILE_METADATA
        res = self.buffered[self.bused:self.bused + 4]
        self.buffered = self.buffered[self.bused + 4:]
        self.blen -= (self.bused + 4)
        return res

    def _parse_file_metadata(self):

        self.saved = ""
        self.bused = 0
        block_count, used = self._read_long(self.buffered, self.blen)

        self.saved += self.buffered[:used]
        self.bused += used

        while block_count != 0:
            for i in xrange(block_count):
                key, used = self._read_bytes(self.buffered[self.bused:],
                        self.blen - self.bused)
                self.saved += self.buffered[self.bused:self.bused + used]
                self.bused += used

                if key == "avro.codec":
                    self._read_avro_codec()
                elif key == "avro.schema":
                    self._read_avro_schema()
                else:
                    throwaway, used = self._read_bytes( \
                            self.buffered[self.bused:], \
                            self.blen - self.bused)
                    self.saved += self.buffered[self.bused:self.bused + used]
                    self.bused += used

            block_count, used = self._read_long(self.buffered[self.bused:],
                        self.blen - self.bused)
            self.saved += self.buffered[self.bused:self.bused + used]
            self.bused += used

        self.buffered = self.buffered[self.bused:]
        self.blen -= self.bused
        self.state = AVRO_SYNC_MARKER

        return self.saved

    def _parse_sync_marker(self):
        if self.blen < 16:
            raise AvroInsufficientDataException()

        if self.syncmarker is None:
            self.syncmarker = self.buffered[:16]
        elif self.buffered[:16] != self.syncmarker:
            raise AvroParsingFailureException("Sync Marker does not match marker in header")

        self.buffered = self.buffered[16:]
        self.blen -= 16
        self.state = AVRO_START_DATABLOCK
        return self.syncmarker

    def _reencode_field(self, val, ftype, fname):
        """
        If you wish to modify or remove certain fields within an Avro
        record, override this function.
        """
        encoded = ""

        if ftype == "long" or ftype == "int":
            encoded += self._encode_long(val)
        else:
            encoded += self._encode_long(len(val))
            encoded += val

        return encoded

    def _parse_single_record(self, source):
        encoded = ""
        skip = 0
        rem = len(source)

        for field in self.schema['fields']:
            t = field['type']['type']

            if t == 'long' or t == 'int':
                val, used = self._read_long(source[skip:], rem - skip)

                skip += used
            elif t == "string":
                val, used = self._read_bytes(source[skip:], rem - skip)
                skip += used
            else:
                # TODO implement other types here!
                raise AvroParsingFailureException("Unsupported data type in schema %s" % (t))

            encoded += self._reencode_field(val, t, field['name'])
        return encoded, skip

    def _parse_data_block(self):
        self.saved = ""
        self.bused = 0
        reencoded = ""

        obj_count, used = self._read_long(self.buffered, self.blen)
        self.saved += self.buffered[:used]
        self.bused += used

        block_size, used = self._read_long(self.buffered[self.bused:],
                self.blen - self.bused)
        self.bused += used

        if self.blen < self.bused + block_size + 16:
            raise AvroInsufficientDataException()

        if self.codec == "snappy":

            inflated = snappy.decompress(\
                    self.buffered[self.bused: self.bused + block_size - 4])
            self.bused += block_size

        elif self.codec == "deflate":
            # TODO
            raise AvroParsingFailureException(\
                    "Unsupported codec: %s" % (self.codec))
        else:
            raise AvroParsingFailureException(\
                    "Unknown codec: %s" % (self.codec))
        infused = 0
        while obj_count > 0:
            rec, skip = self._parse_single_record(inflated[infused:])
            reencoded += rec
            infused += skip

            obj_count -= 1;

        if self.codec == "snappy":
            compressed = snappy.compress(reencoded)
            complen = len(compressed) + 4
        elif self.codec == "deflate":
            # TODO
            compressed = ""
            complen = 0
        else:
            compressed = ""
            complen = 0

        self.saved += self._encode_long(complen)
        self.saved += compressed

        csum = binascii.crc32(reencoded) & 0xffffffff
        self.saved += pack("I", csum)

        self.buffered = self.buffered[self.bused:]
        self.blen -= self.bused

        self.saved += self._parse_sync_marker()
        return self.saved



    def _parse_next_item(self):

        if self.state == AVRO_HEADER_MAGIC:
            return self._parse_header_magic()
        elif self.state == AVRO_HEADER_FILE_METADATA:
            return self._parse_file_metadata()
        elif self.state == AVRO_SYNC_MARKER:
            return self._parse_sync_marker()
        elif self.state == AVRO_START_DATABLOCK:
            return self._parse_data_block()

        raise AvroParsingFailureException(\
                "Generic avro parser has reached an unexpected state? (%s)",\
                self.state)

    def next(self):

        try:
            return self._parse_next_item()
        except AvroInsufficientDataException as e:
            pass
        except:
            raise

        try:
            x = next(self.source)
            self.buffered += x
            self.blen += len(x)
        except StopIteration:
            raise

        return self.next()

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()


class GenericStrippingAvroParser(GenericStreamingAvroParser):
    """
    Extension of the GenericStreamingAvroParser that will automatically
    remove a pre-defined set of fields from each Avro record, as well as
    update the schema accordingly.
    """

    def __init__(self, source, initbody, tostrip):
        """
        tostrip should be a list of field names that are to be removed
        from the streamed Avro data.
        """
        super(GenericStrippingAvroParser, self).__init__(source, initbody)

        self.tostrip = set(tostrip)

    def _reencode_field(self, val, ftype, fname):

        if fname in self.tostrip:
            return ""
        return super(GenericStrippingAvroParser, self)._reencode_field(val, \
                ftype, fname)


    def _parse_schema_fields(self, fields):
        newfields = []

        for f in fields:
            if f['name'] in self.tostrip:
                continue
            newfields.append(f)
        return newfields

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :


