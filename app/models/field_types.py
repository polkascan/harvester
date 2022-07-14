#  Polkascan Harvester
#
#  Copyright 2018-2022 Stichting Polkascan (Polkascan Foundation).
#  This file is part of Polkascan.
#
#  Polkascan is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Polkascan is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Polkascan. If not, see <http://www.gnu.org/licenses/>.

import codecs
from datetime import timezone

import sqlalchemy as sa
from sqlalchemy.dialects.mysql import INTEGER, NUMERIC, TINYINT


class UTCDateTime(sa.types.TypeDecorator):

    impl = sa.types.DateTime

    def process_bind_param(self, value, engine):
        if value is None:
            return
        if value.utcoffset() is None:
            raise ValueError(
                'Got naive datetime while timezone-aware is expected'
            )
        return value.astimezone(timezone.utc)

    def result_processor(self, dialect, coltype):
        """Return a processor that encodes hex values."""
        def process(value):
            return value.replace(tzinfo=timezone.utc)
        return process

    def adapt(self, impltype):
        """Produce an adapted form of this type, given an impl class."""
        return UTCDateTime()


class HashBinary(sa.types.BINARY):
    """Extend BINARY to handle hex strings."""

    impl = sa.types.BINARY

    def bind_processor(self, dialect):
        """Return a processor that decodes hex values."""
        def process(value):
            return value and codecs.decode(value[2:], 'hex') or None
        return process

    def result_processor(self, dialect, coltype):
        """Return a processor that encodes hex values."""
        def process(value):
            return value and f"0x{codecs.encode(value, 'hex').decode('utf-8')}" or None
        return process

    def adapt(self, impltype):
        """Produce an adapted form of this type, given an impl class."""
        return HashBinary()


class HashVarBinary(sa.types.VARBINARY):
    """Extend VARBINARY to handle hex strings."""

    impl = sa.types.VARBINARY

    def bind_processor(self, dialect):
        """Return a processor that decodes hex values."""
        def process(value):
            return value and codecs.decode(value[2:], 'hex') or None
        return process

    def result_processor(self, dialect, coltype):
        """Return a processor that encodes hex values."""
        def process(value):
            return value and f"0x{codecs.encode(value, 'hex').decode('utf-8')}" or None
        return process

    def adapt(self, impltype):
        """Produce an adapted form of this type, given an impl class."""
        return HashVarBinary()

