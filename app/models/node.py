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

import sqlalchemy as sa

from app.models.base import BaseModel


class NodeBlockHeader(BaseModel):
    __tablename__ = 'node_block_header'

    hash = sa.Column(sa.types.BINARY(32), primary_key=True, nullable=False)
    block_number = sa.Column(sa.Integer(), nullable=False, index=True)

    parent_hash = sa.Column(sa.types.BINARY(32), nullable=False)
    number = sa.Column(sa.types.VARBINARY(5), nullable=False)

    extrinsics_root = sa.Column(sa.types.BINARY(32), nullable=False)
    state_root = sa.Column(sa.types.BINARY(32), nullable=False)

    count_extrinsics = sa.Column(sa.Integer(), nullable=False, server_default='0')
    count_logs = sa.Column(sa.Integer(), nullable=False, server_default='0')

    def __repr__(self):
        return "<{}(hash={})>".format(self.__class__.__name__, self.hash.hex())


class NodeBlockRuntime(BaseModel):
    __tablename__ = 'node_block_runtime'

    hash = sa.Column(sa.types.BINARY(32), primary_key=True, nullable=False)
    block_number = sa.Column(sa.Integer(), nullable=False, index=True)

    spec_name = sa.Column(sa.String(32), nullable=False)
    spec_version = sa.Column(sa.Integer(), nullable=False)

    def __repr__(self):
        return "<{}(hash={})>".format(self.__class__.__name__, self.hash.hex())


class NodeBlockExtrinsic(BaseModel):
    __tablename__ = 'node_block_extrinsic'

    block_hash = sa.Column(sa.types.BINARY(32), primary_key=True, index=True, nullable=False)
    extrinsic_idx = sa.Column(sa.Integer(), primary_key=True, index=True, nullable=False)
    block_number = sa.Column(sa.Integer(), nullable=False, index=True)

    data = sa.Column(sa.types.LargeBinary(length=(2**32)-1), nullable=False)
    hash = sa.Column(sa.types.BINARY(32), nullable=False)
    length = sa.Column(sa.types.VARBINARY(5), nullable=False)

    def __repr__(self):
        return f"<{self.__class__.__name__}(block_number={self.block_number}, extrinsic_idx={self.extrinsic_idx})>"


class NodeBlockHeaderDigestLog(BaseModel):
    __tablename__ = 'node_block_header_digest_log'

    block_hash = sa.Column(sa.types.BINARY(32), primary_key=True, index=True, nullable=False)
    log_idx = sa.Column(sa.Integer(), primary_key=True, index=True, nullable=False)

    block_number = sa.Column(sa.Integer(), nullable=False, index=True)

    data = sa.Column(sa.types.LargeBinary(length=(2**32)-1), nullable=False)

    def __repr__(self):
        return "<{}(hash={}, log_idx={})>".format(self.__class__.__name__, self.block_hash.hex(), self.log_idx)


class NodeBlockStorage(BaseModel):
    __tablename__ = 'node_block_storage'

    block_hash = sa.Column(sa.types.BINARY(32), primary_key=True, index=True, nullable=False)
    storage_key = sa.Column(sa.VARBINARY(128), primary_key=True, index=True)

    block_number = sa.Column(sa.Integer(), nullable=False, index=True)

    storage_module = sa.Column(sa.String(255), nullable=True, index=True)
    storage_name = sa.Column(sa.String(255), nullable=True, index=True)

    data = sa.Column(sa.types.LargeBinary(length=(2**32)-1), nullable=True)

    complete = sa.Column(sa.Boolean(), nullable=False, default=False, index=True)

    def __repr__(self):
        return "<{}(storage_key={}, block_hash={})>".format(
            self.__class__.__name__, self.storage_key.hex(), self.block_hash.hex()
        )


class NodeMetadata(BaseModel):
    __tablename__ = 'node_metadata'

    spec_name = sa.Column(sa.String(64), nullable=False, primary_key=True, index=True)
    spec_version = sa.Column(sa.Integer(), nullable=False, primary_key=True, index=True)
    block_hash = sa.Column(sa.types.BINARY(32), nullable=False)
    data = sa.Column(sa.types.LargeBinary(length=(2**32)-1), nullable=True)

    complete = sa.Column(sa.Boolean(), nullable=False, default=False, index=True)

    def __repr__(self):
        return "<{}(spec_name={}, spec_version={})>".format(
            self.__class__.__name__, self.spec_name, self.spec_version
        )


class NodeRuntime(BaseModel):
    __tablename__ = 'node_runtime'

    impl_name = sa.Column(sa.String(64), nullable=False, primary_key=True, index=True)
    impl_version = sa.Column(sa.Integer(), nullable=False, primary_key=True, index=True)
    spec_name = sa.Column(sa.String(64), nullable=False, primary_key=True, index=True)
    spec_version = sa.Column(sa.Integer(), nullable=False, primary_key=True, index=True)
    authoring_version = sa.Column(sa.Integer(), nullable=False, primary_key=True, index=True)
    transaction_version = sa.Column(sa.Integer(), nullable=True)

    block_hash = sa.Column(sa.types.BINARY(32), nullable=False)
    block_number = sa.Column(sa.Integer(), nullable=False)
    apis = sa.Column(sa.JSON(), nullable=True)
    code = sa.Column(sa.types.LargeBinary(length=(2**32)-1), nullable=True)

    complete = sa.Column(sa.Boolean(), nullable=False, default=False, index=True)

    def __repr__(self):
        return "<{}(impl_name={}, impl_version={}, spec_name={}, spec_version={}, authoring_versio={})>".format(
            self.__class__.__name__, self.impl_name, self.impl_version, self.spec_name, self.spec_version,
            self.authoring_version
        )


class HarvesterStatus(BaseModel):
    __tablename__ = 'harvester_status'

    key = sa.Column(sa.String(64), nullable=False, primary_key=True, index=True)
    description = sa.Column(sa.String(255), nullable=True)
    value = sa.Column(sa.JSON(), nullable=True)

    def __repr__(self):
        return "<{}(key={})>".format(self.__class__.__name__, self.key)


class HarvesterStorageCron(BaseModel):
    __tablename__ = 'harvester_storage_cron'

    block_number_interval = sa.Column(sa.Integer(), primary_key=True, index=True)

    storage_module = sa.Column(sa.String(255), primary_key=True, index=True)
    storage_name = sa.Column(sa.String(255), primary_key=True, index=True)

    storage_key = sa.Column(sa.VARBINARY(128))

    def __repr__(self):
        return f"<{self.__class__.__name__}(block_number_interval={self.block_number_interval})," \
               f" storage_module={self.storage_module}, storage_name={self.storage_name}>"


class HarvesterStorageTask(BaseModel):
    __tablename__ = 'harvester_storage_task'

    id = sa.Column(sa.Integer(), primary_key=True, autoincrement=True)
    description = sa.Column(sa.String(255), nullable=True)
    storage_key = sa.Column(sa.types.VARBINARY(128), nullable=True)
    storage_key_prefix = sa.Column(sa.types.VARBINARY(32), nullable=True)

    storage_pallet = sa.Column(sa.String(255), nullable=True)
    storage_name = sa.Column(sa.String(255), nullable=True)

    blocks = sa.Column(sa.JSON(), nullable=True)

    complete = sa.Column(sa.Boolean(), nullable=True, default=False, index=True)

