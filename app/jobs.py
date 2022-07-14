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
import json
from hashlib import blake2b

from sqlalchemy.exc import IntegrityError

from scalecodec.base import ScaleType
from sqlalchemy import func

from app import settings
from app.base import Job, GracefulInterruptHandler
from app.exceptions import ShutdownException
from app.models.codec import CodecBlockExtrinsic, CodecBlockHeaderDigestLog, CodecBlockStorage, CodecBlockEvent, \
    CodecMetadata, Runtime, RuntimePallet, RuntimeCall, RuntimeCallArgument, RuntimeEvent, RuntimeEventAttribute, \
    RuntimeStorage, RuntimeConstant, RuntimeErrorMessage, RuntimeType
from app.models.node import NodeBlockExtrinsic, NodeBlockStorage, HarvesterStatus, NodeBlockHeader, \
    NodeBlockHeaderDigestLog, NodeBlockRuntime, NodeRuntime, NodeMetadata, HarvesterStorageTask
from scalecodec.base import ScaleDecoder, ScaleBytes
from scalecodec.exceptions import RemainingScaleBytesNotEmptyException
from substrateinterface.utils.hasher import xxh128


class Cron(Job):

    icon = '⌛'

    def decode_storage_item(self, node_storage, codec_block_storage):

        decoded_storage_entry = self.db_substrate.query(
            module=node_storage.storage_module,
            storage_function=node_storage.storage_name,
            raw_storage_key=node_storage.storage_key,
            block_hash=f'0x{node_storage.block_hash.hex()}'
        )
        if decoded_storage_entry:
            codec_block_storage.data = decoded_storage_entry.value
            codec_block_storage.scale_type = decoded_storage_entry.type_string

            if codec_block_storage.storage_key == self.harvester.event_storage_key:

                for event_idx, event_data in enumerate(codec_block_storage.data):

                    event_data['event_index'] = f"0x{event_data['event_index']}"

                    codec_event = CodecBlockEvent(
                        block_hash=codec_block_storage.block_hash,
                        block_number=codec_block_storage.block_number,
                        event_idx=event_idx,
                        scale_type='EventRecord<Event, Hash>',
                        event_module=event_data['module_id'],
                        event_name=event_data['event_id'],
                        extrinsic_idx=event_data['extrinsic_idx'],
                        data=event_data,
                        complete=True
                    )

                    codec_event.save(self.session)

                self.log(f'Decoded events for #{node_storage.block_number}')

        codec_block_storage.complete = True
        codec_block_storage.retry = False

        codec_block_storage.save(self.session)
        self.session.commit()

    def decode_extrinsic(self, extrinsic, node_block_extrinsic):
        self.db_substrate.init_runtime(block_hash=f'0x{extrinsic.block_hash.hex()}')

        block_extrinsic = self.db_substrate.runtime_config.create_scale_object(
            "Extrinsic",
            data=ScaleBytes(node_block_extrinsic.length + node_block_extrinsic.data),
            metadata=self.db_substrate.metadata_decoder
        )
        block_extrinsic.decode()

        extrinsic.data = block_extrinsic.value
        extrinsic.call_module = block_extrinsic.value['call']['call_module']
        extrinsic.call_name = block_extrinsic.value['call']['call_function']
        extrinsic.signed = 'signature' in block_extrinsic.value
        extrinsic.complete = True
        extrinsic.retry = False
        extrinsic.save(self.session)
        self.session.commit()

    def decode_log_item(self, log_item, node_log_item):
        self.db_substrate.init_runtime(block_hash=f'0x{node_log_item.block_hash.hex()}')

        log_digest = self.db_substrate.runtime_config.create_scale_object(
            'sp_runtime::generic::digest::DigestItem', data=ScaleBytes(node_log_item.data)
        )
        log_digest.decode()

        log_item.data = log_digest.value
        log_item.complete = True
        log_item.retry = False
        log_item.save(self.session)
        self.session.commit()

    def start(self):

        incomplete_extrinsics = CodecBlockExtrinsic.query(self.session).filter(
            CodecBlockExtrinsic.retry == True
        )

        for extrinsic in incomplete_extrinsics.limit(1000):

            node_block_extrinsic = NodeBlockExtrinsic.query(self.session).filter_by(
                block_hash=extrinsic.block_hash,
                extrinsic_idx=extrinsic.extrinsic_idx
            ).first()

            if node_block_extrinsic:
                try:

                    self.decode_extrinsic(extrinsic, node_block_extrinsic)

                    self.log('Decoded extrinsic {}-{}'.format(
                        extrinsic.block_number, extrinsic.extrinsic_idx)
                    )

                except Exception as e:
                    self.log('⚠️  Failed to decode extrinsic {}-{} ({})'.format(
                        extrinsic.block_number, extrinsic.extrinsic_idx, e),
                    )
                    extrinsic.retry = False
                    extrinsic.save(self.session)
                    self.session.commit()

        incomplete_log = CodecBlockHeaderDigestLog.query(self.session).filter(
            CodecBlockHeaderDigestLog.retry == True
        )

        for log_item in incomplete_log.limit(1000):

            node_log_item = NodeBlockHeaderDigestLog.query(self.session).filter_by(
                block_hash=log_item.block_hash,
                log_idx=log_item.log_idx
            ).first()

            if node_log_item:
                try:

                    self.decode_log_item(log_item, node_log_item)

                    self.log(f'Decoded log item {log_item.block_number}-{log_item.log_idx}')

                except Exception as e:
                    self.log('⚠️  Failed to decode log {}-{} ({})'.format(
                        log_item.block_number, log_item.log_idx, e),
                    )
                    log_item.retry = False
                    log_item.save(self.session)
                    self.session.commit()

        incomplete_storage = CodecBlockStorage.query(self.session).filter(
            CodecBlockStorage.retry == True
        )

        for storage_item in incomplete_storage.limit(1000):

            node_storage_item = NodeBlockStorage.query(self.session).filter_by(
                block_hash=storage_item.block_hash,
                storage_key=storage_item.storage_key
            ).first()

            if node_storage_item:
                try:

                    self.decode_storage_item(node_storage_item, storage_item)

                    self.log(f'Decoded storage item #{storage_item.block_number} "{storage_item.storage_module}.{storage_item.storage_name}"')

                except Exception as e:
                    self.log(f'⚠️  Failed to decode storage item #{storage_item.block_number} "{storage_item.storage_module}.{storage_item.storage_name}"')
                    storage_item.retry = False
                    storage_item.save(self.session)
                    self.session.commit()

        # pending_amount = NodeBlockStorage.query(self.session).filter_by(complete=False).count()
        #
        # self.log('{} pending storage calls'.format(pending_amount))


class RetrieveBlocks(Job):

    icon = '🔗'

    def add_block(self, block_number):
        self.log("🔎 [{}]".format('chain_getBlockHash'), 3)
        block_hash = self.substrate.get_block_hash(block_number)

        self.log("🔎 [{}]".format('chain_getBlock'), 3)
        block_response = self.harvester.rpc_call('chain_getBlock', [block_hash])

        number_obj = self.substrate.create_scale_object('Compact<BlockNumber>')
        number_obj.encode(block_number)

        block_hash = bytes.fromhex(block_hash[2:])

        # Store block header

        block_header = NodeBlockHeader(
            hash=block_hash,
            parent_hash=bytes.fromhex(block_response['result']['block']['header']['parentHash'][2:]),
            number=bytes(number_obj.data.data),
            extrinsics_root=bytes.fromhex(block_response['result']['block']['header']['extrinsicsRoot'][2:]),
            state_root=bytes.fromhex(block_response['result']['block']['header']['stateRoot'][2:]),
            block_number=block_number,
            count_extrinsics=len(block_response['result']['block']['extrinsics']),
            count_logs=len(block_response['result']['block']['header']['digest']['logs']),
        )
        block_header.save(self.session)

        # Store extrinsics

        for extrinsic_idx, extrinsic_data in enumerate(block_response['result']['block']['extrinsics']):
            data_obj = self.substrate.create_scale_object('HexBytes', data=ScaleBytes(extrinsic_data))
            data_obj.decode()

            extrinsic_bytes = bytes.fromhex(data_obj.value[2:])

            extrinsic = NodeBlockExtrinsic(
                block_hash=block_hash,
                extrinsic_idx=extrinsic_idx,
                data=extrinsic_bytes,
                length=bytes(data_obj.length_obj.compact_bytes),
                hash=blake2b(data_obj.data.data, digest_size=32).digest(),
                block_number=block_number
            )
            extrinsic.save(self.session)

        # Store digest logs

        for log_idx, digest_log_data in enumerate(block_response['result']['block']['header']['digest']['logs']):
            digest_log = NodeBlockHeaderDigestLog(
                block_hash=block_hash,
                log_idx=log_idx,
                data=bytes.fromhex(digest_log_data[2:]),
                block_number=block_number
            )
            digest_log.save(self.session)

    def start(self):
        finalised_hash = self.substrate.get_chain_finalised_head()
        finalised_block_number = self.substrate.get_block_number(finalised_hash)

        chaintip_hash = self.substrate.get_chain_head()
        chaintip_block_number = self.substrate.get_block_number(chaintip_hash)

        # Store in settings
        HarvesterStatus.query(self.session).filter_by(key='CHAINTIP_BLOCKNUMBER').update(
            {HarvesterStatus.value: chaintip_block_number}, synchronize_session='fetch'
        )
        HarvesterStatus.query(self.session).filter_by(key='CHAINTIP_HASH').update(
            {HarvesterStatus.value: chaintip_hash}, synchronize_session='fetch'
        )
        HarvesterStatus.query(self.session).filter_by(key='FINALIZATION_BLOCKNUMBER').update(
            {HarvesterStatus.value: finalised_block_number}, synchronize_session='fetch'
        )
        HarvesterStatus.query(self.session).filter_by(key='FINALIZATION_HASH').update(
            {HarvesterStatus.value: finalised_hash}, synchronize_session='fetch'
        )

        self.session.commit()

        if self.session.query(func.max(NodeBlockHeader.block_number)).one()[0] is None:
            max_block_number = 0
        else:
            max_block_number = self.session.query(func.max(NodeBlockHeader.block_number)).one()[0] + 1

        gaps = [{'block_from': max_block_number, 'block_to': finalised_block_number}]

        with GracefulInterruptHandler() as interrupt_handler:
            for row in gaps:
                for block_number in range(row['block_from'], row['block_to'] + 1):
                    try:
                        with self.harvester.prom_block_process_speed.time():
                            self.add_block(block_number=block_number)
                            HarvesterStatus.query(self.session).filter_by(key='PROCESS_BLOCKS_MAX_BLOCKNUMBER').update(
                                {HarvesterStatus.value: block_number}, synchronize_session='fetch'
                            )
                            self.session.commit()

                    except ValueError:
                        self.session.rollback()
                        raise
                        # raise BlockDecodeException("Decoding error: Block #{}".format(block_id))
                    self.log("Retrieving block #{} from node".format(block_number), 2)
                    if interrupt_handler.interrupted:
                        self.log("🛑 Warm shutdown initiated", 1)
                        raise ShutdownException()


class RetrieveRuntimeState(Job):

    icon = '🗄️'

    def start(self):
        """
        Second step in the harvester: store runtime state (only present in archive node)
        :return:
        """

        # TODO store in memory
        if self.session.query(func.max(NodeBlockRuntime.block_number)).one()[0] is None:
            current_block_id = 0
        else:
            current_block_id = self.session.query(func.max(NodeBlockRuntime.block_number)).one()[0] + 1

        with GracefulInterruptHandler() as interrupt_handler:
            item = self.session.query(NodeBlockHeader.hash, NodeBlockHeader.block_number).filter(
                NodeBlockHeader.block_number == current_block_id
            ).first()

            while item:

                self.log('Process runtime state for #{}'.format(item.block_number))

                self.storage_block_runtime_data(block_hash=item.hash, block_number=item.block_number)
                HarvesterStatus.query(self.session).filter_by(key='PROCESS_STATE_MAX_BLOCKNUMBER').update(
                    {HarvesterStatus.value: item.block_number}, synchronize_session='fetch'
                )
                self.session.commit()

                if interrupt_handler.interrupted:
                    self.log("🛑 Warm shutdown initiated", 1)
                    raise ShutdownException()

                current_block_id += 1

                item = self.session.query(NodeBlockHeader.hash, NodeBlockHeader.block_number).filter(
                    NodeBlockHeader.block_number == current_block_id
                ).first()

    def storage_block_runtime_data(self, block_hash, block_number):

        # Store runtime information
        block_hash_hex = '0x{}'.format(block_hash.hex())

        self.log("🔎 [{}]".format('chain_getRuntimeVersion'), 3)
        runtime_response = self.harvester.rpc_call('chain_getRuntimeVersion', [block_hash_hex])

        block_runtime = NodeBlockRuntime(
            hash=block_hash,
            block_number=block_number,
            spec_name=runtime_response['result']['specName'],
            spec_version=runtime_response['result']['specVersion']
        )

        block_runtime.save(self.session)

        # Store storage entries from cron
        for cron_entry in self.harvester.storage_cron_entries:

            if block_number % cron_entry.block_number_interval == 0:

                if cron_entry.storage_key is None:

                    storage_hash = self.substrate.generate_storage_hash(
                        storage_module=cron_entry.storage_module,
                        storage_function=cron_entry.storage_name
                    )

                    cron_entry.storage_key = bytes.fromhex(storage_hash[2:])
                    cron_entry.save(self.session)

                events_response = self.harvester.rpc_call(
                    "state_getStorageAt", [f'0x{cron_entry.storage_key.hex()}', block_hash_hex]
                )

                if events_response.get('result'):
                    events_data = bytes.fromhex(events_response.get('result')[2:])
                else:
                    events_data = None

                storage_item = NodeBlockStorage(
                    block_hash=block_hash,
                    storage_key=cron_entry.storage_key,
                    data=events_data,
                    block_number=block_number,
                    storage_module=cron_entry.storage_module,
                    storage_name=cron_entry.storage_name,
                    complete=True
                )

                storage_item.save(self.session)

        # Check if runtime exists TODO optimize this

        node_runtime = NodeRuntime.query(self.session).get(
            (
                runtime_response['result']['implName'],
                runtime_response['result']['implVersion'],
                runtime_response['result']['specName'],
                runtime_response['result']['specVersion'],
                runtime_response['result']['authoringVersion']
            )
        )

        if not node_runtime:
            node_runtime = NodeRuntime(
                impl_name=runtime_response['result']['implName'],
                impl_version=runtime_response['result']['implVersion'],
                spec_name=runtime_response['result']['specName'],
                spec_version=runtime_response['result']['specVersion'],
                authoring_version=runtime_response['result']['authoringVersion'],
                transaction_version=runtime_response['result'].get('transactionVersion'),
                block_hash=block_hash,
                block_number=block_number,
                apis=runtime_response['result']['apis'],
                complete=False
            )
            node_runtime.save(self.session)

            # Check if metadata exists TODO optimize this

            node_metadata = NodeMetadata.query(self.session).get(
                (runtime_response['result']['specName'], runtime_response['result']['specVersion'])
            )

            if not node_metadata:
                self.log("🔎 [{}]".format('state_getMetadata'), 3)
                metadata_response = self.harvester.rpc_call(
                    'state_getMetadata', ['0x{}'.format(block_hash.hex())]
                )

                if metadata_response.get('result'):
                    node_metadata = NodeMetadata(
                        spec_name=runtime_response['result']['specName'],
                        spec_version=runtime_response['result']['specVersion'],
                        block_hash=block_hash,
                        data=bytes.fromhex(metadata_response.get('result')[2:]),
                        complete=True
                    )
                    node_metadata.save(self.session)

                    # Decode metadata and runtime

                    codec_metadata = CodecMetadata(
                        spec_name=node_metadata.spec_name,
                        spec_version=node_metadata.spec_version,
                        scale_type='MetadataVersioned'
                    )

                    # try:
                    metadata_decoder = self.substrate.runtime_config.create_scale_object(
                        "MetadataVersioned",
                        data=ScaleBytes(node_metadata.data)
                    )
                    codec_metadata.data = metadata_decoder.decode()
                    codec_metadata.complete = True

                    self.substrate.metadata_decoder = metadata_decoder

                    if self.substrate.implements_scaleinfo():
                        self.substrate.reload_type_registry()
                        self.substrate.runtime_config.add_portable_registry(metadata_decoder)

                    self.store_runtime(metadata_decoder, node_runtime, block_hash)

                    # except:
                    #     codec_metadata.complete = False

                    codec_metadata.save(self.session)

    def store_runtime(self, metadata_decoder, runtime_info, block_hash):
        # Store metadata in database
        self.log(f'Store runtime {runtime_info.spec_name}-{runtime_info.spec_version}')

        runtime = Runtime(
            spec_name=runtime_info.spec_name,
            spec_version=runtime_info.spec_version,
            impl_name=runtime_info.impl_name,
            impl_version=runtime_info.impl_version,
            authoring_version=runtime_info.authoring_version,
            count_call_functions=0,
            count_events=0,
            count_pallets=len(metadata_decoder.pallets),
            count_storage_functions=0,
            count_constants=0,
            count_errors=0
        )

        runtime.save(self.session)

        for module_index, module in enumerate(metadata_decoder.pallets):

            if hasattr(module, 'index'):
                module_index = module.index

            if 'index' in module.value:
                module_index = module.value['index']

            # Storage backwards compt check
            if module.storage and isinstance(module.storage, list):
                storage_functions = module.storage
            elif module.storage and isinstance(getattr(module.storage, 'value'), dict):
                storage_functions = module.storage.items
            else:
                storage_functions = []

            runtime_module = RuntimePallet(
                spec_name=runtime_info.spec_name,
                spec_version=runtime_info.spec_version,
                pallet=module.name,
                prefix=module.value['storage']['prefix'] if module.value.get('storage') else None,
                name=module.name,
                count_call_functions=len(module.calls or []),
                count_storage_functions=len(storage_functions),
                count_events=len(module.events or []),
                count_constants=len(module.constants or []),
                count_errors=len(module.errors or []),
            )
            runtime_module.save(self.session)

            # Update totals in runtime
            runtime.count_call_functions += runtime_module.count_call_functions
            runtime.count_events += runtime_module.count_events
            runtime.count_storage_functions += runtime_module.count_storage_functions
            runtime.count_constants += runtime_module.count_constants
            runtime.count_errors += runtime_module.count_errors

            if len(module.calls or []) > 0:
                for idx, call in enumerate(module.calls):

                    if 'index' in call:
                        call_index = call['index'].value
                    else:
                        call_index = idx

                    runtime_call = RuntimeCall(
                        spec_name=runtime_module.spec_name,
                        spec_version=runtime_module.spec_version,
                        pallet=runtime_module.pallet,
                        call_name=call.name,
                        pallet_call_idx=idx,
                        lookup=bytes.fromhex("{:02x}{:02x}".format(module_index, call_index)),
                        documentation='\n'.join(call.docs),
                        count_arguments=len(call.args)
                    )
                    runtime_call.save(self.session)

                    for arg_idx, arg in enumerate(call.args):

                        if arg.value.get('typeName'):
                            scale_type = arg.value.get('typeName')
                        else:
                            scale_type = arg.type

                        runtime_call_arg = RuntimeCallArgument(
                            spec_name=runtime_module.spec_name,
                            spec_version=runtime_module.spec_version,
                            pallet=runtime_module.pallet,
                            call_name=call.name,
                            call_argument_idx=arg_idx,
                            name=arg.name,
                            scale_type=scale_type
                        )
                        runtime_call_arg.save(self.session)

            if len(module.events or []) > 0:
                for event_index, event in enumerate(module.events):

                    if 'index' in event:
                        event_index = event['index'].value

                    runtime_event = RuntimeEvent(
                        spec_name=runtime_module.spec_name,
                        spec_version=runtime_module.spec_version,
                        pallet=runtime_module.pallet,
                        event_name=event.name,
                        pallet_event_idx=event_index,
                        lookup=bytes.fromhex("{:02x}{:02x}".format(module_index, event_index)),
                        documentation='\n'.join(event.docs),
                        count_attributes=len(event.args)
                    )
                    runtime_event.save(self.session)

                    for arg_index, arg in enumerate(event.args):
                        if type(arg.value) is str:
                            scale_type = arg.value
                        elif arg.value.get('typeName'):
                            scale_type = arg.value.get('typeName')
                        else:
                            scale_type = arg.type

                        runtime_event_attr = RuntimeEventAttribute(
                            spec_name=runtime_module.spec_name,
                            spec_version=runtime_module.spec_version,
                            pallet=runtime_module.pallet,
                            event_name=event.name,
                            event_attribute_idx=arg_index,
                            scale_type=scale_type
                        )
                        runtime_event_attr.save(self.session)

            if len(storage_functions) > 0:
                for idx, storage in enumerate(storage_functions):

                    # Determine type
                    type_hasher = None
                    type_key1 = None
                    type_key2 = None
                    type_value = None
                    type_is_linked = None
                    type_key2hasher = None

                    if storage.type.get('Plain'):
                        type_value = storage.type.get('PlainType')

                    elif storage.type.get('Map'):
                        type_hasher = storage.type['Map'].get('hasher')
                        type_value = storage.type['Map'].get('value')

                    # Determine default
                    if 'default' in storage:
                        storage_default = storage['default'].get_used_bytes()
                    elif 'fallback' in storage:
                        storage_default = storage['fallback'].get_used_bytes()
                    else:
                        storage_default = None

                    runtime_storage = RuntimeStorage(
                        spec_name=runtime_module.spec_name,
                        spec_version=runtime_module.spec_version,
                        pallet=runtime_module.pallet,
                        pallet_storage_idx=idx,
                        storage_name=storage.name,
                        default=storage_default,
                        modifier=storage.value['modifier'],
                        key_prefix_pallet=bytes.fromhex(xxh128(module.value['storage']['prefix'].encode())),
                        key_prefix_name=bytes.fromhex(xxh128(storage.name.encode())),
                        key1_hasher=type_hasher,
                        key1_scale_type=type_key1,
                        key2_scale_type=type_key2,
                        value_scale_type=type_value,
                        is_linked=type_is_linked,
                        key2_hasher=type_key2hasher,
                        documentation='\n'.join(storage.value['documentation'])
                    )
                    runtime_storage.save(self.session)

            if len(module.constants or []) > 0:
                for idx, constant in enumerate(module.constants):

                    # Decode value
                    try:
                        value_obj = self.substrate.runtime_config.create_scale_object(
                            constant.type,
                            ScaleBytes(constant.constant_value)
                        )
                        value_obj.decode()
                        value = value_obj.serialize()
                    except (ValueError, RemainingScaleBytesNotEmptyException, NotImplementedError):
                        value = constant.constant_value

                    if type(value) is bytearray:
                        value = value.hex()

                    if type(value) is list or type(value) is dict:
                        value = json.dumps(value)

                    runtime_constant = RuntimeConstant(
                        spec_name=runtime_module.spec_name,
                        spec_version=runtime_module.spec_version,
                        pallet=runtime_module.pallet,
                        pallet_constant_idx=idx,
                        constant_name=constant.name,
                        scale_type=constant.type,
                        value=value,
                        documentation='\n'.join(constant.docs)
                    )
                    runtime_constant.save(self.session)

            if len(module.errors or []) > 0:
                for idx, error in enumerate(module.errors):
                    runtime_error = RuntimeErrorMessage(
                        spec_name=runtime_module.spec_name,
                        spec_version=runtime_module.spec_version,
                        pallet=runtime_module.pallet,
                        error_idx=module_index,
                        pallet_idx=idx,
                        error_name=error.name,
                        documentation='\n'.join(error.docs)
                    )
                    runtime_error.save(self.session)

            runtime.save(self.session)

        # Process types
        for runtime_type_data in list(self.substrate.get_type_registry(block_hash=f'0x{block_hash.hex()}').values()):
            runtime_type = RuntimeType(
                spec_name=runtime_info.spec_name,
                spec_version=runtime_info.spec_version,
                scale_type=runtime_type_data["type_string"],
                decoder_class=runtime_type_data["decoder_class"],
                is_core_primitive=runtime_type_data["is_primitive_core"],
                is_runtime_primitive=runtime_type_data["is_primitive_runtime"]
            )
            runtime_type.save(self.session)


class ScaleDecode(Job):

    icon = '⚙️'

    def start(self):
        # Extrinsics
        min_extrinsic_block_id = (self.session.query(func.max(CodecBlockExtrinsic.block_number)).one()[0] or -1) + 1

        max_extrinsic_block_id = (self.session.query(func.max(NodeBlockExtrinsic.block_number)).one()[0] or -1)

        # Yield per 1000
        max_extrinsic_block_id = min(max_extrinsic_block_id, min_extrinsic_block_id + self.yield_per)

        with GracefulInterruptHandler() as interrupt_handler:

            for current_block_id in range(min_extrinsic_block_id, max_extrinsic_block_id + 1):

                block_extrinsics = NodeBlockExtrinsic.query(self.session).filter(
                    NodeBlockExtrinsic.block_number == current_block_id
                )

                for node_extrinsic in block_extrinsics:
                    self.decode_extrinsic(node_extrinsic)

                self.log('Decoded extrinsics for #{}'.format(current_block_id))

                self.session.commit()

                if interrupt_handler.interrupted:
                    self.log("🛑 Warm shutdown initiated", 1)
                    raise ShutdownException()

        # Logs
        min_log_block_id = (self.session.query(func.max(CodecBlockHeaderDigestLog.block_number)).one()[0] or -1) + 1

        max_log_block_id = (self.session.query(func.max(NodeBlockHeaderDigestLog.block_number)).one()[0] or -1)

        # Yield per 1000
        max_log_block_id = min(max_log_block_id, min_log_block_id + self.yield_per)

        with GracefulInterruptHandler() as interrupt_handler:

            for current_block_id in range(min_log_block_id, max_log_block_id + 1):

                block_logs = NodeBlockHeaderDigestLog.query(self.session).filter(
                    NodeBlockHeaderDigestLog.block_number == current_block_id
                )

                for node_log_item in block_logs:
                    self.decode_log_item(node_log_item)

                self.log('Decoded logs for #{}'.format(current_block_id))

                self.session.commit()

                if interrupt_handler.interrupted:
                    self.log("🛑 Warm shutdown initiated", 1)
                    raise ShutdownException()

        # Storage
        min_storage_block_id = (self.session.query(func.max(CodecBlockStorage.block_number)).one()[0] or -1) + 1

        max_storage_block_id = (self.session.query(func.max(NodeBlockStorage.block_number)).one()[0] or -1)

        max_storage_block_id = min(max_storage_block_id, min_storage_block_id + self.yield_per)

        with GracefulInterruptHandler() as interrupt_handler:

            for current_block_id in range(min_storage_block_id, max_storage_block_id + 1):

                block_storage = NodeBlockStorage.query(self.session).filter(
                    NodeBlockStorage.block_number == current_block_id
                )

                for node_storage in block_storage:
                    self.decode_storage_item(node_storage)

                if interrupt_handler.interrupted:
                    self.log("🛑 Warm shutdown initiated", 1)
                    raise ShutdownException()

        # Update status record
        HarvesterStatus.query(self.session).filter_by(key='PROCESS_DECODER_MAX_BLOCKNUMBER').update(
            {HarvesterStatus.value: min(max_extrinsic_block_id, max_log_block_id, max_storage_block_id)},
            synchronize_session='fetch'
        )
        self.session.commit()

    def decode_extrinsic(self, node_block_extrinsic: NodeBlockExtrinsic):
        self.db_substrate.init_runtime(block_hash=f'0x{node_block_extrinsic.block_hash.hex()}')

        extrinsic = CodecBlockExtrinsic(
            block_hash=node_block_extrinsic.block_hash,
            block_number=node_block_extrinsic.block_number,
            extrinsic_idx=node_block_extrinsic.extrinsic_idx,
            scale_type='Extrinsic',
            complete=False
        )
        try:
            scale_extrinsic = self.db_substrate.runtime_config.create_scale_object(
                "Extrinsic",
                data=ScaleBytes(node_block_extrinsic.length + node_block_extrinsic.data),
                metadata=self.db_substrate.metadata_decoder
            )
            scale_extrinsic.decode()

            # Workaround put MultiAddress as address

            extrinsic.data = scale_extrinsic.value
            extrinsic.call_module = scale_extrinsic.value['call']['call_module']
            extrinsic.call_name = scale_extrinsic.value['call']['call_function']
            extrinsic.complete = True
            extrinsic.signed = 'signature' in scale_extrinsic.value

        except Exception as e:
            self.log('⚠️  Failed to decode extrinsic {}-{} ({})'.format(
                node_block_extrinsic.block_number, node_block_extrinsic.extrinsic_idx, e),
            )
            extrinsic.retry = True

        extrinsic.save(self.session)

    def decode_log_item(self, node_log_item: NodeBlockHeaderDigestLog):
        self.db_substrate.init_runtime(block_hash=f'0x{node_log_item.block_hash.hex()}')

        log_item = CodecBlockHeaderDigestLog(
            block_hash=node_log_item.block_hash,
            block_number=node_log_item.block_number,
            log_idx=node_log_item.log_idx,
            scale_type='sp_runtime::generic::digest::DigestItem',
            complete=False
        )
        try:
            scale_log_item = self.db_substrate.runtime_config.create_scale_object(
                type_string='sp_runtime::generic::digest::DigestItem',
                data=ScaleBytes(node_log_item.data),
                metadata=self.db_substrate.metadata_decoder
            )

            log_item.data = scale_log_item.decode()
            log_item.complete = True
        except Exception as e:
            self.log('⚠️  Failed to decode log item {}-{} ({})'.format(
                node_log_item.block_number, node_log_item.log_idx, e),
            )
            log_item.retry = True

        log_item.save(self.session)

    def decode_storage_item(self, node_storage):
        codec_block_storage = CodecBlockStorage(
            block_hash=node_storage.block_hash,
            block_number=node_storage.block_number,
            storage_key=node_storage.storage_key,
            storage_module=node_storage.storage_module,
            storage_name=node_storage.storage_name
        )

        try:
            decoded_storage_entry = self.db_substrate.query(
                module=node_storage.storage_module,
                storage_function=node_storage.storage_name,
                block_hash=f'0x{node_storage.block_hash.hex()}'
            )
            if decoded_storage_entry:
                codec_block_storage.data = decoded_storage_entry.value
                codec_block_storage.scale_type = decoded_storage_entry.type_string or \
                    decoded_storage_entry.__class__.__name__

            codec_block_storage.complete = True

            self.log(
                f'Decoded storage {node_storage.storage_module}.{node_storage.storage_name} for #{node_storage.block_number}')

            if codec_block_storage.data and codec_block_storage.storage_key == self.harvester.event_storage_key:

                for event_idx, event_data in enumerate(codec_block_storage.data):

                    event_data['event_index'] = f"0x{event_data['event_index']}"

                    codec_event = CodecBlockEvent(
                        block_hash=codec_block_storage.block_hash,
                        block_number=codec_block_storage.block_number,
                        event_idx=event_idx,
                        scale_type='EventRecord<Event, Hash>',
                        event_module=event_data['module_id'],
                        event_name=event_data['event_id'],
                        extrinsic_idx=event_data['extrinsic_idx'],
                        data=event_data,
                        complete=True
                    )

                    codec_event.save(self.session)

                self.log(f'Decoded events for #{node_storage.block_number}')

        except Exception as e:
            self.log('⚠️  Failed to decode storage "{}.{}" for #{}'.format(
                node_storage.storage_module,
                node_storage.storage_name,
                node_storage.block_number
            ))
            codec_block_storage.complete = False
            codec_block_storage.retry = True

        codec_block_storage.save(self.session)
        self.session.commit()


class EtlProcess(Job):

    icon = '🧭'

    def start(self):
        """
        Fourth step in the harvester: ETL process
        :return:
        """

        start_record = HarvesterStatus.query(self.session).get('PROCESS_ETL')

        end_record = HarvesterStatus.query(self.session).get('PROCESS_DECODER_MAX_BLOCKNUMBER')

        if not start_record:
            start_blocknumber = 0
        else:
            start_blocknumber = (start_record.value or -1) + 1

        end_blocknumber = min(end_record.value or 0, start_blocknumber + 999)

        if end_blocknumber >= start_blocknumber:

            self.log('Start ETL process from #{} to #{}'.format(
                start_blocknumber, end_blocknumber
            ))
            self.session.execute('CALL etl_range({}, {}, 1)'.format(
                start_blocknumber, end_blocknumber
            ))

            # Loop through ETL apps
            for etl_db in settings.INSTALLED_ETL_DATABASES:
                self.session.execute(f'CALL {etl_db}.etl_range({start_blocknumber}, {end_blocknumber}, 1)')
                self.session.commit()

            record = HarvesterStatus.query(self.session).get('PROCESS_ETL')
            self.log('Finished ETL process at #{}'.format(record.value or 0))


class StorageTask(Job):

    icon = '💼'

    def start(self):

        # Retrieve task
        task = HarvesterStorageTask.query(self.session).filter_by(complete=False).order_by('id').first()

        if task:
            self.log(f'Processing "{task.description}"')

            # Loop through block range
            if 'block_ids' in task.blocks:
                block_ids = task.blocks['block_ids']
            elif 'block_start' in task.blocks:
                block_ids = range(task.blocks['block_start'], task.blocks['block_end'] + 1)
            else:
                raise ValueError("Unknown format in block data")

            storage_count = 0

            for block_id in block_ids:
                block_hash = self.substrate.get_block_hash(block_id)

                if block_hash:
                    storage_keys = []
                    if task.storage_key:
                        storage_keys.append(task.storage_key)
                    elif task.storage_key_prefix:
                        # Retrieve storage keys from RPC
                        paged_keys = self.get_next_storage_key_page(
                            task.storage_key_prefix, task.storage_key_prefix, block_hash
                        )
                        while len(paged_keys) > 0:
                            storage_keys += [bytes.fromhex(k[2:]) for k in paged_keys]
                            last_key = bytes.fromhex(paged_keys[-1][2:])
                            paged_keys = self.get_next_storage_key_page(
                                task.storage_key_prefix, last_key, block_hash
                            )

                    for storage_key in storage_keys:

                        storage_response = self.harvester.rpc_call(
                            "state_getStorageAt", [f'0x{storage_key.hex()}', block_hash]
                        )

                        if storage_response.get('result'):
                            storage_data = bytes.fromhex(storage_response.get('result')[2:])
                        else:
                            storage_data = None

                        storage_item = NodeBlockStorage(
                            block_hash=bytes.fromhex(block_hash[2:]),
                            storage_key=storage_key,
                            data=storage_data,
                            block_number=block_id,
                            storage_module=task.storage_pallet,
                            storage_name=task.storage_name,
                            complete=True
                        )
                        try:
                            storage_item.save(self.session)

                            codec_block_storage = CodecBlockStorage(
                                block_hash=storage_item.block_hash,
                                block_number=storage_item.block_number,
                                storage_key=storage_item.storage_key,
                                storage_module=storage_item.storage_module,
                                storage_name=storage_item.storage_name
                            )
                            try:
                                self.decode_storage_item(storage_item, codec_block_storage)
                                self.session.commit()
                                storage_count += 1

                            except Exception as e:
                                self.log(str(e))

                        except IntegrityError:
                            self.log(f'Skipped existing storage key {self.format_hash(storage_key)}')
                            self.session.rollback()
                else:
                    self.log(f'Skipped not existing block #{block_id}')

            self.log(f'Added {storage_count} storage records')
            task.complete = True
            task.save(self.session)
            self.session.commit()

    def get_next_storage_key_page(self, prefix: bytes, start_key: bytes, block_hash: str) -> list:
        response = self.harvester.rpc_call(
            method="state_getKeysPaged", params=[f'0x{prefix.hex()}', 1, f'0x{start_key.hex()}', block_hash]
        )
        return response.get('result') or []

    def decode_storage_item(self, node_storage, codec_block_storage):

        decoded_storage_entry = self.db_substrate.query(
            module=node_storage.storage_module,
            storage_function=node_storage.storage_name,
            raw_storage_key=node_storage.storage_key,
            block_hash=f'0x{node_storage.block_hash.hex()}'
        )
        if decoded_storage_entry:
            codec_block_storage.data = decoded_storage_entry.value
            codec_block_storage.scale_type = decoded_storage_entry.type_string or \
                                             decoded_storage_entry.__class__.__name__

            if codec_block_storage.storage_key == self.harvester.event_storage_key:

                for event_idx, event_data in enumerate(codec_block_storage.data):

                    event_data['event_index'] = f"0x{event_data['event_index']}"

                    codec_event = CodecBlockEvent(
                        block_hash=codec_block_storage.block_hash,
                        block_number=codec_block_storage.block_number,
                        event_idx=event_idx,
                        scale_type='EventRecord<Event, Hash>',
                        event_module=event_data['module_id'],
                        event_name=event_data['event_id'],
                        extrinsic_idx=event_data['extrinsic_idx'],
                        data=event_data,
                        complete=True
                    )

                    codec_event.save(self.session)

                self.log(f'Decoded events for #{node_storage.block_number}')

        codec_block_storage.complete = True
        codec_block_storage.retry = False

        codec_block_storage.save(self.session)
