from mina_schemas import mina_explorer_schema
from sgqlc.operation import Operation

SLOTS_PER_EPOCH = 7140


def get_stakers_query(delegate, ledger_hash, staker_limit=5000):
    op = Operation(mina_explorer_schema.Query)

    query = mina_explorer_schema.StakeQueryInput(
        delegate=delegate, ledger_hash=ledger_hash
    )

    stakes = op.stakes(query=query, limit=staker_limit)
    stakes.public_key()
    stakes.balance()
    stakes.timing.timed_weighting()
    return op


def get_epoch_query(epoch, limit=SLOTS_PER_EPOCH, only_canonicals=True, creator=None):
    consensus_state = mina_explorer_schema.BlockProtocolStateConsensusStateQueryInput(
        epoch=epoch
    )
    protocol_state = mina_explorer_schema.BlockProtocolStateQueryInput(
        consensus_state=consensus_state,
    )

    op = Operation(mina_explorer_schema.Query)

    if creator:
        query = mina_explorer_schema.BlockQueryInput(
            protocol_state=protocol_state, creator=creator
        )
    else:
        query = mina_explorer_schema.BlockQueryInput(
            protocol_state=protocol_state, canonical=only_canonicals
        )

    blocks = op.blocks(query=query, limit=limit)
    blocks.block_height()
    blocks.creator()
    blocks.date_time()
    blocks.received_time()
    blocks.canonical()
    blocks.protocol_state.consensus_state.epoch()
    blocks.protocol_state.consensus_state.slot()
    blocks.protocol_state.consensus_state.last_vrf_output()
    blocks.protocol_state.consensus_state.slot_since_genesis()
    blocks.protocol_state.consensus_state.staking_epoch_data.ledger.hash()
    blocks.protocol_state.consensus_state.staking_epoch_data.ledger.total_currency()
    return op
