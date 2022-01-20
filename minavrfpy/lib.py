import datetime
import hashlib
import json
import subprocess
import time
import urllib.request
from functools import lru_cache
from pathlib import Path

import base58
import numpy as np
import orjson
import pandas as pd
from MinaClient import Client
from splitstream import splitfile

from queries import get_epoch_query, get_stakers_query

SLOTS_PER_EPOCH = 7140
MINA_EXPLORER_ENDPOINT = "https://graphql.minaexplorer.com/"

ledger_downlod_location = (
    "https://raw.githubusercontent.com/zkvalidator/mina-graphql-rs/master/data/epochs"
)


# outcomes
WON = "WON"
LOST = "LOST"

MISSED_TOO_LATE = "MISSED_TOO_LATE"
MISSED_HEIGHT_DIFF = "MISSED_HEIGHT_DIFF"
MISSED_NOT_PRODUCED = "MISSED_NOT_PRODUCED"

FUTURE = "FUTURE"  # not yet time
DIDNT_HAPPEN = "DIDNT_HAPPEN"  # this case can never happen - need to check again...


# @lru_cache(maxsize=5)
def get_stakes_df(delegate, ledger_hash, mina_explorer_client):
    op = get_stakers_query(delegate, ledger_hash)
    res = mina_explorer_client.send_any_query(op)
    stakes_df = pd.json_normalize(res["data"]["stakes"], sep="_")
    stakes_df.timing_timed_weighting.fillna(1, inplace=True)
    return stakes_df


def read_check_witness(epoch, block_producer_key, path, only_threshold_met=True):
    if not path:
        path = (
            Path.cwd()
            / "vrf_checked"
            / f"{block_producer_key}"
            / f"check-epoch-{epoch}"
        )

    f = open(path, "r")
    data = []
    for each in splitfile(f, format="json"):
        d = orjson.loads(each)
        if only_threshold_met and not d["thresholdMet"]:
            continue
        data.append(d)

    check_witness_df = pd.json_normalize(data, sep="_")

    check_witness_df.message_globalSlot = check_witness_df.message_globalSlot.astype(
        int
    )
    return check_witness_df


def get_winner_df(epoch, mina_explorer_client):
    op = get_epoch_query(epoch=epoch)
    res = mina_explorer_client.send_any_query(op)
    winner_df = pd.json_normalize(res["data"]["blocks"], sep="_")
    winner_df["dateTime"] = pd.to_datetime(winner_df["dateTime"])
    winner_df["receivedTime"] = pd.to_datetime(winner_df["receivedTime"])
    winner_df["time_diff"] = winner_df.receivedTime - winner_df.dateTime
    return winner_df


def get_my_winner_df(epoch, pk, mina_explorer_client):
    op = get_epoch_query(epoch=epoch, creator=pk)
    res = mina_explorer_client.send_any_query(op)
    my_winner_df = pd.json_normalize(res["data"]["blocks"], sep="_")
    my_winner_df["dateTime"] = pd.to_datetime(my_winner_df["dateTime"])
    my_winner_df["receivedTime"] = pd.to_datetime(my_winner_df["receivedTime"])
    my_winner_df["time_diff"] = my_winner_df.receivedTime - my_winner_df.dateTime
    return my_winner_df


def is_ok_block_time(td, minutes=3):
    return datetime.timedelta(minutes=minutes) > td


def decode_vrf_output(vrf_output):
    v = hashlib.blake2b(digest_size=32)
    v.update(bytes(list(base58.b58decode_check(vrf_output))[3:35]))
    return list(v.digest())


def get_vrf_comp(slot, winner_vrf, our_vrf):
    our_digest = decode_vrf_output(our_vrf)
    winner_digest = decode_vrf_output(winner_vrf)

    for x, y in zip(our_digest, winner_digest):
        if x > y:
            return True
        else:
            return False
    return False


def get_ledger_df(ledger_hash):
    ledger_path = Path.cwd() / "ledgers" / f"{ledger_hash}.json"
    if not ledger_path.exists():
        print(f"downloading ledger {ledger_hash}...")
        urllib.request.urlretrieve(
            f"{ledger_downlod_location}/{ledger_hash}.json", ledger_path
        )
    ledger_df = pd.io.json.read_json(ledger_path)
    return ledger_df


def get_epoch_df(epoch, block_producer_key, mina_explorer_client=None):
    start_time = time.monotonic()

    if not mina_explorer_client:
        mina_explorer_client = Client(endpoint=MINA_EXPLORER_ENDPOINT)

    winner_df = get_winner_df(epoch, mina_explorer_client)

    # extract all form winner_df awkwardly
    epoch_data = {
        "ledger_hash": winner_df.protocolState_consensusState_stakingEpochData_ledger_hash.unique()[
            0
        ],
        "total_currency": winner_df.protocolState_consensusState_stakingEpochData_ledger_totalCurrency.unique()[
            0
        ]
        / 1e9,
        "last_slot": winner_df.tail(n=1).protocolState_consensusState_slot.item(),
        "last_gloabl_slot": winner_df.tail(
            n=1
        ).protocolState_consensusState_slotSinceGenesis.item(),
    }

    # read ledger
    ledger_df = get_ledger_df(epoch_data["ledger_hash"])
    delegator_index_to_pk_dict = ledger_df[ledger_df.delegate == block_producer_key][
        ["pk"]
    ].to_dict()["pk"]

    my_winner_df = get_my_winner_df(epoch, block_producer_key, mina_explorer_client)

    # stakers
    stakes_df = get_stakes_df(
        block_producer_key,
        epoch_data["ledger_hash"],
        mina_explorer_client=mina_explorer_client,
    )

    winner_slot_to_vrf = dict(
        zip(
            winner_df.protocolState_consensusState_slotSinceGenesis,
            winner_df.protocolState_consensusState_lastVrfOutput,
        )
    )

    winner_slot_to_block_height = dict(
        zip(
            winner_df.protocolState_consensusState_slotSinceGenesis,
            winner_df.blockHeight,
        )
    )

    threshold_met_df = read_check_witness(epoch, block_producer_key, path=None)

    my_slots_to_winners = winner_df[
        winner_df.protocolState_consensusState_slotSinceGenesis.isin(
            threshold_met_df.message_globalSlot.values
        )
    ]

    global_slots = threshold_met_df.drop_duplicates(
        subset=["publicKey", "message_globalSlot"]
    ).message_globalSlot.values

    slot_to_delegator_index = dict(
        zip(
            threshold_met_df.message_globalSlot,
            threshold_met_df.message_delegatorIndex,
        )
    )

    my_winner_slot_to_vrf = dict(
        zip(threshold_met_df.message_globalSlot, threshold_met_df.vrfOutput)
    )

    my_block_times = (
        my_winner_df.groupby("protocolState_consensusState_slotSinceGenesis")
        .time_diff.apply(list)
        .to_dict()
    )

    slot_to_received_time = dict(
        zip(
            winner_df.protocolState_consensusState_slotSinceGenesis,
            winner_df.receivedTime,
        )
    )

    my_slots_to_winners_dict = dict(
        zip(
            my_slots_to_winners.protocolState_consensusState_slotSinceGenesis,
            my_slots_to_winners.creator,
        )
    )

    df_data = []

    winner_slots = winner_df.protocolState_consensusState_slotSinceGenesis.values
    my_winner_slots = my_winner_df.protocolState_consensusState_slotSinceGenesis.values

    for global_slot in global_slots:

        bp_won = my_slots_to_winners_dict.get(global_slot) == block_producer_key
        winner_pk = delegator_index_to_pk_dict[slot_to_delegator_index[global_slot]]

        df_data.append(
            {
                "epoch": epoch,
                "slot": global_slot - (epoch * SLOTS_PER_EPOCH),
                "global_slot": global_slot,
                "block": winner_slot_to_block_height.get(global_slot, False),
                "winner_exists": global_slot in winner_slots,
                "saw_my_producer": global_slot in my_winner_slots,
                "bp_won": bp_won,
                "block_time": my_block_times.get(global_slot, []),
                "received_time": slot_to_received_time.get(global_slot),
                "winner_pk": winner_pk,
                "super_charged": winner_pk
                in stakes_df[stakes_df.timing_timed_weighting == 1.0].public_key.values,
            }
        )

    df = pd.DataFrame(df_data)

    def get_is_bh_equal(row):
        if not row["saw_my_producer"]:
            return False
        if not row["winner_exists"]:
            return False

        global_slot = row["global_slot"]

        block_height_equal = (
            winner_df[
                winner_df.protocolState_consensusState_slotSinceGenesis == global_slot
            ].blockHeight.values[0]
            == my_winner_df[
                my_winner_df.protocolState_consensusState_slotSinceGenesis
                == global_slot
            ].blockHeight.values[0]
        )
        return block_height_equal

    def is_too_late(row, max_slot):
        if row["slot"] > max_slot:
            return False
        return not any(
            list(map(is_ok_block_time, my_block_times.get(row["global_slot"], [])))
        )

    df["block_height_equal"] = df.apply(lambda row: get_is_bh_equal(row), axis=1)

    max_slot = winner_df.protocolState_consensusState_slotSinceGenesis.max()

    df["too_late"] = df.apply(lambda row: is_too_late(row, max_slot), axis=1)

    df["min_bt"] = df.block_time.apply(lambda x: min(x) if x else pd.Timedelta(0))
    df["min_bt_minutes"] = df["min_bt"].apply(lambda x: x.seconds / 60)

    def get_outcome(row):
        global_slot = row["global_slot"]

        if row["bp_won"]:
            return WON

        if not row["winner_exists"]:
            if not row["saw_my_producer"]:
                if global_slot > max_slot:
                    return FUTURE
                else:
                    return DIDNT_HAPPEN
            else:
                if row["too_late"]:
                    return MISSED_TOO_LATE
                else:
                    return LOST

        winner_slot_vrf = winner_slot_to_vrf[global_slot]
        my_winner_slot_vrf = my_winner_slot_to_vrf[global_slot]
        comp = get_vrf_comp(global_slot, winner_slot_vrf, my_winner_slot_vrf)

        if row["saw_my_producer"]:
            if row["block_height_equal"]:
                if comp:
                    if row["too_late"]:
                        return MISSED_TOO_LATE
                    else:
                        return LOST
                else:
                    return LOST
            else:
                return MISSED_HEIGHT_DIFF
        else:
            if comp:
                return MISSED_NOT_PRODUCED
            else:
                return LOST

    df["outcome"] = df.apply(lambda row: get_outcome(row), axis=1)

    df["next_block_in"] = df.apply(
        lambda row: pd.Timedelta((row.slot - epoch_data["last_slot"]) * 3, unit="min")
        if row.outcome == FUTURE
        else np.nan,
        axis=1,
    )
    end_time = time.monotonic()
    total_time = end_time - start_time

    return {
        "df": df,
        "total_time": total_time,
        "stakes_df": stakes_df,
        "winner_df": winner_df,
        "my_winner_df": my_winner_df,
        "epoch_data": epoch_data,
    }

