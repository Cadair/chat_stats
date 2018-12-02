"""
This file contains a set of helpers for analysing the history of groups of matrix rooms.
"""
import json
import datetime
from urllib.parse import quote
from collections import defaultdict

import pandas as pd
import matrix_client.api
import matplotlib.pyplot as plt


__all__ = ['print_sorted_value', 'print_sorted_len', 'get_rooms_in_community', 'events_to_dataframe', 'get_all_messages_for_room', 'get_all_events']


def get_all_messages_for_room(room_id):
    """
    Use the matrix ``/messages`` API to back-paginate through the whole history of a room.

    This will probably not work unless your homeserver has all the events locally.
    """
    token = ""
    token = api.get_room_messages(room_id, token, "b")['end']
    messages = []
    for i in range(100):
        m1 = api.get_room_messages(room_id, token, "b", limit=1000)
        token = m1['end']
        messages += m1['chunk']
        if not m1['chunk']:
            break
    return messages


def events_to_dataframe(list_o_json):
    """
    Given a list of json events extract the interesting info into a pandas Dataframe.
    """
    extract_keys = ("origin_server_ts", "sender", "event_id", "type", "content")
    df = defaultdict(list)
    df["body"] = []
    for event in list_o_json:
        if "body" in event['content']:
            df["body"].append(event['content']['body'])
        else:
            df['body'].append(None)
        for k in extract_keys:
            v = event[k]
            df[k].append(v)
    df["origin_server_ts"] = [datetime.datetime.fromtimestamp(ts/1000) for ts in df['origin_server_ts']]
    return pd.DataFrame(df).set_index("origin_server_ts")


def get_all_events(rooms, cache=None, refresh_cache=False):
    """
    Get all events in rooms.

    If cache is a filename it will be loaded with `pandas.HDFStore`,
    if refresh_cache is true then the cache will be saved after
    getting the messages from the server.
    """
    if cache and not refresh_cache:
        store = pd.HDFStore(cache)
        cache = {key[1:]: store.get(key) for key in store.keys()}
        missing_keys = rooms.keys() - cache.keys()
        for key in missing_keys:
            print(f"fetching events for {key}")
            cache[key] = events_to_dataframe(get_all_messages_for_room(rooms[key]))
            store[key] = cache[key]
        for key in cache.keys() - rooms.keys():
            cache.pop(key)
        store.close()
        return cache
    else:
        messages = {}
        for key, id in ids.items():
            print(f"fetching events for {key}")
            messages[key] = events_to_dataframe(get_all_messages_for_room(id))
        if refresh_cache:
            with pd.HDFStore(cache) as store:
                for channel, df in messages.items():
                    store.put(channel, df)
        return messages

def get_rooms_in_community(communtiy):
    """
    Get a mapping of canonical alias (localpart) to room id for all rooms in a communtiy.
    """
    rooms = api._send("GET", "/groups/{}/rooms".format(quote(communtiy)))
    ids = {}
    for room in rooms['chunk']:
        ca = room.get('canonical_alias')
        if not ca:
            continue
        name = ca.split(":")[0][1:]
        name = name.replace("-", "_")
        ids[name] = room['room_id']
    return ids


def print_sorted_len(adict, reverse=True):
    for k in sorted(adict, key=lambda k: len(adict[k]), reverse=reverse):
        m = adict[k]
        print(f"{k}: {len(m)}")

def print_sorted_value(adict, reverse=True):
    for k in sorted(adict, key=adict.__getitem__, reverse=reverse):
        m = adict[k]
        print(f"{k}: {m}")
