"""
This file contains a set of helpers for analysing the history of groups of
matrix rooms.
"""
import datetime
from urllib.parse import quote
from collections import defaultdict
from matrix_client.errors import MatrixRequestError

import pandas as pd
import numpy as np


__all__ = ['flatten_dicts', 'filter_events_by_messages', 'print_sorted_value',
           'print_sorted_len', 'get_rooms_in_community', 'events_to_dataframe',
           'get_all_messages_for_room', 'get_all_events']


def get_all_messages_for_room(api, room_id, stop_time=None):
    """
    Use the matrix ``/messages`` API to back-paginate through the whole history
    of a room.

    This will probably not work unless your homeserver has all the events
    locally.
    """
    token = ""
    messages = []
    try:
        token = api.get_room_messages(room_id, token, "b")['end']
    except MatrixRequestError:
        print("Can't get messages for room...")
        return messages

    for i in range(100):
        try:
            m1 = api.get_room_messages(room_id, token, "b", limit=5000)
        except MatrixRequestError:
            break
        token = m1['end']
        # TODO: I am pretty sure this doesn't work
        if stop_time:
            stop_time = int(pd.Timestamp("2019/01/01").to_pydatetime().timestamp()*1000)
            times = [e['origin_server_ts'] for e in m1['chunk']]
            stopping = np.less(times, stop_time).nonzero()[0]
            if len(stopping) > (len(times)/1.1):
                messages += m1['chunk']
                return messages
        messages += m1['chunk']
        if not m1['chunk']:
            break
    return messages


def events_to_dataframe(list_o_json):
    """
    Given a list of json events extract the interesting info into a pandas
    Dataframe.
    """
    extract_keys = ("origin_server_ts", "sender",
                    "event_id", "type", "content")
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


def get_all_events(api, rooms, cache=None, refresh_cache=False, stop_time=None):
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
            cache[key] = events_to_dataframe(get_all_messages_for_room(api, rooms[key], stop_time=stop_time))
            store[key] = cache[key]
        for key in cache.keys() - rooms.keys():
            cache.pop(key)
        store.close()
        return cache
    else:
        messages = {}
        for key, id in rooms.items():
            print(f"fetching events for {key}")
            messages[key] = events_to_dataframe(get_all_messages_for_room(api, id, stop_time=stop_time))
        if refresh_cache:
            with pd.HDFStore(cache) as store:
                for channel, df in messages.items():
                    store.put(channel, df)
        return messages


def get_rooms_in_community(api, communtiy):
    """
    Get a mapping of canonical alias (localpart) to room id for all rooms in a
    communtiy.
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


def get_room_aliases_in_community(api, community):
    rooms = api._send("GET", "/groups/{}/rooms".format(quote(community)))
    ids = {}
    for room in rooms['chunk']:
        ca = room.get('canonical_alias')
        if not ca:
            continue
        name = ca.split(":")[0][1:]
        name = name.replace("-", "_")
        ids[name] = ca
    return ids

def print_sorted_len(adict, reverse=True):
    for k in sorted(adict, key=lambda k: len(adict[k]), reverse=reverse):
        m = adict[k]
        print(f"{k}: {len(m)}")


def print_sorted_value(adict, reverse=True):
    for k in sorted(adict, key=adict.__getitem__, reverse=reverse):
        m = adict[k]
        print(f"{k}: {m}")


def filter_events_by_messages(events):
    """
    Filter events so that only "m.room.message" events are kept.

    events should be a dict of room events as returned by ``get_all_events``.
    """
    
    messages = {k: v[v['type'] == "m.room.message"] for k, v in events.items()}
    messages = {k: v[v['sender'] != "@_neb_github_=40_cadair=3amatrix.org:matrix.org"] for k, v in messages.items()}
    return messages


def flatten_dicts(dicts):
    """
    Flatten all the dicts, but assume there are no key conflicts.
    """
    out = {}
    for adict in dicts.values():
        for key, value in adict.items():
            out[key] = value
    return out


def get_display_names(api, senders, template):
    display_names = []
    for s in senders:
        m = True
        if s == "@Cadair:matrix.org":
            s = "@cadair:cadair.com"
        if ":" not in s:
            s = template.format(s=s)
            m = False
        try:
            dn = api.get_display_name(s)
        except Exception:
            dn = s
        if m:
            dn += "*"
        display_names.append(dn)
    return display_names
