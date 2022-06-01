import pandas as pd
from telethon.tl.types import PeerUser, PeerChannel, PeerChat

from datetime import datetime
from typing import List, Dict

from telegramanalysor.authentication import create_client

from pathlib import Path

DATA_PATH = Path().cwd().parent / 'data'


def replace_none(text, replacement):
    if text is None:
        return replacement
    else:
        return text


async def download_messages(dialog: str) -> List:
    messages = []
    async with create_client() as client:
        async for message in client.iter_messages(dialog):
            messages.append(message.to_dict())
    return messages


def sort_messages(messages: List) -> (List, List, List):
    messages_messages = []

    actions_add_user = []
    actions_delete_user = []
    actions_chat_joined_by_link = []

    for m in messages:
        if m.get('_') == 'Message':
            # actual messages
            messages_messages.append(m)
        else:
            # others
            actiontype = m.get('action').get('_')
            if actiontype == 'MessageActionChatAddUser':
                actions_add_user.append(m)
            elif actiontype == 'MessageActionChatDeleteUser':
                actions_delete_user.append(m)
            elif actiontype == 'MessageActionChatJoinedByLink':
                actions_chat_joined_by_link.append(m)
            elif actiontype in ['MessageActionChannelMigrateFrom',
                                'MessageActionPinMessage',
                                'MessageActionChatEditPhoto',
                                'MessageActionChatCreate',  # TODO: review what this is and if it should be ignored
                                'MessageActionChatEditTitle']:
                pass
            else:
                raise ValueError(f"Unknown message service action found: {actiontype}")
    return messages_messages, actions_add_user, actions_delete_user, actions_chat_joined_by_link


def extract_group_id(peer_id_dict: Dict):
    if peer_id_dict.get('_') == 'PeerChannel':
        return peer_id_dict.get('channel_id')
    elif peer_id_dict.get('_') == 'PeerChat':
        return peer_id_dict.get('chat_id')


def convert_messages_to_df(messages: List) -> pd.DataFrame:
    df_messages = pd.DataFrame([{'channel_id': extract_group_id(m.get('peer_id')),
                                 'message_id': m.get('id'),
                                 'datetime': m.get('date'),
                                 'user_id': m.get('from_id').get('user_id') if m.get('from_id') is not None else '',
                                 'message': m.get('message')}
                                for m in messages])
    return df_messages


def convert_delete_user_events_to_df(delete_user_events: List) -> pd.DataFrame:
    if len(delete_user_events) == 0:
        return pd.DataFrame(columns=['channel_id', 'message_id', 'action', 'datetime', 'deleted_user_id', 'user_id'])
    else:
        df_delete_user_events = \
            pd.DataFrame([{'channel_id': extract_group_id(m.get('peer_id')),
                           'message_id': m.get('id'),
                           'action': m.get('action').get('_'),
                           'datetime': m.get('date'),
                           'deleted_user_id': m.get('action').get('user_id'),
                           'user_id': m.get('from_id').get('user_id'),
                           }
                          for m in delete_user_events])
    return df_delete_user_events


def convert_add_user_events_to_df(add_user_events: List) -> pd.DataFrame:
    added_users = []

    for m in add_user_events:
        added_user_ids = m.get('action').get('users')
        for added_user_id in added_user_ids:
            added_users.append({'channel_id': extract_group_id(m.get('peer_id')),
                                'message_id': m.get('id'),
                                'action': m.get('action').get('_'),
                                'datetime': m.get('date'),
                                'added_user_id': added_user_id,
                                'user_id': m.get('from_id').get('user_id'),
                                })
    return pd.DataFrame(added_users)


def convert_chat_joined_by_link_events_to_df(chats_joined_by_link_events: List) -> pd.DataFrame:
    if len(chats_joined_by_link_events) == 0:
        return pd.DataFrame(columns=['channel_id', 'message_id', 'action', 'datetime', 'inviter_id', 'user_id'])
    else:
        df_chat_joined_by_link = \
            pd.DataFrame([{'channel_id': m.get('peer_id').get('channel_id'),
                           'message_id': m.get('id'),
                           'action': m.get('action').get('_'),
                           'datetime': m.get('date'),
                           'inviter_id': m.get('action').get('inviter_id'),
                           'user_id': m.get('from_id').get('user_id'),
                           }
                          for m in chats_joined_by_link_events])
        return df_chat_joined_by_link


async def create_user_df(user_ids: List[int]) -> pd.DataFrame:
    users = []
    async with create_client() as client:
        for user_id in user_ids:
            try:
                u = await client.get_entity(PeerUser(int(user_id)))
                users.append(u.to_dict())
            except ValueError:
                print(f"{user_id} not found.")

    df_users = pd.DataFrame([{'user_id': u.get('id'),
                              'username': u.get('username'),
                              'first_name': u.get('first_name'),
                              'last_name': u.get('last_name'),
                              'full_name': ' '.join([replace_none(u.get('first_name'), ''),
                                                     replace_none(u.get('last_name'), '')])}
                             for u in users])
    return df_users


async def create_channel_df(channel_ids: List[int]) -> pd.DataFrame:
    channels = []
    async with create_client() as client:
        for channel_id in channel_ids:
            try:
                u = await client.get_entity(PeerChannel(int(channel_id)))
                channels.append({**u.to_dict(), 'is_supergroup': True})
            except ValueError:
                try:
                    u = await client.get_entity(PeerChat(int(channel_id)))
                    channels.append({**u.to_dict(), 'is_supergroup': False})
                except ValueError:
                    print(f"{channel_id} not found.")

    df_channels = pd.DataFrame([{'channel_id': u.get('id'),
                                 'creation_datetime': u.get('date'),
                                 'title': u.get('title'),
                                 'is_supergroup': u.get('is_supergroup')
                                 } for u in channels])
    return df_channels


async def create_message_datasets(dialogs: List):
    messages_messages = []
    actions_add_user = []
    actions_delete_user = []
    actions_chat_joined_by_link = []

    for dialog in dialogs:
        messages = await download_messages(dialog)

        msgs, add_users, delete_users, chat_joined_by_link = sort_messages(messages)
        messages_messages = messages_messages + msgs
        actions_add_user = actions_add_user + add_users
        actions_delete_user = actions_delete_user + delete_users
        actions_chat_joined_by_link = actions_chat_joined_by_link + chat_joined_by_link

    df_msgs = convert_messages_to_df(messages_messages)
    df_add_user_events = convert_add_user_events_to_df(actions_add_user)
    df_delete_user_events = convert_delete_user_events_to_df(actions_delete_user)
    df_chat_joined_by_link_events = convert_chat_joined_by_link_events_to_df(actions_chat_joined_by_link)

    return {'messages': df_msgs,
            'add_user_events': df_add_user_events,
            'delete_user_events': df_delete_user_events,
            'chat_joined_by_link_events': df_chat_joined_by_link_events}


async def download_participants(dialog: str) -> List:
    participants = []
    async with create_client() as client:
        async for participant in client.iter_participants(dialog):
            participants.append(participant.to_dict())
    return participants


async def generate_participants_df(dialog: str) -> pd.DataFrame:
    # TODO: adapt to also pull from deleted users
    participants = await download_participants(dialog)
    df_users = pd.DataFrame([{'channel_name': dialog,
                              'user_id': u.get('id'),
                              'username': u.get('username'),
                              'first_name': u.get('first_name'),
                              'last_name': u.get('last_name'),
                              'full_name': ' '.join([replace_none(u.get('first_name'), ''),
                                                     replace_none(u.get('last_name'), '')])}
                             for u in participants])
    return df_users


async def save_dataset_to_disk(dialogs):
    loadtime = datetime.now()
    save_folder = DATA_PATH / loadtime.isoformat()
    save_folder.mkdir()

    msg_datasets = await create_message_datasets(dialogs)

    user_ids = set(msg_datasets['messages']['user_id']) \
        .union(set(msg_datasets['add_user_events']['added_user_id'])) \
        .union(set(msg_datasets['add_user_events']['user_id'])) \
        .union(set(msg_datasets['delete_user_events']['user_id'])) \
        .union(set(msg_datasets['chat_joined_by_link_events']['inviter_id'])) \
        .union(set(msg_datasets['chat_joined_by_link_events']['user_id']))

    channel_ids = set(msg_datasets['messages']['channel_id'])

    # in ipython >= 7.0 event loop already running
    # https://stackoverflow.com/questions/55409641/asyncio-run-cannot-be-called-from-a-running-event-loop
    df_users = await create_user_df(list(user_ids))
    df_channels = await create_channel_df(list(channel_ids))

    msg_datasets['messages'].to_csv(save_folder / 'messages.csv')
    msg_datasets['add_user_events'].to_csv(save_folder / 'add_user_events.csv')
    msg_datasets['delete_user_events'].to_csv(save_folder / 'delete_user_events.csv')
    msg_datasets['chat_joined_by_link_events'].to_csv(save_folder / 'chat_joined_by_link_events.csv')

    df_users.to_csv(save_folder / 'users.csv')
    df_channels.to_csv(save_folder / 'channels.csv')

    linebreak = '\n'
    print(f"Successfully saved data from {linebreak}{linebreak.join(dialogs)}{linebreak}to folder {save_folder}.")


def load_dataset_from_disk(dataset, latest=True):
    if latest:
        folder = max(list(DATA_PATH.glob("*")))
        print(f"Loading data from {folder}.")
        return pd.read_csv(folder / f"{dataset}.csv")
    else:
        raise ValueError("Only Latest=True is currently implemented.")
