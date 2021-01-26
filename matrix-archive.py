#!/usr/bin/env python3

from nio import (
    AsyncClient,
    AsyncClientConfig,
    MatrixRoom,
    MessageDirection,
    RedactedEvent,
    RoomEncryptedMedia,
    RoomMessage,
    RoomMessageFormatted,
    RoomMessageMedia,
    RoomMessageImage,
    RoomMessageVideo,
    RoomMessageAudio,
    RoomMessageFile,
    RoomEncryptedImage,
    RoomEncryptedAudio,
    RoomEncryptedVideo,
    RoomEncryptedFile,
    crypto,
    store,
    exceptions
)
from functools import partial
from typing import Union, TextIO, Dict
import os
import sys
import aiofiles
import argparse
import asyncio
import getpass
import itertools
import yaml
from urllib.parse import urlparse
from aiohttp import ClientPayloadError
from print_utils import PrintUtils

DEVICE_NAME = "matrix-archive"


def mkdir(path):
    try:
        os.mkdir(path)
    except FileExistsError:
        pass
    return path


async def create_client() -> AsyncClient:
    homeserver = "https://matrix-client.matrix.org"
    homeserver = input(f"Enter URL of your homeserver: [{homeserver}] ") or homeserver
    user_id = input(f"Enter your full user ID (e.g. @user:matrix.org): ")
    password = getpass.getpass()
    client = AsyncClient(
        homeserver=homeserver,
        user=user_id,
        config=AsyncClientConfig(store=store.SqliteMemoryStore),
    )
    await client.login(password, DEVICE_NAME)
    client.load_store()
    room_keys_path = input("Enter full path to room E2E keys: ")
    room_keys_password = getpass.getpass("Room keys password: ")
    PrintUtils.smart_print("Importing keys. This may take a while...")
    await client.import_keys(room_keys_path, room_keys_password)
    return client


async def select_room(client: AsyncClient) -> MatrixRoom:
    PrintUtils.smart_print("\nList of joined rooms (room id, display name):")
    for room_id, room in client.rooms.items():
        PrintUtils.smart_print(f"{room_id}, {room.display_name}")
    room_id = input(f"Enter room id: ")
    return client.rooms[room_id]


def choose_filename(filename):
    start, ext = os.path.splitext(filename)
    for i in itertools.count(1):
        if not os.path.exists(filename):
            break
        filename = f"{start}({i}){ext}"
    return filename


async def write_event(
    client: AsyncClient, room: MatrixRoom, output_file: TextIO, event: RoomMessage
) -> Dict[str, str]:
    if not args.no_media:
        media_dir = mkdir(f"{OUTPUT_DIR}/{room.display_name}_{room.room_id}_media")

    sender_name = f"<{event.sender}>"
    if event.sender in room.users:
        # If user is still present in room, include current nickname
        sender_name = f"{room.users[event.sender].display_name} {sender_name}"

    in_reply_to = None
    if "m.relates_to" in event.source['content'] and "m.in_reply_to" in event.source['content']['m.relates_to']:
        in_reply_to = event.source['content']['m.relates_to']['m.in_reply_to']['event_id']
    replaces = None
    if "m.relates_to" in event.source['content'] and "rel_type" in event.source['content']['m.relates_to'] and event.source['content']['m.relates_to']['rel_type'] == "m.replace":
        replaces = event.source['content']['m.relates_to']['event_id']

    serialize_event = lambda event_payload: yaml.dump(
        [
            {
                'event_id': event.event_id,
                'sender_id': event.sender,
                'sender_name': sender_name,
                'timestamp': event.server_timestamp,
                **event_payload,
            }
        ]
    )

    failed_download = None
    if isinstance(event, RoomMessageFormatted):
        if in_reply_to is not None:
            await output_file.write(serialize_event({'type': "text", 'body': event.body, 'formatted_body': event.formatted_body, 'format': event.format, 'in_reply_to': in_reply_to}))
        elif replaces is not None:
            new_body = event.source['content']['m.new_content']['body']
            new_format = None
            new_formatted_body = None
            if event.source['content']['m.new_content']['format'] is not None:
                new_format = event.source['content']['m.new_content']['format']
                new_formatted_body = event.source['content']['m.new_content']['formatted_body']

            evnt = {'type': "text", 'body': event.body, 'formatted_body': event.formatted_body, 'format': event.format, 'replaces': replaces, 'new_body': new_body, 'new_format': new_format, 'new_formatted_body': new_formatted_body}
            evnt = {k: v for k, v in evnt.items() if v is not None}
            await output_file.write(serialize_event(evnt))
        else:
            evnt = {'type': "text", 'body': event.body, 'formatted_body': event.formatted_body, 'format': event.format}
            evnt = {k: v for k, v in evnt.items() if v is not None}
            await output_file.write(serialize_event(evnt))
    elif isinstance(event, (RoomMessageMedia, RoomEncryptedMedia)):
        filename = choose_filename(f"{media_dir}/{event.body}")
        if not args.no_media_dl:
            # Sometimes the homeserver does not respond due to load so instead of interrupting the whole backup, just save the non-downloaded links and filenames and save the messages anyway so we can try to re-download the files later
            try:
                media_data = await download_mxc(client, event.url)
                async with aiofiles.open(filename, "wb") as f:
                    try:
                        await f.write(
                            crypto.attachments.decrypt_attachment(
                                media_data,
                                event.source["content"]["file"]["key"]["k"],
                                event.source["content"]["file"]["hashes"]["sha256"],
                                event.source["content"]["file"]["iv"]
                            )
                        )
                    except KeyError:  # EAFP: Unencrypted media produces KeyError
                        await f.write(media_data)
                    # Set atime and mtime of file to event timestamp
                    os.utime(filename, ns=((event.server_timestamp * 1000000,) * 2))
            except ClientPayloadError:
                failed_download = {'url': event.url,
                            'filename': filename,
                            'key': event.source["content"]["file"]["key"]["k"],
                            'hash':event.source["content"]["file"]["hashes"]["sha256"],
                            'iv': event.source["content"]["file"]["iv"],
                            'server_timestamp': event.server_timestamp
                        }

        media_type = ""
        if isinstance(event, (RoomMessageImage, RoomEncryptedImage)):
            media_type = "m.image"
        elif isinstance(event, (RoomMessageAudio, RoomEncryptedAudio)):
            media_type = "m.audio"
        elif isinstance(event, (RoomMessageVideo, RoomEncryptedVideo)):
            media_type = "m.video"
        elif isinstance(event, (RoomMessageFile, RoomEncryptedFile)):
            media_type = "m.file"
        await output_file.write(serialize_event({
            'type': media_type,
            'body': event.body,
            'mimetype': event.source['content']['info']['mimetype'] if "mimetype" in event.source['content']['info'] else None,
            'src': filename
        }))

    elif isinstance(event, RedactedEvent):
        await output_file.write(serialize_event({'type': "redacted"}))

    return failed_download

async def save_avatars(client: AsyncClient, room: MatrixRoom) -> None:
    avatar_dir = mkdir(f"{OUTPUT_DIR}/{room.display_name}_{room.room_id}_avatars")
    for user in room.users.values():
        if user.avatar_url:
            async with aiofiles.open(f"{avatar_dir}/{user.user_id}", "wb") as f:
                await f.write(await download_mxc(client, user.avatar_url))


async def download_mxc(client: AsyncClient, url: str):
    mxc = urlparse(url)
    response = await client.download(mxc.netloc, mxc.path.strip("/"))
    return response.body


def is_valid_event(event):
    events = (RoomMessageFormatted, RedactedEvent)
    if not args.no_media:
        events += (RoomMessageMedia, RoomEncryptedMedia)
    return isinstance(event, events)


async def fetch_room_events(
    client: AsyncClient,
    start_token: str,
    room: MatrixRoom,
    direction: MessageDirection,
) -> list:
    events = []
    while True:
        response = await client.room_messages(
            room.room_id, start_token, limit=1000, direction=direction
        )
        if len(response.chunk) == 0:
            break
        events.extend(event for event in response.chunk if is_valid_event(event))
        start_token = response.end
    return events


async def write_room_events(client, room):
    PrintUtils.smart_print(f"Fetching {room.room_id} room messages...")
    sync_resp = await client.sync(
        full_state=True, sync_filter={"room": {"timeline": {"limit": 1}}}
    )
    start_token = sync_resp.rooms.join[room.room_id].timeline.prev_batch
    # Generally, it should only be necessary to fetch back events but,
    # sometimes depending on the sync, front events need to be fetched
    # as well.
    fetch_room_events_ = partial(fetch_room_events, client, start_token, room)
    async with aiofiles.open(
        f"{OUTPUT_DIR}/{room.display_name}_{room.room_id}.yaml", "w"
    ) as f:
        back_fetched_events = await fetch_room_events_(MessageDirection.back)
        front_fetched_events = await fetch_room_events_(MessageDirection.front)
        fetched_events = [reversed(back_fetched_events), front_fetched_events]

        PrintUtils.smart_print(f"Writing {room.room_id} room messages to disk...")
        progress, event_count = 0, len(back_fetched_events) + len(front_fetched_events)
        failed_downloads = {}
        for events in fetched_events:
            for event in events:
                progress += 1
                PrintUtils.print_progress_bar(progress, event_count, "", "f" if isinstance(event, RoomMessageMedia) else "m")

                try:
                    failed_download = await write_event(client, room, f, event)
                    if failed_download is not None:
                        failed_downloads[event.event_id] =  failed_download
                except exceptions.EncryptionError as e:
                    PrintUtils.smart_print(e, file=sys.stderr)

        if failed_downloads:
            async with aiofiles.open(
                    f"{OUTPUT_DIR}/{room.display_name}_{room.room_id}_faileddownloads.yaml", "w"
            ) as fdf:
                await fdf.write(yaml.dump(failed_downloads))

    await save_avatars(client, room)
    PrintUtils.print_progress_bar(100, 100)
    PrintUtils.smart_print("Successfully wrote all room events to disk.")

async def redownload_failed_files(client, room):
    PrintUtils.smart_print(f"Re-downloading {room.room_id} failed files...")
    delete_file = True
    async with aiofiles.open(
            f"{OUTPUT_DIR}/{room.display_name}_{room.room_id}_faileddownloads.yaml", "r+"
    ) as f:
        failed_downloads = yaml.load(await f.read(), Loader=yaml.FullLoader)
        completed_downloads = {}
        for event_id, failed_download in failed_downloads.items():
            try:
                media_data = await download_mxc(client, failed_download['url'])
                async with aiofiles.open(failed_download['filename'], "wb") as f:
                    try:
                        await f.write(
                            crypto.attachments.decrypt_attachment(
                                media_data,
                                failed_download['key'],
                                failed_download['hash'],
                                failed_download['iv']
                            )
                        )
                    except KeyError:  # EAFP: Unencrypted media produces KeyError
                        await f.write(media_data)
                    # Set atime and mtime of file to event timestamp
                    os.utime(failed_download['filename'], ns=((failed_download['server_timestamp'] * 1000000,) * 2))
                    completed_downloads[event_id] = failed_download
            except ClientPayloadError:
                pass

        for event_id in completed_downloads:
            del failed_downloads[event_id]

        f.truncate(0)
        f.seek(0)
        if failed_downloads:
            f.write(yaml.dump(failed_downloads))
            delete_file = False

    if delete_file:
        os.remove(f"{OUTPUT_DIR}/{room.display_name}_{room.room_id}_faileddownloads.yaml")

async def main() -> None:
    try:
        client = await create_client()
        await client.sync(
            full_state=True,
            # Limit fetch of room events as they will be fetched later
            sync_filter={"room": {"timeline": {"limit": 1}}})
        if args.all_rooms:
            for room in client.rooms.values():
                if args.command == "re-download":
                    await redownload_failed_files(client, room)
                else:
                    await write_room_events(client, room)
        else:
            while True:
                room = await select_room(client)
                if args.command == "re-download":
                    await redownload_failed_files(client, room)
                else:
                    await write_room_events(client, room)
                break
    except KeyboardInterrupt:
        sys.exit(1)
    finally:
        await client.logout()
        await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True
    parser_a = subparsers.add_parser("backup", help = "backup matrix room")
    parser_b = subparsers.add_parser("re-download", help = "try to re-download failed file downloads (does not re-export messages again)")
    parser_a.add_argument("output_dir", default=".", nargs="?",
        help = "directory to store output (optional; defaults to current directory)")
    parser_b.add_argument("output_dir", default=".", nargs="?",
        help = "directory to store output (optional; defaults to current directory)")
    parser_a.add_argument("--no-media", action = "store_true",
        help = "don't download media and don't backup media events/messages")
    parser_a.add_argument("--no-media-dl", action = "store_true",
        help = "don't download media but backup media event/messages")
    parser_a.add_argument("--all-rooms", action = "store_true",
        help = "select all rooms")
    parser_b.add_argument("--all-rooms", action = "store_true",
        help = "select all rooms")
    args = parser.parse_args()
    OUTPUT_DIR = mkdir(args.output_dir)
    asyncio.get_event_loop().run_until_complete(main())
