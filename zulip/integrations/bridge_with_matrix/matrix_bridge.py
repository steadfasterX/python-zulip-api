#!/usr/bin/env python3
import argparse
import asyncio
import configparser
import logging
import os
import re
import signal
import sys
import traceback
import urllib.request
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import Any, ClassVar, Dict, List, Match, Optional, Set, Tuple, Type, Union
import json
import nio
import subprocess
from nio.responses import (
    DownloadError,
    DownloadResponse,
    ErrorResponse,
    JoinError,
    JoinResponse,
    LoginError,
    LoginResponse,
    Response,
    SyncError,
    SyncResponse,
)

import zulip

# change these templates to change the format of displayed message
ZULIP_MESSAGE_TEMPLATE: str = "**{username}** [{uid}]: {message}"
#MATRIX_MESSAGE_TEMPLATE = "#{topic}# | @**{username}** : {message}"
MATRIX_MESSAGE_TEMPLATE = "@{username} wrote:\n{message}"
MATRIX_NOTICE_TEMPLATE = "****Bridge notice****\n{message}"

# required to talk with the bot in a bridged Matrix room:
matrix_bot_prefix = '!zm'
matrix_bot_prefix_len = len(matrix_bot_prefix)
matrix_bot_max_cmd_len = matrix_bot_prefix_len + 10

# how often should the bridge restart in case of an internal error
# increasing this can cause serious issues like re-creating threads and/or topics
bridge_restart_max_retry = 1
# delay before the bridge will be restarted in case of an error 
bridge_restart_delay = 2

# logging level
# INFO, DEBUG, WARNING, ERROR
loglevel = "INFO"
#loglevel = "DEBUG"

import logging

logging.basicConfig(filename="zulip_matrix.log",
                            filemode='a',
                            format='%(asctime)s %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=loglevel
                            )

logging.info("starting bridge...")

class BridgeConfigError(Exception):
    pass


class BridgeFatalMatrixError(Exception):
    pass


class BridgeFatalZulipError(Exception):
    pass


class MatrixToZulip:
    """
    Matrix -> Zulip
    """

    non_formatted_messages: ClassVar[Dict[Type[nio.Event], str]] = {
        nio.StickerEvent: "sticker",
    }

    def __init__(
        self,
        matrix_client: nio.AsyncClient,
        matrix_config: Dict[str, Any],
        zulip_client: zulip.Client,
        zulip_config: Dict[str, Any],
        no_noise: bool,
    ) -> None:
        self.matrix_client: nio.AsyncClient = matrix_client
        self.matrix_config: Dict[str, Any] = matrix_config
        self.zulip_client: zulip.Client = zulip_client
        self.no_noise: bool = no_noise
        self.zulip_config: Dict[str, Any] = zulip_config

    @classmethod
    async def create(
        cls,
        matrix_client: nio.AsyncClient,
        matrix_config: Dict[str, Any],
        zulip_client: zulip.Client,
        zulip_config: Dict[str, Any],
        no_noise: bool,
    ) -> "MatrixToZulip":
        matrix_to_zulip: "MatrixToZulip" = cls(matrix_client, matrix_config, zulip_client, zulip_config, no_noise)

        # Login to Matrix
        await matrix_to_zulip.matrix_login()
        await matrix_to_zulip.matrix_join_rooms()

        # Do an initial sync of the matrix client in order to continue with
        # the new messages and not with all the old ones.
        sync_response: Union[SyncResponse, SyncError] = await matrix_client.sync()
        if isinstance(sync_response, nio.SyncError):
            raise BridgeFatalMatrixError(sync_response.message)

        return matrix_to_zulip

    async def _get_matrix_threads(self, room_id):
        """
        TODO: DEDUP with ZulipToMatrix!
        check threads
        -> room_get_threads(access_token, room_id, include=ThreadInclusion.all, paginate_from=None, limit=None)
        """
        threads = []

        async for gth in self.matrix_client.room_get_threads(room_id=room_id, limit=None):
            if isinstance(gth, nio.RoomSendError):
                raise BridgeFatalMatrixError(str(gth))
            threads.append(gth)
            logging.debug("_get_matrix_threads: found matrix thread: %s", gth)

        return threads

    async def _matrix_to_zulip(self, room: nio.MatrixRoom, event: nio.Event) -> None:
        logging.debug("_matrix_to_zulip; room %s, event: %s", room.room_id, event)

        # We do this to identify the messages generated from Zulip -> Matrix
        # and we make sure we don't forward it again to the Zulip stream.
        if event.sender == self.matrix_config["mxid"]:
            return

        if room.room_id not in self.matrix_config["bridges"]:
            logging.error("room %s not in bridges", room.room_id)
            return
        stream, topic = self.matrix_config["bridges"][room.room_id]

        raw_content: Optional[str] = await self.get_message_content_from_event(event, room)
        if not raw_content:
            return

        # TODO: handle redaction, edits properly instead of ignoring
        if not hasattr(event, 'body'):
            logging.warning("unhandled event: >%s<", event)
            return

        for s in self.zulip_config:
            if s != "api_key":
                v = self.zulip_config[s]
                logging.debug("parsed zulip_config: %s=%s", s, v)
            else:
                logging.debug("parsed zulip_config: %s=REDACTED", s)

        logging.debug("ROOM: %s", vars(room))

        # get available Matrix threads
        threads_dict = {}
        threads_linked = {}
        m = await self._get_matrix_threads(room.room_id)
        for mxthreads in m:
            if 'body' in mxthreads.source['content']:
                threadstart_body = mxthreads.source['content']['body']
                thread_name = matrix_validate_topic(threadstart_body)
                thread_event_id = mxthreads.event_id

                if thread_name:
                    threads_dict[thread_name] = thread_event_id
                else:
                    logging.warning("threadstart_body is not in expected format: %s, %s", threadstart_body, thread_event_id)

                threads_flat = json.dumps(mxthreads.source['unsigned'])

                logging.debug("threads source is: %s", mxthreads.source)
                logging.debug("current event id: %s", event.event_id)
                    
                if threads_flat and (event.event_id in threads_flat
                                     and 'unsigned' in mxthreads.source
                                     and 'm.relations' in mxthreads.source['unsigned']):
                    in_thread = True
                    logging.debug("we found a matching event id: %s", event.event_id)
                    logging.debug("we are in a matrix thread with zulip topic: %s and id: %s", thread_name, thread_event_id)
                    threads_dict[thread_name] = thread_event_id

        # get available Zulip topics
        s_id = self.zulip_client.get_stream_id(stream)
        zp_topics = self.zulip_client.get_stream_topics(s_id["stream_id"])
        zulip_topics = zp_topics['topics']
        logging.debug("topic handling. stream name: %s, stream_id: %s", stream, s_id["stream_id"])
        logging.debug("topics found: %s", zulip_topics)

        # check bot cmd
        botcmd = False
        if hasattr(event, 'body') and event.body:
            botcmd = matrix_validate_cmd(event.body[:matrix_bot_prefix_len])
            if botcmd:
                logging.warning("BOT CMD detected! -> %s", event.body[:matrix_bot_prefix_len])
                if event.body[matrix_bot_prefix_len:matrix_bot_max_cmd_len] == " topics":
                    # parse topics, threads and create clickable links if possible
                    tl = find_topics_threads(room.room_id, zulip_topics, threads_dict, "threadlinks")
                    threads_linked = tl.get("threads_linked", {})
                    threads_unlinked = tl.get("threads_unlinked", {})
                    linked_topics = ", ".join(threads_linked.values())
                    unlinked_topics = "<br/>".join(threads_unlinked.values())

                    msg_notice = f"""<b>Zulip topics and their corresponding Matrix threads!</b><br/>
                    Click on a topic name <code>#name#</code> and continue a discussion there:<br/><br/>{linked_topics}
                    <br/><br/>
                    The following topics do not exist as a Matrix thread yet - but on Zulip.<br/>
                    Use the given command to create a matching Matrix thread:<br/><br/>
                    <pre>{unlinked_topics}</pre>
                    """
                else:
                    msg_notice = matrix_bot_cmd(event.body[matrix_bot_prefix_len:matrix_bot_max_cmd_len])

                await self.matrix_client.room_send(
                            room.room_id,
                            message_type="m.room.message",
                            content={"msgtype": "m.notice", "body": msg_notice, "format": "org.matrix.custom.html", "formatted_body": msg_notice },
                            )
                return

        # check for (matching) Matrix threads
        # https://spec.matrix.org/unstable/client-server-api/#threading
        # https://spec.matrix.org/unstable/client-server-api/#validation-of-mthread-relationships
        # TODO: DEDUP with ZulipToMatrix!
        thread_event_id = None
        matching_event_ids = []
        in_thread = False
        ztopic = None

        logging.debug("ALL avail threads: %s", m)
        for message in m:
            if 'body' in message.source['content']:
                threadstart_body = message.source['content']['body']
                message_flat = json.dumps(message.source['unsigned'])
                ztopic = matrix_validate_topic(threadstart_body)
                thread_event_id = message.event_id

                if message_flat and (event.event_id in message_flat
                                     and 'unsigned' in message.source
                                     and 'm.relations' in message.source['unsigned']):
                    in_thread = True
                    logging.debug("we found a matching event id: %s", event.event_id)
                    logging.debug("we are in a matrix thread with zulip topic: %s and id: %s", ztopic, thread_event_id)
                    break   # FIXME: this will break only from the if - not the for loop!
                else:
                    # if we are NOT in a thread
                    logging.warning("No matching event id (%s), i.e. we are not in a Matrix thread.", event.event_id)

                    # if user specified a topic via #topic# check if there is an existing one already
                    topic_msg = matrix_validate_topic(event.body)
                    tl = find_topics_threads(room.room_id, zulip_topics, threads_dict, "threadids")
                    threads_linked = tl.get("threads_linked", {})
                    tid = tl.get("threads_ids", {})
                    if topic_msg and topic_msg in tid:
                        thread_link = threads_linked[topic_msg]
                        logging.warning("Matrix thread exists for Zulip topic: %s -> %s -> %s", topic_msg, thread_event_id, thread_link)
                        if thread_link:
                            msg_notice = f"""<b>YOUR MESSAGE WAS NOT BRIDGED TO ZULIP!</b><br/>
                            Reason: <i>There is an existing Matrix thread for your topic!</i><br/>
                            <br/>
                            {thread_link} 👈️ click to open the related Matrix thread
                            """
                        else:
                            msg_notice = f"""<b>YOUR MESSAGE WAS NOT BRIDGED TO ZULIP!</b><br/>
                            Reason: <i>There is an existing Matrix thread for your topic!</i><br/>
                            Due to an internal error we cannot find it though.. try <code>{matrix_bot_prefix} topics</code>
                            """
                        await self.matrix_client.room_send(
                              room.room_id,
                              message_type="m.room.message",
                              content={"msgtype": "m.notice", "body": msg_notice, "format": "org.matrix.custom.html", "formatted_body": msg_notice},
                              )
                        return
                    else:
                        logging.warning("no match found for: %s", ztopic)
            else:
                logging.debug("no body in source[content]: %s", message.source)

        # check topic next
        try:
            if in_thread:
                subject = ztopic
                content = raw_content
            else:
                subject = matrix_validate_topic(event.body)
                if self.zulip_config["enforce_topic_per_room"] == "true" and subject == None:
                    # if set: do not allow setting custom topics:
                    subject = topic
                    content = raw_content
                    logging.warning("enforce_topic_per_room is >%s<! no custom topic allowed, using: %s", self.zulip_config["enforce_topic_per_room"], subject)
                elif self.zulip_config["enforce_topic_per_message"] == "true" and subject == None:
                    # if set: enforce users to SET a topic per message:
                    terr = "enforce_topic_per_message is enforced but no custom topic set!"
                    logging.warning("%s", terr)
                    raise ValueError(terr)
                else:
                    if subject == None:
                        # set the default topic if empty
                        subject = topic
                        content = raw_content
                        logging.info("missing zulip topic, using default: %s", subject)
                    else:
                        # set the given topic if defined
                        subject = subject
                        logging.info("zulip topic found from message: %s", subject)
                        content = re.sub("#"+subject+"#","",raw_content)
                        logging.warning("no matching thread found, creating...")
                        msg_notice = "#" + subject + "#\nNew Matrix THREAD<->TOPIC created!\n<i>Note: Your client must support threading to view it properly!</i><br/>Only use this thread for this topic from now on."
                        msg_notice_html = "<b>#" + subject + "#</b><br/>New Matrix THREAD<->TOPIC created!<br/><i>Note: Your client must support threading to view it properly!</i><br/>Only use this thread for this topic from now on."
                        created_thread = await self.matrix_client.room_send(
                            room.room_id,
                            message_type="m.room.message",
                            content={"msgtype": "m.text", "body": msg_notice, "format": "org.matrix.custom.html", "formatted_body": msg_notice_html},
                        )
                        await self.matrix_client.room_send(
                            room.room_id,
                            message_type="m.room.message",
                            content={"m.relates_to":{"rel_type": "m.thread","event_id": created_thread.event_id}, "msgtype": "m.text", "body": content}
                        )
        except ValueError as err:
            msg_notice = f"""<b>YOUR MESSAGE WAS NOT BRIDGED TO ZULIP!</b><br/>
            Reason: <i>no Zulip topic given!</i><br/>
            <br/>
            Zulip uses a stream+topic concept which translates in Matrix as room+thread.<br/>
            This room has been configured to FORCE following this concept strictly<br/>
            so you have to use topics (i.e. Matrix threads) in order to send data between Matrix and Zulip.<br/>
            <br/>
            How to start?<br/>
            1. use the command <code>{matrix_bot_prefix} topics</code> which lists available topics/threads<br/>
            2. click a matching topic/thread to open the corresponding Matrix thread<br/>
            3. write as usual within that thread<br/>
            <br/>
            ONLY if there is no matching topic/thread you can easily create a new one by:<br/>
            <code>#any-topic-name-you-like# your message</code><br/><br/>
            Example:<br/>
            <code>#howto install twrp# hi guys, I want to install TWRP but no idea where to start..</code><br/>
            <br/>
            this will create a new Zulip topic named <code>"howto install twrp"</code> and starts a new Matrix thread named <code>#howto install twrp#</code> where all related communication for this discussion should go now.
            """
            await self.matrix_client.room_send(
                        room.room_id,
                        message_type="m.room.message",
                        content={"msgtype": "m.notice", "body": msg_notice, "format": "org.matrix.custom.html", "formatted_body": msg_notice},
                        )
            content = ""
            return self._matrix_to_zulip

        # we do not want to see the zulip bot in quotes
        content = re.sub('<' + self.matrix_config["mxid"] + '>','', content)
        try:
            result: Dict[str, Any] = self.zulip_client.send_message(
                {
                    "type": "stream",
                    "to": stream,
                    "subject": subject,
                    "content": content,
                }
            )
        except Exception as exception:
            # Generally raised when user is forbidden
            raise BridgeFatalZulipError(exception) from exception
        if result["result"] != "success":
            # Generally raised when API key is invalid
            raise BridgeFatalZulipError(result["msg"])

        # Update the bot's read marker in order to show the other users which
        # messages are already processed by the bot.
        await self.matrix_client.room_read_markers(
            room.room_id, fully_read_event=event.event_id, read_event=event.event_id
        )

    async def get_message_content_from_event(
        self,
        event: nio.Event,
        room: nio.MatrixRoom,
    ) -> Optional[str]:
        message: str
        sender: Optional[str] = room.user_name(event.sender)

        if isinstance(event, nio.RoomMemberEvent):
            if self.no_noise:
                return None
            # Join and leave events can be noisy. They are ignored by default.
            # To enable these events pass `no_noise` as `False` as the script argument
            message = event.state_key + " " + event.membership
        elif isinstance(event, nio.RoomMessageFormatted):
            message = event.body
        elif isinstance(event, nio.RoomMessageMedia):
            message = await self.handle_media(event)
        elif type(event) in self.non_formatted_messages:
            message = "sends " + self.non_formatted_messages[type(event)]
        elif isinstance(event, nio.MegolmEvent):
            message = "sends an encrypted message"
        elif isinstance(event, nio.UnknownEvent) and event.type == "m.reaction":
            return None
        else:
            message = "event: " + type(event).__name__

        return ZULIP_MESSAGE_TEMPLATE.format(username=sender, uid=event.sender, message=message)

    async def handle_media(self, event: nio.RoomMessageMedia) -> str:
        """Parse a nio.RoomMessageMedia event.

        Upload the media to zulip and build an appropriate message.
        """
        # Split the mxc uri in "server_name" and "media_id".
        mxc_match: Optional[Match[str]] = re.fullmatch("mxc://([^/]+)/([^/]+)", event.url)
        if mxc_match is None or len(mxc_match.groups()) != 2:
            return "[message from bridge: media could not be handled]"
        server_name, media_id = mxc_match.groups()

        download: Union[DownloadResponse, DownloadError] = await self.matrix_client.download(
            server_name, media_id
        )
        if isinstance(download, nio.DownloadError):
            return "[message from bridge: media could not be downloaded]"

        file_fake: BytesIO = BytesIO(download.body)
        # zulip.client.do_api_query() needs a name. TODO: hacky...
        file_fake.name = download.filename

        result: Dict[str, Any] = self.zulip_client.upload_file(file_fake)
        if result["result"] != "success":
            return "[message from bridge: media could not be uploaded]"

        message: str
        if download.filename:
            message = "[{}]({})".format(download.filename, result["uri"])
        else:
            message = result["uri"]

        return message

    async def matrix_join_rooms(self) -> None:
        for room_id in self.matrix_config["bridges"]:
            result: Union[JoinResponse, JoinError] = await self.matrix_client.join(room_id)
            if isinstance(result, nio.JoinError):
                raise BridgeFatalMatrixError(str(result))

    async def matrix_login(self) -> None:
        result: Union[LoginResponse, LoginError] = await self.matrix_client.login(
            self.matrix_config["password"]
        )
        if isinstance(result, nio.LoginError):
            raise BridgeFatalMatrixError(str(result))

    async def run(self) -> None:
        print("Starting message handler on Matrix client")

        # Set up event callback.
        self.matrix_client.add_event_callback(self._matrix_to_zulip, nio.Event)

        await self.matrix_client.sync_forever(timeout=30)

class ZulipToMatrix:
    """
    Zulip -> Matrix
    """

    def __init__(
        self,
        zulip_client: zulip.Client,
        zulip_config: Dict[str, Any],
        matrix_config: Dict[str, Any],
        matrix_client: nio.AsyncClient,
    ) -> None:
        self.zulip_client: zulip.Client = zulip_client
        self.zulip_config: Dict[str, Any] = zulip_config
        self.matrix_config: Dict[str, Any] = matrix_config
        self.matrix_client: nio.AsyncClient = matrix_client
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        # Precompute the url of the Zulip server, needed later.
        result: Dict[str, Any] = self.zulip_client.get_server_settings()
        if result["result"] != "success":
            raise BridgeFatalZulipError("cannot get server settings")
        self.server_url: str = result["realm_uri"]
        if not self.server_url.startswith(("http:", "https:")):
            raise ValueError("Unexpected server URL scheme")

    @classmethod
    async def create(
        cls,
        zulip_client: zulip.Client,
        zulip_config: Dict[str, Any],
        matrix_config: Dict[str, Any],
        matrix_client: nio.AsyncClient,
    ) -> "ZulipToMatrix":
        zulip_to_matrix: "ZulipToMatrix" = cls(zulip_client, zulip_config, matrix_config, matrix_client)
        zulip_to_matrix.ensure_stream_membership()
        return zulip_to_matrix

    def _matrix_send(self, **kwargs: Any) -> None:
        """Wrapper for sending messages to the matrix server."""
        result: Union[Response, ErrorResponse] = asyncio.run_coroutine_threadsafe(
            self.matrix_client.room_send(**kwargs), self.loop
        ).result()
        if isinstance(result, nio.RoomSendError):
            raise BridgeFatalMatrixError(str(result))
        else:
            # Check if the response contains an event_id and return it
            if hasattr(result, 'event_id'):
                logging.debug("_matrix_send; response was: %s", result)
                return result.event_id
            else:
                logging.debug("_matrix_send; no event_id in response: %s", result)
                return None

    async def _get_matrix_threads(self, room_id):
        """
        check threads
        -> room_get_threads(access_token, room_id, include=ThreadInclusion.all, paginate_from=None, limit=None)
        """
        threads = []

        async for gth in self.matrix_client.room_get_threads(room_id=room_id, limit=None):
            if isinstance(gth, nio.RoomSendError):
                raise BridgeFatalMatrixError(str(gth))
            threads.append(gth)
            logging.debug("_get_matrix_threads; found matrix thread: %s", gth)

        return threads

    def _zulip_to_matrix(self, msg: Dict[str, Any]) -> None:
        logging.debug("_zulip_to_matrix; msg: %s", msg)

        room_id: Optional[str] = self.get_matrix_room_for_zulip_message(msg)
        if room_id is None:
            logging.debug("_zulip_to_matrix; room_id is None!")
            return

        sender: str = msg["sender_full_name"]
        content: str = MATRIX_MESSAGE_TEMPLATE.format(
            topic=msg["subject"], username=sender, uid=msg["sender_id"], message=msg["content"]
            )
        
        # check for (matching) Matrix threads
        # https://spec.matrix.org/unstable/client-server-api/#threading
        # room_get_threads(access_token, room_id, include=ThreadInclusion.all, paginate_from=None, limit=None)
        mthreads = asyncio.run_coroutine_threadsafe(self._get_matrix_threads(room_id), self.loop)
        m = mthreads.result()
        thread_event_id = None
        matching_event_ids = []

        for message in m:
            if 'content' in message.source and 'body' in message.source['content']:
                body = message.source['content']['body']
                ztopic = matrix_validate_topic(body)
                if ztopic == msg["subject"]:
                    thread_event_id = message.event_id
                    matching_event_ids.append(thread_event_id)
                    logging.debug("_zulip_to_matrix; found existing matrix thread: %s with id: %s", ztopic, thread_event_id)
                    break
                else:
                    logging.debug("_zulip_to_matrix; skipping %s as it does not match matrix thread: %s", ztopic, msg["subject"])

        # Forward Zulip message to Matrix and ensure they go into a thread if there is any
        if thread_event_id is None:
            logging.warning("_zulip_to_matrix; no matching thread found, creating...")
            created_thread_event_id = self._matrix_send(
				room_id=room_id,
				message_type="m.room.message",
				content={"msgtype": "m.text", "body": "#" + msg["subject"] + "# New Matrix THREAD <-> TOPIC initiated (#" + msg["subject"] + "#). Your client must support threading to view it properly!"},
			)
            self._matrix_send(
				room_id=room_id,
				message_type="m.room.message",
				content={"m.relates_to":{"rel_type": "m.thread","event_id": created_thread_event_id}, "msgtype": "m.text", "body": content}
            )
        else:
             self._matrix_send(
                    room_id=room_id,
                    message_type="m.room.message",
                    content={"m.relates_to":{"rel_type": "m.thread","event_id": thread_event_id}, "msgtype": "m.text", "body": content}
                )
        
        # Get embedded files.
        files_to_send, media_success = asyncio.run_coroutine_threadsafe(
            self.handle_media(msg["content"]), self.loop
        ).result()

        if files_to_send:
            self._matrix_send(
                room_id=room_id,
                message_type="m.room.message",
                content={"msgtype": "m.text", "body": "This message contains the following files:"},
            )
            for file in files_to_send:
                self._matrix_send(room_id=room_id, message_type="m.room.message", content=file)
        if not media_success:
            self._matrix_send(
                room_id=room_id,
                message_type="m.room.message",
                content={
                    "msgtype": "m.text",
                    "body": "This message contained some files which could not be forwarded.",
                },
            )

    def ensure_stream_membership(self) -> None:
        """Ensure that the client is member of all necessary streams.

        Note that this may create streams if they do not exist and if
        the bot has enough rights to do so.
        """
        for stream, _ in self.zulip_config["bridges"]:
            result: Dict[str, Any] = self.zulip_client.get_stream_id(stream)
            if result["result"] == "error":
                raise BridgeFatalZulipError(f"cannot access stream '{stream}': {result}")
            if result["result"] != "success":
                raise BridgeFatalZulipError(f"cannot checkout stream id for stream '{stream}'")
            result = self.zulip_client.add_subscriptions(streams=[{"name": stream}])
            if result["result"] != "success":
                raise BridgeFatalZulipError(f"cannot subscribe to stream '{stream}': {result}")

    def get_matrix_room_for_zulip_message(self, msg: Dict[str, Any]) -> Optional[str]:
        """Check whether we want to process the given message.

        Return the room to which the given message should be forwarded, or
        None if we do not want to process the given message.
        """
        if msg["type"] != "stream":
            return None

        # We do this to identify the messages generated from Matrix -> Zulip
        # and we make sure we don't forward it again to the Matrix.
        if msg["sender_email"] == self.zulip_config["email"]:
            return None

        key: Tuple[str, str] = (msg["display_recipient"], msg["subject"])

        # if stream and topic match - return room id
        if key in self.zulip_config["bridges"]:
            logging.debug("requested stream/topic is configured: %s", key)
            return self.zulip_config["bridges"][key]
        # if stream not configured at all - skip
        elif not any(msg["display_recipient"] in k for k in self.zulip_config["bridges"]):
            logging.error("requested stream is NOT configured: >%s< (your config: %s)", msg["display_recipient"], self.zulip_config["bridges"])
            return None
        # if stream IS configured but the topic does not match, return the room id if we allow unknown topics regardless
        elif not any(msg["subject"] in k for k in self.zulip_config["bridges"]) and self.matrix_config["allow_unknown_zulip_topics"] == "true":
            logging.warning("requested topic >%s< is NOT configured but we ALLOW it because of allow_unknown_zulip_topics", msg["subject"])
            bkt = None
            for bkey in self.zulip_config["bridges"]:
                if bkey[0] == msg["display_recipient"]:
                    bkt: Tuple[str, str] = (msg["display_recipient"], bkey[1])
                    break
            return self.zulip_config["bridges"][bkt]
        # if stream is configured but we do NOT allow unknown topics - skip
        else:
            logging.error("requested stream >%s< is configured but the topic not: >%s<. You may want to set allow_unknown_zulip_topics?", msg["display_recipient"], msg["subject"])
            return None

    async def handle_media(self, msg: str) -> Tuple[Optional[List[Dict[str, Any]]], bool]:
        """Handle embedded media in the Zulip message.

        Download the linked files from the Zulip server and upload them
        to mthe matrix server.
        Return a tuple containing the list of the messages which need
        to be sent to the matrix room and a boolean flag indicating
        whether there have been files for which the download/upload part
        failed.
        """
        msgtype: str
        files_to_send: List[Dict[str, Any]] = []
        success: bool = True

        for file in re.findall(r"\[[^\[\]]*\]\((/user_uploads/[^\(\)]*)\)", msg):
            result: Dict[str, Any] = self.zulip_client.call_endpoint(file, method="GET")
            if result["result"] != "success":
                success = False
                continue

            try:
                with urllib.request.urlopen(self.server_url + result["url"]) as response:  # noqa: S310
                    file_content: bytes = response.read()
                    mimetype: str = response.headers.get_content_type()
            except Exception:
                success = False
                continue

            filename: str = file.split("/")[-1]

            response, _ = await self.matrix_client.upload(
                data_provider=BytesIO(file_content), content_type=mimetype, filename=filename
            )
            if isinstance(response, nio.UploadError):
                success = False
                continue

            if mimetype.startswith("audio/"):
                msgtype = "m.audio"
            elif mimetype.startswith("image/"):
                msgtype = "m.image"
            elif mimetype.startswith("video/"):
                msgtype = "m.video"
            else:
                msgtype = "m.file"

            files_to_send.append(
                {
                    "body": filename,
                    "info": {"mimetype": mimetype},
                    "msgtype": msgtype,
                    "url": response.content_uri,
                }
            )

        return (files_to_send, success)

    async def run(self) -> None:
        print("Starting message handler on Zulip client")

        self.loop = asyncio.get_event_loop()

        with ThreadPoolExecutor() as executor:
            await asyncio.get_event_loop().run_in_executor(
                executor, self.zulip_client.call_on_each_message, self._zulip_to_matrix
            )

def find_topics_threads(roomid, zulip_topics, threads_dict, ret, tname=None):
    """
    search for a topic (tname) within Zulip topics (zulip_topics)
    and find matches of that topic with a Matrix thread (threads_dict)
    returns a given type: threadlinks or threadids or None
    """
    threads_ids = {}
    threads_linked = {}
    threads_unlinked = {}
    res = None

    for zt in zulip_topics:
        topic_name = zt.get('name')
        logging.debug("topic >%s< in topics: %s", topic_name, zulip_topics)

        if topic_name in threads_dict:
            thread_link = "https://matrix.to/#/" + roomid + "/" + threads_dict[topic_name]
            threads_linked[topic_name] = f"<a href='{thread_link}'>#{topic_name}#</a>"
            threads_ids[topic_name] = threads_dict[topic_name]
        else:
            threads_unlinked[topic_name] = f"#{topic_name}# type your message here"
            logging.debug("topic >%s< not found in thread dict: %s", topic_name, threads_dict)

#    if ret == "threadlinks":
#        res = {"threads_linked": threads_linked, "threads_unlinked": threads_unlinked}
#    elif ret == "threadids" and threads_ids:
#        res = threads_ids
    
    return {"threads_linked": threads_linked, "threads_unlinked": threads_unlinked, "threads_ids": threads_ids}

def matrix_validate_topic(content):
    """
    Matrix -> Zulip validate if a topic was given with:
        #<topic># <message>
    or
        @username #<topic># <message>
    """
    # find a zulip topic if any (first line only)
    msg_subject = re.search(r"^(?:@\w+:[^#]+)?#(.*?)#", content)
    if msg_subject:
        topic = msg_subject.group(1)
        logging.debug("valid topic found: >%s<", topic)
    else:
        topic = None
        logging.warning("no (valid) topic found in >%s<!", content)
    return topic

def matrix_validate_cmd(content):
    """
    checks the first X chars to identify a bot command
    returns true if a bot cmd and false if not
    """
    r = False
    if content[:matrix_bot_prefix_len] == matrix_bot_prefix:
        r = True
    return r

def bridge_get_version(*args, **kwargs):
    """
    Returns the current bridge version based on the latest git commit hash.
    Any arguments passed are ignored.
    """
    try:
        ghash = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD'], stderr=subprocess.STDOUT).strip().decode('utf-8')
        version = "<b>Bridge version: </b>" + ghash + " (<a href='https://github.com/steadfasterX/python-zulip-api'>source</a>)"
    except subprocess.CalledProcessError as e:
        version = "unknown"
        logging.error(f"Error retrieving git commit hash: {e.output.decode('utf-8')}")
    return version

def matrix_bot_cmd(cmd):
    """
    validate and executes a bot command and return its output to the calling func
    """
    valid_cmds = {
        ' topics': None,
        ' version': bridge_get_version,
        ' help': matrix_bot_help
    }
    if cmd in valid_cmds:
        out = valid_cmds[cmd](valid_cmds)
    else:
        out = "Invalid command '{}'. Try one of the following: {}".format(cmd, ', '.join(valid_cmds.keys()))
        logging.error(out)
    return out

def matrix_bot_help(valid_cmds):
    """
    Prints the matrix bot usage.
    """
    vcmds = '<br/>'.join([f'{matrix_bot_prefix}{cmd}'.lstrip() for cmd in valid_cmds.keys()])
    out = f'<b>This bridge understands the following commands:</b><br/><br/><pre>{vcmds}</pre>'
    return out

def die(*_: Any) -> None:
    # We actually want to exit, so run os._exit (so as not to be caught and restarted)
    os._exit(1)


def exception_handler(loop: asyncio.AbstractEventLoop, context: Dict[str, Any]) -> None:
    loop.default_exception_handler(context)
    os._exit(1)


def generate_parser() -> argparse.ArgumentParser:
    description: str = """
    Bridge between Zulip topics and Matrix channels.

    Example matrix 'room_id' options might be, if via matrix.org:
        * #zulip:matrix.org (zulip channel on Matrix)
        * #freenode_#zulip:matrix.org (zulip channel on irc.freenode.net)"""

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "-c", "--config", required=False, help="Path to the config file for the bridge."
    )
    parser.add_argument("-d", "--debug", action="store_true", help="debugging mode switch")
    parser.add_argument(
        "--write-sample-config",
        metavar="PATH",
        dest="sample_config",
        help="Generate a configuration template at the specified location.",
    )
    parser.add_argument(
        "--from-zuliprc",
        metavar="ZULIPRC",
        dest="zuliprc",
        help="Optional path to zuliprc file for bot, when using --write-sample-config",
    )
    parser.add_argument(
        "--show-join-leave",
        dest="no_noise",
        default=True,
        action="store_false",
        help="Enable IRC join/leave events.",
    )
    return parser


def read_configuration(config_file: str) -> Dict[str, Dict[str, Any]]:
    matrix_key_set: Set[str] = {"host", "mxid", "password", "allow_unknown_zulip_topics", "room_id"}
    matrix_bridge_key_set: Set[str] = {"room_id", "allow_unknown_zulip_topics"}
    matrix_full_key_set: Set[str] = matrix_key_set | matrix_bridge_key_set
    zulip_key_set: Set[str] = {"email", "api_key", "site", "enforce_topic_per_room", "enforce_topic_per_message"}
    zulip_bridge_key_set: Set[str] = {"stream", "topic", "enforce_topic_per_room", "enforce_topic_per_message"}
    zulip_full_key_set: Set[str] = zulip_key_set | zulip_bridge_key_set
    bridge_key_set: Set[str] = {"room_id", "stream", "topic", "enforce_topic_per_room", "enforce_topic_per_message"}

    config: configparser.ConfigParser = configparser.ConfigParser()

    try:
        config.read(config_file)
    except configparser.Error as exception:
        raise BridgeConfigError(str(exception)) from exception

    if set(config.sections()) < {"matrix", "zulip"}:
        raise BridgeConfigError("Please ensure the configuration has zulip & matrix sections.")

    result: Dict[str, Dict[str, Any]] = {"matrix": {}, "zulip": {}}
    # For Matrix: create a mapping with the Matrix room_ids as keys and
    # the corresponding (stream, topic) tuple as value.
    result["matrix"]["bridges"] = {}
    # For Zulip: create a mapping with the tuple (stream, topic) as keys
    # and the corresponding Matrix room_id as value.
    result["zulip"]["bridges"] = {}
    # One (and maybe the only) bridge is configured in the matrix/zulip
    # sections to keep backwards compatibility with older configuration
    # files.
    first_bridge: Dict[str, Any] = {}
    # Represent a (stream,topic) tuple.
    zulip_target: Tuple[str, str]

    for section in config.sections():
        section_config: Dict[str, str] = dict(config[section])
        section_keys: Set[str] = set(section_config.keys())

        if section.startswith("additional_bridge"):
            if section_keys != bridge_key_set:
                raise BridgeConfigError(
                    f"Please ensure the bridge configuration section {section} contain the following keys: {bridge_key_set}."
                )

            zulip_target = (section_config["stream"], section_config["topic"])
            result["zulip"]["bridges"][zulip_target] = section_config["room_id"]
            result["matrix"]["bridges"][section_config["room_id"]] = zulip_target
        elif section == "matrix":
            if section_keys != matrix_full_key_set:
                raise BridgeConfigError(
                    "Please ensure the matrix configuration section contains the following keys: %s."
                    % str(matrix_full_key_set)
                )

            result["matrix"].update({key: section_config[key] for key in matrix_key_set})

            for key in matrix_bridge_key_set:
                first_bridge[key] = section_config[key]

            # Verify the format of the Matrix user ID.
            if re.fullmatch(r"@[^:]+:.+", result["matrix"]["mxid"]) is None:
                raise BridgeConfigError("Malformatted mxid.")
        elif section == "zulip":
            if section_keys != zulip_full_key_set:
                raise BridgeConfigError(
                    "Please ensure the zulip configuration section contains the following keys: %s."
                    % str(zulip_full_key_set)
                )

            result["zulip"].update({key: section_config[key] for key in zulip_key_set})

            for key in zulip_bridge_key_set:
                first_bridge[key] = section_config[key]
        else:
            logging.warning("Unknown section %s", section)

    # Add the "first_bridge" to the bridges.
    zulip_target = (first_bridge["stream"], first_bridge["topic"])
    result["zulip"]["bridges"][zulip_target] = first_bridge["room_id"]
    result["matrix"]["bridges"][first_bridge["room_id"]] = zulip_target

    return result


async def run(zulip_config: Dict[str, Any], matrix_config: Dict[str, Any], no_noise: bool) -> None:
    asyncio.get_event_loop().set_exception_handler(exception_handler)

    matrix_client: Optional[nio.AsyncClient] = None

    print("Starting Zulip <-> Matrix mirroring bot")

    # Initiate clients and start the event listeners.
    backoff = zulip.RandomExponentialBackoff(timeout_success_equivalent=300)

    retry_count = 0

    while backoff.keep_going():
        try:
            zulip_client = zulip.Client(
                email=zulip_config["email"],
                api_key=zulip_config["api_key"],
                site=zulip_config["site"],
            )
            matrix_client = nio.AsyncClient(matrix_config["host"], matrix_config["mxid"])

            matrix_to_zulip: MatrixToZulip = await MatrixToZulip.create(
                matrix_client, matrix_config, zulip_client, zulip_config, no_noise
            )
            zulip_to_matrix: ZulipToMatrix = await ZulipToMatrix.create(
                zulip_client, zulip_config, matrix_config, matrix_client
            )

            await asyncio.gather(matrix_to_zulip.run(), zulip_to_matrix.run())

        except BridgeFatalMatrixError as exception:
            logging.error("Matrix bridge error occured: >%s<. Restarting bridge in %is", exception, bridge_restart_delay)
            await asyncio.sleep(bridge_restart_delay)

        except BridgeFatalZulipError as exception:
            logging.error("Zulip bridge error occured: >%s<. Restarting bridge in %is", exception, bridge_restart_delay)
            await asyncio.sleep(bridge_restart_delay)

        except zulip.ZulipError as exception:
            logging.error("Zulip error occured: >%s<. Restarting bridge in %is", exception, bridge_restart_delay)
            await asyncio.sleep(bridge_restart_delay)

        except Exception as e:
            traceback.print_exc()
            logging.error("Internal exception occured. Restarting bridge in %is", bridge_restart_delay)
            break

        finally:
            if matrix_client:
                await matrix_client.close()

        backoff.fail()
        retry_count += 1
        logging.info(f"Retry attempt: {retry_count}/{bridge_restart_max_retry}")

        if retry_count >= bridge_restart_max_retry:
            logging.error(f"FATAL! Maximum retry attempts ({bridge_restart_max_retry}) reached. Exiting...")
            break


def write_sample_config(target_path: str, zuliprc: Optional[str]) -> None:
    if os.path.exists(target_path):
        raise BridgeConfigError(f"Path '{target_path}' exists; not overwriting existing file.")

    sample_dict: OrderedDict[str, OrderedDict[str, str]] = OrderedDict(
        (
            (
                "matrix",
                OrderedDict(
                    (
                        ("host", "https://matrix.org"),
                        ("mxid", "@username:matrix.org"),
                        ("password", "password"),
                        ("room_id", "#zulip:matrix.org"),
                    )
                ),
            ),
            (
                "zulip",
                OrderedDict(
                    (
                        ("email", "glitch-bot@chat.zulip.org"),
                        ("api_key", "aPiKeY"),
                        ("site", "https://chat.zulip.org"),
                        ("stream", "test here"),
                        ("topic", "matrix"),
                        ("enforce_topic_per_message", "false"),
                        ("enforce_topic_per_room", "false"),
                    )
                ),
            ),
            (
                "additional_bridge1",
                OrderedDict(
                    (
                        ("room_id", "#example:matrix.org"),
                        ("stream", "new test"),
                        ("topic", "matrix"),
                        ("enforce_topic_per_message", "false"),
                        ("enforce_topic_per_room", "false"),
                    )
                ),
            ),
        )
    )

    if zuliprc is not None:
        if not os.path.exists(zuliprc):
            raise BridgeConfigError(f"Zuliprc file '{zuliprc}' does not exist.")

        zuliprc_config: configparser.ConfigParser = configparser.ConfigParser()
        try:
            zuliprc_config.read(zuliprc)
        except configparser.Error as exception:
            raise BridgeConfigError(str(exception)) from exception

        try:
            sample_dict["zulip"]["email"] = zuliprc_config["api"]["email"]
            sample_dict["zulip"]["site"] = zuliprc_config["api"]["site"]
            sample_dict["zulip"]["api_key"] = zuliprc_config["api"]["key"]
        except KeyError as exception:
            raise BridgeConfigError(
                "You provided an invalid zuliprc file: " + str(exception)
            ) from exception

    sample: configparser.ConfigParser = configparser.ConfigParser()
    sample.read_dict(sample_dict)
    with open(target_path, "w") as target:
        sample.write(target)


def main() -> None:
    signal.signal(signal.SIGINT, die)
    signal.signal(signal.SIGTERM, die)
    logging.basicConfig(level=logging.WARNING)

    parser: argparse.ArgumentParser = generate_parser()
    options: argparse.Namespace = parser.parse_args()

    if options.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if options.sample_config:
        try:
            write_sample_config(options.sample_config, options.zuliprc)
        except BridgeConfigError as exception:
            print(f"Could not write sample config: {exception}")
            sys.exit(1)
        if options.zuliprc is None:
            print(f"Wrote sample configuration to '{options.sample_config}'")
        else:
            print(
                "Wrote sample configuration to '{}' using zuliprc file '{}'".format(
                    options.sample_config, options.zuliprc
                )
            )
        sys.exit(0)
    elif not options.config:
        print("Options required: -c or --config to run, OR --write-sample-config.")
        parser.print_usage()
        sys.exit(1)

    try:
        config: Dict[str, Dict[str, Any]] = read_configuration(options.config)
    except BridgeConfigError as exception:
        print(f"Could not parse config file: {exception}")
        sys.exit(1)

    # Get config for each client
    zulip_config: Dict[str, Any] = config["zulip"]
    matrix_config: Dict[str, Any] = config["matrix"]

    loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    loop.run_until_complete(run(zulip_config, matrix_config, options.no_noise))
    loop.close()


if __name__ == "__main__":
    main()
