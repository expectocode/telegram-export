#!/bin/env python3
import asyncio
import datetime
import itertools
import logging
import mimetypes
import os
import time
from collections import defaultdict

import tqdm
from telethon import utils
from telethon.errors import ChatAdminRequiredError
from telethon.tl import types, functions

import utils as export_utils

__log__ = logging.getLogger(__name__)


VALID_TYPES = {
    'photo', 'document', 'video', 'audio', 'sticker', 'voice', 'chatphoto'
}
BAR_FORMAT = "{l_bar}{bar}| {n_fmt}/{total_fmt} " \
             "[{elapsed}/{remaining}, {rate_noinv_fmt}{postfix}]"


QUEUE_TIMEOUT = 5
DOWNLOAD_PART_SIZE = 256 * 1024


class Downloader:
    """
    Download dialogs and their associated data, and dump them.
    Make Telegram API requests and sleep for the appropriate time.
    """
    def __init__(self, client, config, dumper):
        self.client = client
        self.max_size = config.getint('MaxSize')
        self.types = {x.strip().lower()
                      for x in (config.get('MediaWhitelist') or '').split(',')
                      if x.strip()}
        self.media_fmt = os.path.join(config['OutputDirectory'],
                                      config['MediaFilenameFmt'])
        assert all(x in VALID_TYPES for x in self.types)
        if self.types:
            self.types.add('unknown')  # Always allow "unknown" media types

        self.dumper = dumper
        self._checked_entity_ids = set()
        self._media_bar = None
        # We're gonna need a few queues if we want to do things concurrently.
        # None values should be inserted to notify that the dump has finished.
        self._media_queue = asyncio.Queue()
        self._user_queue = asyncio.Queue()
        self._chat_queue = asyncio.Queue()
        self._running = False

    def _check_media(self, media):
        """
        Checks whether the given MessageMedia should be downloaded or not.
        """
        if not media or not self.max_size:
            return False
        if not self.types:
            return True
        return export_utils.get_media_type(media) in self.types

    def _dump_full_entity(self, entity):
        """
        Dumps the full entity into the Dumper, also enqueuing their profile
        photo if any so it can be downloaded later by a different coroutine.
        """
        if isinstance(entity, types.UserFull):
            user = entity.user
            self.enqueue_media(entity.profile_photo, user, user)
            photo_id = self.dumper.dump_media(entity.profile_photo)
            self.dumper.dump_user(entity, photo_id=photo_id)

        elif isinstance(entity, types.Chat):
            self.enqueue_media(entity.photo, entity, entity)
            photo_id = self.dumper.dump_media(entity.photo)
            self.dumper.dump_chat(entity, photo_id=photo_id)

        elif isinstance(entity, types.messages.ChatFull):
            photo_id = self.dumper.dump_media(entity.full_chat.chat_photo)
            chat = next(
                x for x in entity.chats if x.id == entity.full_chat.id
            )
            self.enqueue_media(entity.full_chat.chat_photo, chat, chat)
            if chat.megagroup:
                self.dumper.dump_supergroup(entity.full_chat, chat,
                                            photo_id)
            else:
                self.dumper.dump_channel(entity.full_chat, chat, photo_id)

    def _dump_messages(self, messages, target, entities):
        """
        Helper method to iterate the messages from a GetMessageHistoryRequest
        and dump them into the Dumper, mostly to avoid excessive nesting.

        Also enqueues any media to be downloaded later by a different coroutine.
        """
        for m in messages:
            if isinstance(m, types.Message):
                self.enqueue_media(m, target, entities.get(m.from_id))
                self.dumper.dump_message(
                    message=m,
                    context_id=utils.get_peer_id(target),
                    forward_id=self.dumper.dump_forward(m.fwd_from),
                    media_id=self.dumper.dump_media(m.media)
                )
            elif isinstance(m, types.MessageService):
                if isinstance(m.action, types.MessageActionChatEditPhoto):
                    media_id = self.dumper.dump_media(m.action.photo)
                    self.enqueue_media(m.action.photo, target,
                                       entities.get(m.from_id), known_id=m.id)
                else:
                    media_id = None
                self.dumper.dump_message_service(
                    message=m,
                    context_id=utils.get_peer_id(target),
                    media_id=media_id
                )

    def _dump_admin_log(self, events, target, entities):
        """
        Helper method to iterate the events from a GetAdminLogRequest
        and dump them into the Dumper, mostly to avoid excessive nesting.

        Also enqueues any media to be downloaded later by a different coroutine.
        """
        for event in events:
            if isinstance(event.action,
                          types.ChannelAdminLogEventActionChangePhoto):
                media_id1 = self.dumper.dump_media(event.action.new_photo)
                media_id2 = self.dumper.dump_media(event.action.prev_photo)
                self.enqueue_media(event.action.new_photo, target,
                                   from_entity=entities[event.user_id])
                self.enqueue_media(event.action.prev_photo, target,
                                   from_entity=entities[event.user_id])
            else:
                media_id1 = None
                media_id2 = None
            self.dumper.dump_admin_log_event(
                event, utils.get_peer_id(target), media_id1, media_id2
            )
        return min(e.id for e in events)

    async def _media_consumer(self, queue, bar):
        def progress(saved, total):
            if total is None:
                # No size was found so the bar total wasn't incremented before
                bar.total += saved
                bar.update(saved)
            elif saved == total:
                # Downloaded the last bit (which is probably <> part size)
                mod = (saved % DOWNLOAD_PART_SIZE) or DOWNLOAD_PART_SIZE
                bar.update(mod)
            else:
                # All chunks are of the same size and this isn't the last one
                bar.update(DOWNLOAD_PART_SIZE)

        while self._running:
            start = time.time()
            location, file, file_size = await queue.get()
            if file_size is not None:
                bar.total += file_size

            os.makedirs(os.path.dirname(file), exist_ok=True)
            await self.client.download_file(
                location, file=file, file_size=file_size,
                part_size_kb=DOWNLOAD_PART_SIZE // 1024,
                progress_callback=progress
            )
            queue.task_done()
            await asyncio.sleep(max(1.5 - (time.time() - start), 0))

    async def _user_consumer(self, queue, bar):
        while self._running:
            start = time.time()
            self._dump_full_entity(await self.client(
                functions.users.GetFullUserRequest(await queue.get())
            ))
            queue.task_done()
            bar.update(1)
            await asyncio.sleep(max(1.5 - (time.time() - start), 0))

    async def _chat_consumer(self, queue, bar):
        while self._running:
            start = time.time()
            chat = await queue.get()
            if isinstance(chat, types.Chat):
                self._dump_full_entity(chat)
            else:  # isinstance(chat, types.Channel):
                self._dump_full_entity(await self.client(
                    functions.channels.GetFullChannelRequest(chat)
                ))
            queue.task_done()
            bar.update(1)
            await asyncio.sleep(max(1.5 - (time.time() - start), 0))

    def enqueue_entities(self, entities):
        """
        Enqueues the given iterable of entities to be dumped later by a
        different coroutine. These in turn might enqueue profile photos.
        """
        for entity in entities:
            eid = utils.get_peer_id(entity)
            if isinstance(entity, types.User):
                if entity.deleted or entity.min:
                    continue  # Empty name would cause IntegrityError
            elif isinstance(entity, types.Channel):
                if entity.left:
                    continue  # Getting full info triggers ChannelPrivateError
            elif not isinstance(entity, types.Chat):
                # Drop UserEmpty, ChatEmpty, ChatForbidden and ChannelForbidden
                continue

            if eid in self._checked_entity_ids:
                continue
            else:
                self._checked_entity_ids.add(eid)
                if isinstance(entity, types.User):
                    self._user_queue.put_nowait(entity)
                else:
                    self._chat_queue.put_nowait(entity)

    def enqueue_media(self, media, target, from_entity, known_id=None):
        """
        Enqueues the given message or media from the given context entity
        to be downloaded later. If the ID of the message is known it should
        be set in known_id. The media won't be enqueued unless its download
        is desired.
        """
        if isinstance(media, types.Message):
            msg = media
            if not self._check_media(msg.media):
                return

            media = msg.media
            location, file_size = export_utils.get_file_location(media)
            if not location:
                return

            # TODO Reuse the formatter when getting a filename. Somehow.
            formatter = defaultdict(
                str,
                id=msg.id,
                context_id=utils.get_peer_id(target),
                sender_id=msg.from_id or 0,
                ext=utils.get_extension(media) or '.bin',
                type=export_utils.get_media_type(media) or 'unknown',
                name=utils.get_display_name(target) or 'unknown',
                sender_name=utils.get_display_name(
                    from_entity) or 'unknown'
            )
            filename = None
            if isinstance(media, types.MessageMediaDocument):
                for attr in media.document.attributes:
                    if isinstance(attr, types.DocumentAttributeFilename):
                        filename = attr.file_name

            formatter['filename'] = filename or msg.date.strftime(
                '{}_%Y-%m-%d_%H-%M-%S'.format(formatter['type'])
            )
            filename = msg.date.strftime(self.media_fmt).format_map(formatter)
            if not filename.endswith(formatter['ext']):
                if filename.endswith('.'):
                    filename = filename[:-1]
                filename += formatter['ext']

            self._media_queue.put_nowait((location, filename, file_size))

        elif isinstance(media, (types.Photo,
                                types.UserProfilePhoto, types.ChatPhoto)):
            if 'chatphoto' not in self.types:
                return

            if isinstance(media, types.Photo):
                date = media.date
                known_id = known_id or media.id
            else:
                date = datetime.datetime.now()
                known_id = known_id or utils.get_peer_id(target)

            location, file_size = export_utils.get_file_location(media)
            formatter = defaultdict(
                str,
                id=known_id,
                context_id=utils.get_peer_id(target),
                sender_id=utils.get_peer_id(target),
                ext='.jpg',
                type='chatphoto',
                filename=date.strftime('chatphoto_%Y-%m-%d_%H-%M-%S'),
                name=utils.get_display_name(target) or 'unknown',
                sender_name=utils.get_display_name(target) or 'unknown'
            )
            filename = date.strftime(self.media_fmt).format_map(formatter)
            if not filename.endswith(formatter['ext']):
                if filename.endswith('.'):
                    filename = filename[:-1]
                filename += formatter['ext']

            self._media_queue.put_nowait((location, filename, file_size))

    async def start(self, target_id):
        """
        Starts the dump with the given target ID.
        """
        self._running = True
        target_in = await self.client.get_input_entity(target_id)
        target = await self.client.get_entity(target_in)
        target_id = utils.get_peer_id(target)

        found = self.dumper.get_message_count(target_id)
        pbar = tqdm.tqdm(unit=' messages',
                         desc=utils.get_display_name(target),
                         initial=found, bar_format=BAR_FORMAT)
        entbar = tqdm.tqdm(unit=' entities', bar_format=BAR_FORMAT,
                           postfix={'chat': utils.get_display_name(target)})
        medbar = tqdm.tqdm(unit='B', unit_divisor=1024, unit_scale=True,
                           bar_format=BAR_FORMAT, postfix={'media': 'saved'})

        medbar.total = 0

        asyncio.ensure_future(self._user_consumer(self._user_queue, entbar))
        asyncio.ensure_future(self._chat_consumer(self._chat_queue, entbar))
        asyncio.ensure_future(self._media_consumer(self._media_queue, medbar))
        try:
            self.enqueue_entities((target,))
            entbar.total = len(self._checked_entity_ids)
            req = functions.messages.GetHistoryRequest(
                peer=target_in,
                offset_id=0,
                offset_date=None,
                add_offset=0,
                limit=self.dumper.chunk_size,
                max_id=0,
                min_id=0,
                hash=0
            )
            if isinstance(target_in,
                          (types.InputPeerChat, types.InputPeerChannel)):
                try:
                    __log__.info('Getting participants...')
                    participants = await self.client.get_participants(target_in)
                    added, removed = self.dumper.dump_participants_delta(
                        target_id, ids=[x.id for x in participants]
                    )
                    __log__.info('Saved %d new members, %d left the chat.',
                                 len(added), len(removed))
                except ChatAdminRequiredError:
                    __log__.info('Getting participants aborted (not admin).')

            req.offset_id, req.offset_date, stop_at = self.dumper.get_resume(
                target_id
            )
            if req.offset_id:
                __log__.info('Resuming at %s (%s)',
                             req.offset_date, req.offset_id)

            # Check if we have access to the admin log
            # TODO Resume admin log?
            # Rather silly considering logs only last up to two days and
            # there isn't much information in them (due to their short life).
            if isinstance(target_in, types.InputPeerChannel):
                log_req = functions.channels.GetAdminLogRequest(
                    target_in, q='', min_id=0, max_id=0, limit=1
                )
                try:
                    await self.client(log_req)
                    log_req.limit = 100
                except ChatAdminRequiredError:
                    log_req = None
            else:
                log_req = None

            chunks_left = self.dumper.max_chunks
            # This loop is for get history, although the admin log
            # is interlaced as well to dump both at the same time.
            while True:
                start = time.time()
                history = await self.client(req)
                # Queue found entities so they can be dumped later
                self.enqueue_entities(itertools.chain(
                    history.users, history.chats
                ))
                entbar.total = len(self._checked_entity_ids)

                # Dump the messages from this batch
                entities = {utils.get_peer_id(x): x for x in itertools.chain(
                    history.users, history.chats, (target,)
                )}
                self._dump_messages(history.messages, target, entities)

                # Determine whether to continue dumping or we're done
                count = len(history.messages)
                pbar.total = getattr(history, 'count', count)
                pbar.update(count)
                if history.messages:
                    # We may reinsert some we already have (so found > total)
                    found = min(found + len(history.messages), pbar.total)
                    req.offset_id = min(m.id for m in history.messages)
                    req.offset_date = min(m.date for m in history.messages)

                # Receiving less messages than the limit means we have
                # reached the end, so we need to exit. Next time we'll
                # start from offset 0 again so we can check for new messages.
                #
                # We dump forward (message ID going towards 0), so as soon
                # as the minimum message ID (now in offset ID) is less than
                # the highest ID ("closest" bound we need to reach), stop.
                if count < req.limit or req.offset_id <= stop_at:
                    __log__.debug('Received less messages than limit, done.')
                    max_id = self.dumper.get_message_id(target_id, 'MAX')
                    self.dumper.save_resume(target_id, stop_at=max_id)
                    break

                # Keep track of the last target ID (smallest one),
                # so we can resume from here in case of interruption.
                self.dumper.save_resume(
                    target_id, msg=req.offset_id, msg_date=req.offset_date,
                    stop_at=stop_at  # We DO want to preserve stop_at.
                )
                self.dumper.commit()

                chunks_left -= 1  # 0 means infinite, will reach -1 and never 0
                if chunks_left == 0:
                    __log__.debug('Reached maximum amount of chunks, done.')
                    break

                # Interlace with the admin log request if any
                if log_req:
                    result = await self.client(log_req)
                    self.enqueue_entities(itertools.chain(
                        result.users, result.chats
                    ))
                    if result.events:
                        entities = {
                            utils.get_peer_id(x): x for x in itertools.chain(
                                result.users, result.chats, (target,))
                        }
                        log_req.max_id = self._dump_admin_log(
                            result.events, target, entities
                        )
                    else:
                        log_req = None

                # 30 request in 30 seconds (sleep a second *between* requests)
                await asyncio.sleep(max(1 - (time.time() - start), 0))

            # Message loop complete, wait for the queues to empty
            pbar.n = pbar.total
            pbar.close()
            self.dumper.commit()

            # This loop is specific to the admin log (to finish up)
            while log_req:
                start = time.time()
                result = await self.client(log_req)
                self.enqueue_entities(itertools.chain(
                    result.users, result.chats
                ))
                if result.events:
                    log_req.max_id = self._dump_admin_log(
                        result.events, target, entities={
                            utils.get_peer_id(x): x for x in itertools.chain(
                                result.users, result.chats, (target,))
                        }
                    )
                    await asyncio.sleep(max(1 - (time.time() - start), 0))
                else:
                    log_req = None

            __log__.info(
                'Done. Retrieving full information about %s missing entities.',
                self._user_queue.qsize() + self._chat_queue.qsize()
            )
            await self._user_queue.join()
            await self._chat_queue.join()
            await self._media_queue.join()
        finally:
            self._running = False
            entbar.n = entbar.total
            entbar.close()
            medbar.n = medbar.total
            medbar.close()

    async def download_past_media(self, dumper, target_id):
        """
        Downloads the past media that has already been dumped into the
        database but has not been downloaded for the given target ID yet.

        Media which formatted filename results in an already-existing file
        will be *ignored* and not re-downloaded again.
        """
        # TODO Should this respect and download only allowed media? Or all?
        target_in = await self.client.get_input_entity(target_id)
        target = await self.client.get_entity(target_in)
        target_id = utils.get_peer_id(target)

        msg_cursor = dumper.conn.cursor()
        msg_cursor.execute('SELECT ID, Date, FromID, MediaID FROM Message '
                           'WHERE ContextID = ? AND MediaID IS NOT NULL',
                           (target_id,))

        msg_row = msg_cursor.fetchone()
        while msg_row:
            media_row = dumper.conn.execute(
                'SELECT LocalID, VolumeID, Secret, Type, MimeType, Name '
                'FROM Media WHERE ID = ?', (msg_row[3],)
            ).fetchone()
            # Documents have attributed and they're saved under the "document"
            # namespace so we need to split it before actually comparing.
            media_type = media_row[3].split('.')
            media_type, media_subtype = media_type[0], media_type[-1]
            if media_type not in ('photo', 'document'):
                # Only photos or documents are actually downloadable
                msg_row = msg_cursor.fetchone()
                continue

            user_row = dumper.conn.execute(
                'SELECT FirstName, LastName FROM User WHERE ID = ?',
                (msg_row[2],)
            ).fetchone()
            if user_row:
                sender_name = '{} {}'.format(
                    msg_row[0] or '', msg_row[1] or ''
                ).strip()
            else:
                sender_name = ''

            date = datetime.datetime.utcfromtimestamp(msg_row[1])
            formatter = defaultdict(
                str,
                id=msg_row[0],
                context_id=target_id,
                sender_id=msg_row[2] or 0,
                type=media_subtype or 'unknown',
                ext=mimetypes.guess_extension(media_row[4]) or '.bin',
                name=utils.get_display_name(target) or 'unknown',
                sender_name=sender_name or 'unknown'
            )
            if formatter['ext'] == '.jpe':
                formatter['ext'] = '.jpg'  # Nobody uses .jpe for photos

            name = None if media_subtype == 'photo' else media_row[5]
            formatter['filename'] = name or date.strftime(
                '{}_%Y-%m-%d_%H-%M-%S'.format(formatter['type'])
            )
            filename = date.strftime(self.media_fmt).format_map(formatter)
            if not filename.endswith(formatter['ext']):
                if filename.endswith('.'):
                    filename = filename[:-1]
                filename += formatter['ext']

            if os.path.isfile(filename):
                __log__.debug('Skipping existing file %s', filename)
            else:
                __log__.info('Downloading to %s', filename)
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                if media_type == 'document':
                    await self.client.download_file(types.InputDocumentFileLocation(
                        id=media_row[0],
                        version=media_row[1],
                        access_hash=media_row[2]
                    ), file=filename)
                else:
                    await self.client.download_file(types.InputFileLocation(
                        local_id=media_row[0],
                        volume_id=media_row[1],
                        secret=media_row[2]
                    ), file=filename)
                await asyncio.sleep(1)
            msg_row = msg_cursor.fetchone()
