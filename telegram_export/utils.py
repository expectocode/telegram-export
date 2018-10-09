"""Utility functions for telegram-export which aren't specific to one purpose"""
import mimetypes

from telethon.tl import types
from urllib.parse import urlparse
try:
    import socks
except ImportError:
    socks = None

ENTITY_TO_TEXT = {
    types.MessageEntityPre: 'pre',
    types.MessageEntityCode: 'code',
    types.MessageEntityBold: 'bold',
    types.MessageEntityItalic: 'italic',
    types.MessageEntityTextUrl: 'texturl',
    types.MessageEntityUrl: 'url',
    types.MessageEntityMentionName: 'mentionname'
}

TEXT_TO_ENTITY = {v: k for k, v in ENTITY_TO_TEXT.items()}

# The mimetypes module has many extension for the same mimetype and it will
# return the one that happens to be first (e.g. ".bat" for "text/plain").
# This map contains a few common mimetypes and their most common extension.
#
# The following code can be use to find out which mimetypes have several ext:
'''
import mimetypes
from collections import defaultdict
d = defaultdict(list)
for k, v in mimetypes.types_map.items():
    d[v].append(k)

d = {k: v for k, v in d.items() if len(v) > 1}
'''
COMMON_MIME_TO_EXTENSION = {
    'text/plain': '.txt',  # To avoid ".bat"
    'image/jpeg': '.jpg',  # To avoid ".jpe"
    'image/bmp': '.bmp',  # To avoid ".dib"
    'video/mp4': '.mp4',  # To avoid ".m4v"
}


def encode_msg_entities(entities):
    """
    Encodes a list of MessageEntity into a string so it
    can easily be dumped into e.g. Dumper's database.
    """
    if not entities:
        return None
    parsed = []
    for entity in entities:
        if entity.__class__ in ENTITY_TO_TEXT:
            if isinstance(entity, types.MessageEntityTextUrl):
                extra = ',{}'.format(
                    entity.url.replace(',', '%2c').replace(';', '%3b')
                )
            elif isinstance(entity, types.MessageEntityMentionName):
                extra = ',{}'.format(entity.user_id)
            else:
                extra = ''
            parsed.append('{},{},{}{}'.format(
                ENTITY_TO_TEXT[type(entity)],
                entity.offset, entity.length, extra
            ))
    return ';'.join(parsed)


def decode_msg_entities(string):
    """
    Reverses the transformation made by ``utils.encode_msg_entities``.
    """
    if not string:
        return None
    parsed = []
    for part in string.split(';'):
        split = part.split(',')
        kind, offset, length = split[0], int(split[1]), int(split[2])
        if kind in TEXT_TO_ENTITY:
            if kind == 'texturl':
                parsed.append(types.MessageEntityTextUrl(
                    offset, length, split[-1]
                ))
            elif kind == 'mentionname':
                parsed.append(types.MessageEntityMentionName(
                    offset, length, int(split[-1])
                ))
            else:
                parsed.append(TEXT_TO_ENTITY[kind](offset, length))
    return parsed


def get_media_type(media):
    """
    Returns a friendly type for the given media.
    """
    if not media:
        return ''

    if isinstance(media, types.MessageMediaPhoto):
        return 'photo'

    elif isinstance(media, types.MessageMediaDocument):
        if isinstance(media, types.Document):
            for attr in media.attributes:
                if isinstance(attr, types.DocumentAttributeSticker):
                    return 'document.sticker'
                elif isinstance(attr, types.DocumentAttributeVideo):
                    return 'document.video'
                elif isinstance(attr, types.DocumentAttributeAnimated):
                    return 'document.animated'
                elif isinstance(attr, types.DocumentAttributeAudio):
                    if attr.voice:
                        return 'document.voice'
                    return 'document.audio'
        return 'document'

    if isinstance(media, (types.Photo,
                          types.UserProfilePhoto, types.ChatPhoto)):
        return 'chatphoto'

    return 'unknown'


def get_extension(mime):
    """
    Returns the most common extension for the given mimetype, or '.bin' if
    none can be found to indicate that it contains arbitrary binary data.
    """
    if not mime:
        mime = ''
    
    return (
        COMMON_MIME_TO_EXTENSION.get(mime)
        or mimetypes.guess_extension(mime)
        or '.bin'
    )


def get_file_location(media):
    """
    Helper method to turn arbitrary media into (InputFileLocation, size/None).
    """
    location = file_size = None
    if isinstance(media, types.MessageMediaPhoto):
        media = media.photo

    if isinstance(media, types.Photo):
        for size in reversed(media.sizes):
            if isinstance(size, types.PhotoSize):
                if isinstance(size.location, types.FileLocation):
                    file_size = size.size
                    location = size.location
                    break
    elif isinstance(media, types.MessageMediaDocument):
        if isinstance(media.document, types.Document):
            file_size = media.document.size
            location = types.InputDocumentFileLocation(
                id=media.document.id,
                access_hash=media.document.access_hash,
                version=media.document.version
            )
    elif isinstance(media, (types.UserProfilePhoto, types.ChatPhoto)):
        if isinstance(media.photo_big, types.FileLocation):
            location = media.photo_big
        elif isinstance(media.photo_small, types.FileLocation):
            location = media.photo_small

    if isinstance(location, types.FileLocation):
        location = types.InputFileLocation(
            volume_id=location.volume_id,
            local_id=location.local_id,
            secret=location.secret
        )

    return location, file_size


def action_to_name(action):
    """
    Returns a namespace'd "friendly" name for the given
    ``MessageAction`` or ``ChannelAdminLogEventAction``.
    """
    return {
        types.MessageActionChannelCreate: 'channel.create',
        types.MessageActionChannelMigrateFrom: 'channel.migratefrom',
        types.MessageActionChatAddUser: 'chat.adduser',
        types.MessageActionChatCreate: 'chat.create',
        types.MessageActionChatDeletePhoto: 'chat.deletephoto',
        types.MessageActionChatDeleteUser: 'chat.deleteuser',
        types.MessageActionChatEditPhoto: 'chat.editphoto',
        types.MessageActionChatEditTitle: 'chat.edittitle',
        types.MessageActionChatJoinedByLink: 'chat.joinedbylink',
        types.MessageActionChatMigrateTo: 'chat.migrateto',
        types.MessageActionCustomAction: 'custom',
        types.MessageActionEmpty: 'empty',
        types.MessageActionGameScore: 'game.score',
        types.MessageActionHistoryClear: 'history.clear',
        types.MessageActionPaymentSent: 'payment.sent',
        types.MessageActionPaymentSentMe: 'payment.sentme',
        types.MessageActionPhoneCall: 'phone.call',
        types.MessageActionPinMessage: 'pin.message',
        types.MessageActionScreenshotTaken: 'screenshottaken',

        types.ChannelAdminLogEventActionChangeAbout: 'change.about',
        types.ChannelAdminLogEventActionChangePhoto: 'change.photo',
        types.ChannelAdminLogEventActionChangeStickerSet: 'change.stickerset',
        types.ChannelAdminLogEventActionChangeTitle: 'change.title',
        types.ChannelAdminLogEventActionChangeUsername: 'change.username',
        types.ChannelAdminLogEventActionDeleteMessage: 'delete.message',
        types.ChannelAdminLogEventActionEditMessage: 'edit.message',
        types.ChannelAdminLogEventActionParticipantInvite: 'participant.invite',
        types.ChannelAdminLogEventActionParticipantJoin: 'participant.join',
        types.ChannelAdminLogEventActionParticipantLeave: 'participant.leave',
        types.ChannelAdminLogEventActionParticipantToggleAdmin: 'participant.toggleadmin',
        types.ChannelAdminLogEventActionParticipantToggleBan: 'participant.toggleban',
        types.ChannelAdminLogEventActionToggleInvites: 'toggle.invites',
        types.ChannelAdminLogEventActionTogglePreHistoryHidden: 'toggle.prehistoryhidden',
        types.ChannelAdminLogEventActionToggleSignatures: 'toggle.signatures',
        types.ChannelAdminLogEventActionUpdatePinned: 'update.pinned',
    }.get(type(action), None)


def parse_proxy_str(proxy_str):
    """
    Returns proxy from given string
    """
    if socks is None:
        raise Exception('Please install PySocks if you want to use a proxy')
    url_parser = urlparse(proxy_str)
    proxy_type = None
    proxy_type_str = url_parser.scheme
    
    if proxy_type_str.lower() == "socks5":
        proxy_type = socks.SOCKS5
    elif proxy_type_str.lower() == "socks4":
        proxy_type = socks.SOCKS4
    elif proxy_type_str.lower() == "https":
        proxy_type = socks.HTTP
    elif proxy_type_str.lower() == "http":
        proxy_type = socks.HTTP
    else:
        raise ValueError("Proxy type %s is not supported" % proxy_type)

    host = url_parser.hostname
    port = url_parser.port

    if host is None:
        raise ValueError("Host parsing error")
    if port is None:
        raise ValueError("Port parsing error")

    user = url_parser.username
    password = url_parser.password

    if user is not None and password is not None:
        proxy = (proxy_type, host, port, True, user, password)
    else:
        proxy = (proxy_type, host, port)
    return proxy
