from telethon.tl import types


ENTITY_TO_TEXT = {
    types.MessageEntityPre: 'pre',
    types.MessageEntityCode: 'code',
    types.MessageEntityBold: 'bold',
    types.MessageEntityItalic: 'italic',
    types.MessageEntityTextUrl: 'texturl',
    types.MessageEntityMentionName: 'mentionname'
}

TEXT_TO_ENTITY = {v: k for k, v in ENTITY_TO_TEXT.items()}


def encode_msg_entities(entities):
    """
    Encodes a list of MessageEntity into a string so it
    can easily be dumped into e.g. Dumper's database.
    """
    if not entities:
        return None
    parsed = []
    for entity in entities:
        if type(entity) in ENTITY_TO_TEXT:
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
