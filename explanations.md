# Context ID

The ID of the place a message was sent. UserID for PMs, Group or Channel ID for groups, channels, and supergroups. Something to note is that it is thought that Channel IDs can collide with User and Chat IDs, so we add -100 to ChannelIDs to ensure we do not have this issue.

# DateUpdated

Stored as seconds since epoch, this is used to address the issue of the exporter only 'seeing' a snapshot of the state, and to store historical data. When, for example, a user changes their name, the old User will be kept, and the new User with updated details will have a newer DateUpdated. For this reason, DateUpdated and UserID together form the primary key of the User table.

#Invalidation time

Related to DateUpdated, this can be thought of as a cache invalidation time. When the dumper is run, it checks for if an entity (say, a User) has changed since the last export. If there has been a change, the new User will always be saved. However, if the User is the same as the last export, there is a problem. If the exporter only saves on updates, there is an information gap between User updates, even though in each individual export the exporter knew there was no change, since in analysis we cannot know if a User was checked at times between the saved records. However, if the exporter always saves the new User regardless of changes, there will be many redundant records that serve only to say that a User has not changed. The invalidation time is used to solve this. If the User has not changed, and time since the last saved record is less than this time, the new User will not be saved.

# Various schema decisions

Message text can be null since photos with no caption have no text. Message FromID can be null since Channels provide no FromID. Supergroups are artificially separated from Channels for user friendliness - to Telegram, they are the same thing.
