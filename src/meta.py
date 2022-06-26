import datetime as dt

import datazimmer as dz


class FacebookPage(dz.AbstractEntity):
    fid = dz.Index & str


class FacebookPost(dz.AbstractEntity):
    pid = dz.Index & str
    posted = dz.Index & dt.datetime
    link = str
    source = FacebookPage


# TODO: unify these two - maybe?
class PageRecord(dz.AbstractEntity):
    recorded = dt.datetime
    page = FacebookPage
    likes = int
    followers = int
    name = str


class PostRecord(dz.AbstractEntity):
    recorded = dt.datetime
    post = FacebookPost
    reactions = int
    shares = int
    comments = int
