#!/usr/bin/env python3


import datetime as dt
import praw
import pandas as pd
from psaw import PushshiftAPI
from PIL import Image
import requests
import io
from threading import Lock


class Reddit:


    def __init__(self, token) -> None:
        '''
        Initialize Reddit API by reading client ID and key from token file.
        '''
        self._reddit = praw.Reddit(user_agent=token['api_name'], \
                                   client_id=token['client_id'], \
                                   client_secret=token['secret_key'])

        assert(self._reddit.read_only)

        self._api = PushshiftAPI(self._reddit)


    def getPosts(self, subReddit, epoch) -> None:
        '''
        Download posts for an epoch.
        '''
        generator = self._api.search_submissions(after=int(epoch['start'].timestamp()),
                                                 before=int(epoch['end'].timestamp()),
                                                 limit=epoch['numOfPosts'],
                                                 subreddit=subReddit)

        allPostsAttributes = []

        for post in generator:
            attributes = Reddit._extractAttributes(post)

            if post.removed_by_category is None and not post.is_self:
                # post available and has a link (not text only)
                try:
                    response = requests.get(attributes['url_to_meme'])  # try to download image
                    if 200 == response.status_code:  # request is OK
                        attributes['image'] = Image.open(io.BytesIO(response.content))  # try to read image
                        allPostsAttributes.append(attributes)
                except (IOError, requests.exceptions.RequestException):
                    pass

        return pd.DataFrame(allPostsAttributes)


    @classmethod
    def _extractAttributes(cls, post) -> dict:
        '''
        Extract the attributes from the post and return them in a dictionary.
        '''
        attributes = {'id':                 post.id,                      # ID of post
                      'subreddit':          post.subreddit_name_prefixed, # name of subreddit
                      'subscribers':        post.subreddit_subscribers,   # number of subscribers of subreddit
                      'author':             post.author,                  # name (userid) of the author
                      'title':              post.title,                   # title of thumbnail
                      'awards':             post.total_awards_received,   # number of received awards
                      'created_local_time': post.created,                 # local timestamp of post submission
                      'created_utc_time':   post.created_utc,             # UTC   timestamp of post submission
                      'downs':              post.downs,                   # ownvotes received by post
                      'ups':                post.ups,                     # upvotes  received by post
                      'over_18+_content':   post.over_18,                 # is only suitable for 18+
                      'thumbnail':          post.thumbnail,               
                      'thumbnail_height':   post.thumbnail_height,        
                      'thumbnail_width':    post.thumbnail_width,         
                      'views':              post.view_count,              # number of views of the post
                      'likes':              post.likes,                   # number of likes of the post
                      'score':              post.score,                   
                      'url_to_meme':        post.url,                     # link to the picture
                      'permalink':          post.permalink,
                      'is_video':           post.is_video,
                      'text':               post.selftext}

        return attributes