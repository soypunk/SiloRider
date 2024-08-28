import re
import bs4
import os.path
import gzip
import json
import time
import random
import signal
import urllib.error
import urllib.parse
import urllib.request
import getpass
import logging
import datetime
from .base import Silo
from ..config import has_lxml
from ..format import CardProps, UrlFlattener, URLMODE_ERASE

import atproto
from atproto import models as atprotomodels


logger = logging.getLogger(__name__)


class _BlueskyClient(atproto.Client):
    def __init__(self, *args, **kwargs):
        atproto.Client.__init__(self, *args, **kwargs)

    def send_post(self, text, *, post_datetime=None, embed=None, facets=None):
        # Override the atproto.Client send_post function because it
        # doesn't support facets yet. The code is otherwise more or
        # less identical.
        repo = self.me.did
        langs = [atprotomodels.languages.DEFAULT_LANGUAGE_CODE1]

        # Make sure we have a proper time zone.
        post_datetime = post_datetime or datetime.datetime.now()
        if not post_datetime.tzinfo:
            tz_dt = datetime.datetime.now().astimezone()
            post_datetime = post_datetime.replace(tzinfo=tz_dt.tzinfo)
        created_at = post_datetime.isoformat()

        # Do it!
        data = atprotomodels.ComAtprotoRepoCreateRecord.Data(
                repo=repo,
                collection=atprotomodels.ids.AppBskyFeedPost,
                record=atprotomodels.AppBskyFeedPost.Main(
                    createdAt=created_at,
                    text=text,
                    facets=facets,
                    embed=embed,
                    langs=langs)
                )
        self.com.atproto.repo.create_record(data)


class BlueskySilo(Silo):
    SILO_TYPE = 'bluesky'
    PHOTO_LIMIT = 976560
    _DEFAULT_SERVER = 'bsky.app'
    _CLIENT_CLASS = _BlueskyClient

    def __init__(self, ctx):
        super().__init__(ctx)

        base_url = self.getConfigItem('url')
        self.client = self._CLIENT_CLASS(base_url)

    def authenticate(self, ctx):
        force = ctx.exec_ctx.args.force

        password = self.getCacheItem('password')
        if not password or force:
            logger.info("Authenticating client app with Bluesky for %s" %
                        self.ctx.silo_name)
            email = input("Email: ")
            self.setCacheItem('email', email)

            password = getpass.getpass(prompt="Application password: ")
            profile = self.client.login(email, password)

            logger.info("Authenticated as %s" % profile.display_name)
            self.setCacheItem('password', password)

    def onPostStart(self, ctx):
        if not ctx.args.dry_run:
            email = self.getCacheItem('email')
            password = self.getCacheItem('password')
            if not email or not password:
                raise Exception("Please authenticate Bluesky silo %s" %
                                self.ctx.silo_name)
            self.client.login(email, password)

    def getEntryCard(self, entry, ctx):
        # We use URLMODE_ERASE to remove all hyperlinks from the
        # formatted text, and we later add them as facets to the atproto
        # record.
        url_flattener = BlueskyUrlFlattener()
        card = self.formatEntry(
            entry,
            limit=300,
            # Use Twitter's meta properties
            card_props=CardProps('name', 'twitter'),
            profile_url_handlers=ctx.profile_url_handlers,
            url_flattener=url_flattener,
            url_mode=URLMODE_ERASE)
        card.__bsky_url_flattener = url_flattener
        return card

    def mediaCallback(self, tmpfile, mt, url, desc):
        with open(tmpfile, 'rb') as tmpfp:
            data = tmpfp.read()

        logger.debug("Uploading image to Bluesky (%d bytes) with description: %s" %
                     (len(data), desc))
        upload = self.client.com.atproto.repo.upload_blob(data)

        if desc is None:
            desc = ""
        return atprotomodels.AppBskyEmbedImages.Image(alt=desc, image=upload.blob)

    def postEntry(self, entry_card, media_ids, ctx):
        # Add images as an embed on the atproto record.
        embed = None
        if media_ids:
            embed = atprotomodels.AppBskyEmbedImages.Main(images=media_ids)

        # Grab any URLs detected by our URL flattener and add them as
        # facets on the atproto record.
        facets = None
        first_url = None
        url_flattener = entry_card.__bsky_url_flattener
        if url_flattener.urls:
            facets = []
            for url_info in url_flattener.urls:
                # atproto requires an http or https scheme.
                start, end, url = url_info
                if not url.startswith('http'):
                    url = 'https://' + url

                facet = atprotomodels.AppBskyRichtextFacet.Main(
                    features=[atprotomodels.AppBskyRichtextFacet.Link(uri=url)],
                    index=atprotomodels.AppBskyRichtextFacet.ByteSlice(
                        byteStart=start, byteEnd=end),
                    )
                facets.append(facet)

                if first_url is None:
                    first_url = url

        # Make a link embed for the first link if we didn't have an embed already.
        if embed is None and first_url is not None:
            embed = self._makeUrlEmbed(first_url)

        # Create the record!
        entry_dt = entry_card.entry.get('published')
        self.client.send_post(
                text=entry_card.text,
                post_datetime=entry_dt,
                embed=embed,
                facets=facets)

    def _makeUrlEmbed(self, url):
        # Fetch the document at the URL.
        urlopen = urllib.request.urlopen
        # Because we may hit well-known servers like YouTube, we need to:
        # 1. specify a user-agent that won't get us thrown out
        # 2. handle the case of an error 429, which tells us to wait
        req_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-us,en;q=0.5',
                'Accept-Encoding': 'gzip,deflate'
                }
        logging.debug("Fetching link to build Bluesky link embed: %s" % url)

        attempts = 0
        max_attempts = 3
        html_raw = None
        html_encoding = None
        while attempts < max_attempts:
            attempts += 1
            try:
                req = _build_http_request(url, req_headers)
                # Wrap the request inside a signal-based timeout just in
                # case we encounter some problem in low-level code.
                with SignalTimeout(6, "urlopen timed out!") as sto:
                    with urlopen(req, timeout=5) as resp:
                        logging.debug("Response status: %s" % str(resp.status))
                        logging.debug("Response headers: %s" % str(resp.headers))
                        html_encoding = resp.headers['Content-Encoding']
                        html_raw = resp.read()
                        break
            except Exception as ex:
                logger.warning("Couldn't fetch link: %s" % url)
                logger.warning(str(ex))
                # See if we are being told to retry after a while. If so,
                # wait and retry. If not, abort.
                if not hasattr(ex, 'headers'):
                    break
                retry_after = ex.headers.get('Retry-After')
                if not retry_after:
                    break
                try:
                    wait_time = int(float(retry_after)) + 1
                except ValueError:
                    wait_time = -1
                if wait_time < 0:
                    break
                logger.warning(
                        "Received 'Too Many Requests' error from the server, "
                        "waiting %d seconds" % wait_time)
                if wait_time > 60:
                    logger.warning("Don't want to wait too long, aborting.")
                    break
                time.sleep(wait_time)

        if html_raw is None:
            logger.error("Aborting after %d attempts." % attempts)
            return None

        # Optionally unzip it.
        if html_encoding == 'gzip':
            html_raw = gzip.decompress(html_raw).decode()

        # Use BeautifulSoup to parse the HTML.
        logging.debug("Parsing '%s' html document (%d bytes)" % (url, len(html_raw)))
        html_doc = bs4.BeautifulSoup(
                html_raw,
                'lxml' if has_lxml else 'html5lib')

        # Look for title, description, and thumbnail image.
        # We first try OpenGraph info, fallback to Twitter info, and fallback
        # last on general HTML5 info.
        embed_title = _find_meta(html_doc, property="og:title")
        if not embed_title:
            embed_title = _find_meta(html_doc, name="twitter:title")
        if not embed_title:
            embed_title = html_doc.find("title").string

        if not embed_title:
            logger.error("Couldn't find title! Aborting making an embed.")
            return None

        embed_description = _find_meta(html_doc, property="og:description")
        if not embed_description:
            embed_description = _find_meta(html_doc, name="twitter:description")
        if not embed_description:
            embed_description = _find_meta(html_doc, name="description")
        if not embed_description:
            logger.warning("Couldn't find description, falling back to title.")
            embed_description = embed_title

        embed_image = _find_meta(html_doc, property="og:image")
        if not embed_image:
            embed_image = _find_meta(html_doc, name="twitter:image")
        if not embed_image:
            embed_image = _find_meta(html_doc, property="thumbnail")

        logger.debug(
                "Creating Bluesky embed with title '%s', description '%s', and "
                "image '%s'" % (embed_title, embed_description, embed_image))

        # Upload the thumbnail image to Bluesky.
        embed_thumb_blob = None
        if embed_image:
            try:
                thumb_req = _build_http_request(embed_image)
                with SignalTimeout(6, "urlopen timed out!") as sto:
                    with urlopen(thumb_req, timeout=5) as thumb_resp:
                        thumb_data = thumb_rest.read()
                        logger.debug(
                                "Uploading embed image '%s' to Bluesky (%d bytes)" %
                                (embed_image, len(thumb_data)))
                        embed_thumb_blob = self.client.com.atproto.repo.upload_blob(thumb_data)
            except Exception as ex:
                logger.warning(
                        "Couldn't fetch thumbnail URL '%s' to build Bluesky embed" %
                        embed_image)
                logger.warning(str(ex))

        # Make the embed!
        embed = atprotomodels.AppBskyEmbedExternal.Main(
                external=atprotomodels.AppBskyEmbedExternal.External(
                    title=embed_title,
                    description=embed_description,
                    uri=url,
                    thumb=embed_thumb_blob))
        return embed


def _build_http_request(url, headers=None):
    req = urllib.request.Request(url)
    req.add_header('User-Agent', _get_random_user_agent())
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    return req


class SignalTimeout:
    def __init__(self, seconds, error_message):
        self.seconds = seconds
        self.error_message = error_message

    def __enter__(self):
        signal.signal(signal.SIGALRM, self._onTimeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)

    def _onTimeout(self, signum, frame):
        raise TimeoutError(self.error_message)


_user_agents = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.3',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.1',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.3',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.3',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.3',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        ]

def _get_random_user_agent():
    return random.choice(_user_agents)


def _find_meta(html_doc, **kwargs):
    # Pass kwargs as a dictionary so we can also look for tags with a property
    # named 'name' without conflicting with the find() method's 'name' arg.
    meta_tag = html_doc.find("meta", dict(kwargs))
    return meta_tag["content"] if meta_tag else None


BLUESKY_NETLOC = 'bsky.app'

# Match both links to a profile by name, and by ID
profile_path_re = re.compile(r'/profile/([\w\d\.]+|(did\:plc\:[\w\d]+))')


class BlueskyUrlFlattener(UrlFlattener):
    def __init__(self):
        self.urls = []

    def replaceHref(self, text, raw_url, ctx):
        url = urllib.parse.urlparse(raw_url)

        # If this is a Bluesky profile URL, replace it with a mention.
        if url.netloc == BLUESKY_NETLOC:
            m = profile_path_re.match(url.path)
            if m:
                return '@' + m.group(1)

        # Otherwise, keep track of where the URL is so we can add a facet
        # for it.
        start = ctx.byte_length
        end = start + len(text.encode())
        self.urls.append((start, end, raw_url))

        # Always keep the text as-is.
        return text

    def measureUrl(self, url):
        return len(url)

    def reset(self):
        self.urls = []

