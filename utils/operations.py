from anystore.settings import Settings
from banal import ensure_dict
from memorious.logic.context import Context
from servicelayer import env

from utils import Data, get_method
from utils.cache import CACHE, make_emit_cache_key

DEBUG = env.to_bool("DEBUG")
PROXY = env.get("MEMORIOUS_CRAWL_PROXY")

settings = Settings()


def init(context: Context, data: Data):
    """
    Set crawl proxy if not running in debug mode and add any context params to
    data dictionary
    """
    if not DEBUG and PROXY:
        context.http.reset()
        proxies = {"http": PROXY, "https": PROXY}
        context.http.session.proxies = proxies
        context.http.save()
    context.emit(data={**data, **ensure_dict(context.params)})


def cached_emit(context: Context, data: Data, rule: str | None = None):
    """
    Only emit (pass through next stage) if a cache key is not present yet. The
    cache key will be set in the last (store) stage.
    """
    if not settings.use_cache:
        context.emit(rule or "pass", data=data)
        return
    cache_key = make_emit_cache_key(context, data)
    if not cache_key or not CACHE.exists(cache_key):
        context.emit(rule or "pass", data=data)
        return
    context.log.info(f"Skipping emit cache key: `{cache_key}`")


def store(context: Context, data: Data):
    """
    An extended store to be able to set the emit cache key after successful
    store
    """
    handler = get_method(context.params.get("operation", "directory"))
    handler(context, data)
    cache_key = make_emit_cache_key(context, data)
    if cache_key:
        CACHE.touch(cache_key)
