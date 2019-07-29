import socket
import warnings
from collections import OrderedDict

from django.core.cache import caches

import nexus

from nexus_memcache import conf

def parse_backend_uri(backend_uri):
    """
    Converts the "backend_uri" into a cache scheme ('db', 'memcached', etc), a
    host and any extra params that are required for the backend. Returns a
    (scheme, host, params) tuple.
    """
    from django.core.cache.backends.base import InvalidCacheBackendError
    try:
        from urllib.parse import parse_qsl
    except:
        from urlparse import parse_qsl

    if backend_uri.find(':') == -1:
        raise InvalidCacheBackendError("Backend URI must start with scheme://")
    scheme, rest = backend_uri.split(':', 1)
    if not rest.startswith('//'):
        raise InvalidCacheBackendError("Backend URI must start with scheme://")

    host = rest[2:]
    qpos = rest.find('?')
    if qpos != -1:
        params = dict(parse_qsl(rest[qpos+1:]))
        host = rest[2:qpos]
    else:
        params = {}
    if host.endswith('/'):
        host = host[:-1]

    return scheme, host, params


class MemcacheModule(nexus.NexusModule):
    home_url = 'index'
    name = 'memcache'

    def get_caches(self):
        caches = []
        schema, hosts, params = parse_backend_uri(conf.BACKEND)
        for host in hosts.split(';'):
            try:
                caches.append((host, caches['%s://%s?%s' % (schema, host, params)]._cache))
            except Exception as e:
                self.logger.exception(e)
        return caches

    def get_stats(self, timeout=5):
        for host, cache in self.get_caches():
            default_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(timeout)
            try:
                stats = cache.get_stats()[0][1]
            except:
                stats = {'online': 0}
            else:
                stats['online'] = 1
            finally:
                socket.setdefaulttimeout(default_timeout)
            yield host, stats

    def get_title(self):
        return 'Memcache'

    def get_urls(self):
        from django.conf.urls import url

        urlpatterns = [
            url(r'^$', self.as_view(self.index), name='index'),
        ]

        return urlpatterns

    def render_on_dashboard(self, request):
        try:
            cache_stats = list(self.get_stats())
        except AttributeError:
            warnings.warn('`get_stats()` not found on cache backend')
            cache_stats = []

        global_stats = {
            'bytes': 0,
            'limit_maxbytes': 0,
            'curr_items': 0,
            'curr_connections': 0,
            'total_connections': 0,
            'total_items': 0,
            'cmd_get': 0,
            'get_hits': 0,
            'get_misses': 0,
            'rusage_system': 0,
            'online': 0,
        }
        for host, stats in cache_stats:
            for k in global_stats.keys():
                global_stats[k] += float(stats.get(k, 0))
        global_stats['total'] = len(cache_stats)

        return self.render_to_string('nexus/memcache/dashboard.html', {
            'global_stats': global_stats,
        })

    def index(self, request):
        try:
            cache_stats = ((k, OrderedDict(sorted(v.items(), key=lambda x: x[0]))) for k, v in self.get_stats())
        except AttributeError:
            cache_stats = []

        return self.render_to_response("nexus/memcache/index.html", {
            'cache_stats': cache_stats,
        }, request)
nexus.site.register(MemcacheModule, 'memcache', category='cache')
