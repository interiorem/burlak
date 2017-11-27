#
# System/process metrics gatherer
#
import os
import resource
from collections import namedtuple

from tornado import gen

from .loop_sentry import LoopSentry


RUsage = namedtuple('RUsage', [
    'maxrss_mb',
    'utime',
    'stime',
])


def _get_rusage_partly():
    usage = resource.getrusage(resource.RUSAGE_SELF)

    return RUsage(
        usage.ru_maxrss / 1024.0,  # kB according man page,
        usage.ru_utime,
        usage.ru_stime,
    )


class SysMetricsGatherer(LoopSentry):

    GATHER_INTERVAL_SEC = 0.5

    def __init__(self, **kwargs):
        super(SysMetricsGatherer, self).__init__(**kwargs)

        self.rusage = RUsage(0.0, 0.0, 0.0)
        self.load_avg = [0.0 for _ in xrange(0, 3)]

    @gen.coroutine
    def gather(self):
        while(self.should_run()):
            self.rusage = _get_rusage_partly()
            self.load_avg = os.getloadavg()

            yield gen.sleep(SysMetricsGatherer.GATHER_INTERVAL_SEC)

    def as_dict(self):
        return dict(
            load_avg=self.load_avg,
            maxrss_mb=self.rusage.maxrss_mb,
            utime=self.rusage.utime,
            stime=self.rusage.stime,
        )
