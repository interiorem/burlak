"""Exponential Weighted Moving Avarage.

Link: https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
"""


class EWMA(object):
    """Exponential Weighted Moving Avarage."""

    def __init__(self, alpha=.7, init=0):
        """EWMA.

        :param alpha: degree of weighting decrease in interval [0, 1]
        :type alpha: float

        :param init: initial sequcne value, 0 is ok
        :type init: float | int | long
        """
        if alpha < 0. or alpha > 1.:
            raise ValueError(
                'incorrect `alpha` parameter: {}, should in [0, 1] range',
                alpha
            )

        self._alpha = alpha
        self._s = init

    def update(self, value):
        """Recalculate moving avarage."""
        self._s = self._alpha * value + (1 - self._alpha) * self._s

    @property
    def value(self):
        """Get current moving avarage."""
        return self._s
