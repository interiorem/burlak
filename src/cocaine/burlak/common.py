"""Project wide utilities.

TODO: move common utilities from project here.

"""


def clamp(v, low=0., hi=1.):
    """Classical math `clamp` implementation.

    :return: v if low < v < hi, else low if v <= low, else hi if v >= hi

    """
    return min(max(v, low), hi)
