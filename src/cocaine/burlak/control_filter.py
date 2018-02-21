
from .defaults import Defaults


class ControlFilter(object):

    def __init__(self, apply_control, white_list):
        self._apply_control = apply_control
        self._white_list = white_list

    @property
    def apply_control(self):
        return self._apply_control

    @property
    def white_list(self):
        return self._white_list

    def as_dict(self):
        return dict(
            control_filter=dict(
                apply_control=self.apply_control,
                white_list=self.white_list,
            )
        )

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    @staticmethod
    def from_dict(d):
        '''Creates control filter from dict with reasonable defaults
        '''
        apply_control = d.get('apply_control', Defaults.APPLY_CONTROL)
        white_list = d.get('white_list', [])

        return ControlFilter(apply_control, white_list)

    @staticmethod
    def with_defaults():
        '''Creates control filter from dict with reasonable defaults
        '''
        return ControlFilter.from_dict(dict())
