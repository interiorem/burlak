import yaml


CONFIG_GLOB = '/etc/cocaine/.cocaine/tools.yml'
CONFIG_USER = '~/.cocaine/tools.yml'
CONFIG_PATHS = [CONFIG_GLOB, CONFIG_USER]


class Config(object):

    def __init__(self):
        self._config = dict()

    def update(self, paths=CONFIG_PATHS):
        for fl in paths:
            try:
                with open(fl) as fs:
                    self.config.update(yaml.safe_load(fs.read()))
            except Exception as err:
                print('failed to read config file {}'.format(fl))

    @property
    def config(self):
        return self._config

    @property
    def secure(self):
        secure_conf = self._config.get('secure', {})

        mod = secure_conf.get('mod', 'promisc')

        client_id = secure_conf.get('client_id', 0)
        client_secret = secure_conf.get('client_secret', '')

        return mod, client_id, client_secret
