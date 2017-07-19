import yaml


CONFIG_GLOB = '/etc/cocaine/orca.yml'
CONFIG_USER = '~/.cocaine/orca.yml'
CONFIG_PATHS = [CONFIG_GLOB, CONFIG_USER]

DEFAULT_TOK_UPDATE_SEC = 10

#
# TODO: (cerberus) validator someday
#
# Should be compatible with tools secure section
#
class Config(object):

    def __init__(self):
        self._config = dict()

    def update(self, paths=CONFIG_PATHS):
        parsed = []
        for fl in paths:
            try:
                with open(fl) as fs:
                    self.config.update(yaml.safe_load(fs.read()))
                    parsed.append(fl)
            except Exception as err:
                print('failed to read config file {}'.format(fl))

        if not parsed:
            print('no config to read was found in file(s), using defaults.')

    @property
    def config(self):
        return self._config

    @property
    def secure(self):
        secure_conf = self._config.get('secure', {})

        mod = secure_conf.get('mod', 'promisc')

        client_id = secure_conf.get('client_id', 0)
        client_secret = secure_conf.get('client_secret', '')

        tok_update = secure_conf.get('tok_update_sec', DEFAULT_TOK_UPDATE_SEC)

        return mod, client_id, client_secret, tok_update
