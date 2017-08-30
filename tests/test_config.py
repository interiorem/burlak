from cocaine.burlak import Config

import pytest


good_secret_conf = [
    ('tests/assets/conf1.yaml',
        'test1', 100500, 'top secret', Config.DEFAULT_TOK_UPDATE_SEC),
    ('tests/assets/conf2.yaml',
        'tvm', 42, 'not as secret at all', 600),
]

empty_conf = 'tests/assets/empty.conf.yaml'

default_secure = ('promisc', 0, '', Config.DEFAULT_TOK_UPDATE_SEC)


@pytest.mark.parametrize(
    'config_file,mod,cid,secret,update', good_secret_conf)
def test_secure_config(config_file, mod, cid, secret, update):
    cfg = Config()
    cnt = cfg.update([config_file])

    assert cfg.secure == (mod, cid, secret, update)
    assert cnt == 1


def test_config_group():
    cfg = Config()
    cnt = cfg.update([conf for conf, _, _, _, _ in good_secret_conf])

    assert cfg.secure == (good_secret_conf[-1][1:])
    assert cnt == len(good_secret_conf)


def test_broken_conf():
    cfg = Config()
    cnt = cfg.update([empty_conf])

    assert cnt == 1


def test_config_group_with_broken():
    conf_files = [conf for conf, _, _, _, _ in good_secret_conf]
    conf_files.append(empty_conf)

    cfg = Config()
    cnt = cfg.update(conf_files)

    assert cfg.secure == (good_secret_conf[-1][1:])
    assert cnt == len(good_secret_conf) + 1


def test_config_group_with_broken_and_noexist():
    conf_files = [empty_conf, 'boo/foo.yml']

    cfg = Config()
    cnt = cfg.update(conf_files)

    assert cnt == 1
    assert cfg.secure == default_secure


def test_empty_config():
    cfg = Config()
    assert cfg.secure == default_secure


@pytest.mark.parametrize(
    'config,expect_port,expect_web_path,expect_uuid_path',
    [
        ('tests/assets/conf1.yaml', 100500, '', '/state'),
        ('tests/assets/conf2.yaml', 8877, '/to-heaven', '/some/deep/location'),
    ]
)
def test_endpoints_options(
        config, expect_port, expect_web_path, expect_uuid_path):

    cfg = Config()
    cfg.update([config])

    assert (expect_port, expect_web_path) == cfg.endpoint
    assert expect_uuid_path == cfg.uuid_path


@pytest.mark.parametrize(
    'config,expect_unicorn_name,expect_node_name',
    [
        ('tests/assets/conf1.yaml', 'unicorn', 'some_other_node'),
        ('tests/assets/conf2.yaml', 'big_unicorn', 'node'),
    ]
)
def test_service_names(config, expect_unicorn_name, expect_node_name):
    cfg = Config()
    cfg.update([config])

    assert expect_unicorn_name == cfg.unicorn_name
    assert expect_node_name == cfg.node_name


@pytest.mark.parametrize(
    'config,expect_profile',
    [
        ('tests/assets/conf1.yaml', 'default'),
        ('tests/assets/conf2.yaml', 'isolate_profile'),
    ]
)
def test_misc_options(config, expect_profile):
    cfg = Config()
    cfg.update([config])

    assert cfg.default_profile == expect_profile
