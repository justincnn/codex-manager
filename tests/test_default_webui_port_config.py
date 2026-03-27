from src.config.constants import (
    DEFAULT_SETTINGS,
    DEFAULT_WEBUI_BASE_URL,
    DEFAULT_WEBUI_PORT,
    DEFAULT_WEBUI_WS_BASE_URL,
    OAUTH_REDIRECT_URI,
)
from src.config.settings import SETTING_DEFINITIONS, Settings


def test_default_webui_port_is_shared_from_one_constant():
    default_settings_map = {key: value for key, value, *_ in DEFAULT_SETTINGS}

    assert SETTING_DEFINITIONS["webui_port"].default_value == DEFAULT_WEBUI_PORT
    assert Settings().webui_port == DEFAULT_WEBUI_PORT
    assert default_settings_map["webui.port"] == str(DEFAULT_WEBUI_PORT)
    assert DEFAULT_WEBUI_BASE_URL == f"http://127.0.0.1:{DEFAULT_WEBUI_PORT}"
    assert DEFAULT_WEBUI_WS_BASE_URL == f"ws://127.0.0.1:{DEFAULT_WEBUI_PORT}"
    assert OAUTH_REDIRECT_URI == f"http://localhost:{DEFAULT_WEBUI_PORT}/auth/callback"
