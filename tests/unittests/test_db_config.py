from sarc.config import DbConfig


def test_engine_url_with_port():
    url = DbConfig(host="myhost", name="mydb", user="myuser", port=6543).engine.url
    assert (url.host, url.port, url.username) == ("myhost", 6543, "myuser")


def test_engine_url_without_port():
    url = DbConfig(host="myhost", name="mydb", user="myuser").engine.url
    assert url.port is None
