from src.storage.client import LocalS3Client, get_storage_client


def test_local_client_put_get():
    s3 = get_storage_client()
    assert isinstance(s3, LocalS3Client)

    s3.put_object("test/unit_test.txt", b"hello world")
    assert s3.exists("test/unit_test.txt")
    assert s3.get_object("test/unit_test.txt") == b"hello world"
    s3.delete_object("test/unit_test.txt")
    assert not s3.exists("test/unit_test.txt")


def test_local_client_list_objects():
    s3 = get_storage_client()
    s3.put_object("test/list/a.txt", b"a")
    s3.put_object("test/list/b.txt", b"b")

    keys = s3.list_objects("test/list/")
    assert "test/list/a.txt" in keys
    assert "test/list/b.txt" in keys

    s3.delete_object("test/list/a.txt")
    s3.delete_object("test/list/b.txt")
