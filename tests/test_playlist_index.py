import sqlite3

from yt_fts import db_utils
from yt_fts.download import download_handler as download_module


def test_seed_playlist_index_from_existing_videos(tmp_path, monkeypatch):
    db_path = tmp_path / "subtitles.db"
    db_utils.make_db(str(db_path))
    monkeypatch.setattr(db_utils, "get_db_path", lambda: str(db_path))

    db_utils.add_channel_info("channel-1", "Channel 1", "https://www.youtube.com/channel/channel-1/videos")
    db_utils.add_video("channel-1", "video-a", "Video A", "https://youtu.be/video-a", "2026-03-06")
    db_utils.add_video("channel-1", "video-b", "Video B", "https://youtu.be/video-b", "2026-03-06")

    seeded_video_ids = db_utils.seed_playlist_index(
        "PL123",
        "https://www.youtube.com/playlist?list=PL123",
        ["video-a", "video-c", "video-b"],
    )

    assert seeded_video_ids == {"video-a", "video-b"}
    assert db_utils.get_indexed_video_ids_for_playlist("PL123") == {"video-a", "video-b"}

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    playlist_rows = cur.execute("SELECT playlist_id, playlist_url FROM Playlists").fetchall()
    conn.close()

    assert playlist_rows == [("PL123", "https://www.youtube.com/playlist?list=PL123")]


def test_download_playlist_skips_already_indexed_videos(monkeypatch):
    handler = download_module.DownloadHandler(number_of_jobs=1, language="en")
    captured: dict[str, list[str]] = {}

    monkeypatch.setattr(
        handler,
        "get_playlist_data",
        lambda playlist_url: [
            {
                "channel_name": "Channel",
                "channel_id": "channel-1",
                "channel_url": "https://www.youtube.com/channel/channel-1/videos",
                "video_id": "video-a",
                "video_url": "https://youtu.be/video-a",
            },
            {
                "channel_name": "Channel",
                "channel_id": "channel-1",
                "channel_url": "https://www.youtube.com/channel/channel-1/videos",
                "video_id": "video-b",
                "video_url": "https://youtu.be/video-b",
            },
            {
                "channel_name": "Channel",
                "channel_id": "channel-1",
                "channel_url": "https://www.youtube.com/channel/channel-1/videos",
                "video_id": "video-c",
                "video_url": "https://youtu.be/video-c",
            },
        ],
    )
    monkeypatch.setattr(handler, "download_vtts", lambda: None)
    monkeypatch.setattr(handler, "vtt_to_db", lambda: captured.setdefault("video_ids", list(handler.video_ids or [])))
    monkeypatch.setattr(download_module, "get_playlist_id_from_url", lambda playlist_url: "PL123")
    monkeypatch.setattr(download_module, "upsert_playlist", lambda playlist_id, playlist_url: None)
    monkeypatch.setattr(download_module, "get_indexed_video_ids_for_playlist", lambda playlist_id: {"video-a"})
    monkeypatch.setattr(download_module, "seed_playlist_index", lambda playlist_id, playlist_url, video_ids: set())
    monkeypatch.setattr(download_module, "check_if_channel_exists", lambda channel_id: True)
    monkeypatch.setattr(download_module, "add_channel_info", lambda channel_id, channel_name, channel_url: None)

    handler.download_playlist("https://www.youtube.com/playlist?list=PL123", "en", 1)

    assert captured["video_ids"] == ["video-b", "video-c"]
