import pathlib
from collections import defaultdict
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from tqdm import tqdm
from yyjson import Document

from src.disaster import Disaster


def get_raw(disaster: Disaster):
    """GoogleDriveからデータを取得"""
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    sa_creds = service_account.Credentials.from_service_account_file("credentials.json")
    scoped_creds = sa_creds.with_scopes(SCOPES)
    drive_service = build("drive", "v3", credentials=scoped_creds)
    path2id = defaultdict(set)

    page_token = None
    while True:
        response = (
            drive_service.files()
            .list(
                q=f"'{disaster.input_dir_id}' in parents",
                spaces="drive",
                fields="nextPageToken, files(id, name)",
                pageToken=page_token,
            )
            .execute()
        )
        for file in response.get("files", []):
            file_name = pathlib.Path(file.get("name"))
            crawled_datetime = datetime.strptime(
                file_name.stem.split("_")[1], "%Y-%m-%d-%H-%M"
            )
            if not disaster.start <= crawled_datetime < disaster.end:
                continue
            # ファイルをダウンロード
            print(f"Found file: {file.get('name')} ({file.get('id')})")
            request = drive_service.files().get_media(fileId=file.get("id"))
            output_file = disaster.output_dir_path / file.get("name")
            path2id[file.get("name")].add(file.get("id"))
            with open(output_file, "wb") as f:
                downloader = MediaIoBaseDownload(f, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                    print(f"Download {int(status.progress() * 100)}%.")

        page_token = response.get("nextPageToken", None)
        if page_token is None:
            break
    print(len(path2id))


def aggregate_raw(disaster: Disaster) -> pathlib.Path:
    """10分ないし、1時間ごとのデータを集約"""
    checked = set()
    raw_file_path = disaster.output_dir_path.parent / "raw.jsonl"
    with open(raw_file_path, "w") as out:
        file_paths = list(disaster.output_dir_path.glob("*.jsonl"))
        for file_path in tqdm(file_paths, total=len(file_paths)):
            # ファイルをひとづつ開いてチェック
            with open(file_path) as inp:
                for line in inp:
                    doc = Document(line)
                    tweet_id = doc.get_pointer("/data/id")
                    # すでに処理済みIDはスキップ
                    if tweet_id in checked:
                        continue
                    text = doc.get_pointer("/data/text")
                    # RTもしくはクエリを含まないツイートはスキップ
                    if text.startswith("RT ") or disaster.query not in text:
                        continue
                    # 言語が日本語でない場合はスキップ
                    lang = doc.get_pointer("/data/lang")
                    if lang != "ja":
                        continue
                    out.write(doc.dumps())
                    out.write("\n")
                    checked.add(tweet_id)
    return raw_file_path


