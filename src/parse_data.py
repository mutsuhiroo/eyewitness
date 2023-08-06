import json
from datetime import datetime
from typing import Dict

from dateutil import tz
from yyjson import Document

from src.disaster import Disaster


class IncludesMapper:
    def __init__(self, includes: Dict):
        def init_map(includes: Dict, key: str, id_: str = "id"):
            if includes.get(key):
                return {u[id_]: u for u in includes[key]}
            return dict()

        self.id2user = init_map(includes, "users")
        self.id2media = init_map(includes, "media", id_="media_key")
        self.id2places = init_map(includes, "places")


class CustomTweetBuilder:
    def __init__(self, mapper: IncludesMapper):
        self.d = dict()
        self.mapper = mapper
        self.tz = tz.gettz("Asia/Tokyo")

    def add_default_info(self, d: Dict):
        self.d["text"] = d["text"]
        self.d["tweet_id"] = d["id"]
        self.d["created_at"] = (
            datetime.strptime(d["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
            .astimezone(self.tz)
            .isoformat()
        )

    def add_public_metrics(self, public_metrics: Dict):
        self.d["retweet_count"] = public_metrics.get("retweet_count")
        self.d["reply_count"] = public_metrics.get("reply_count")
        self.d["like_count"] = public_metrics.get("like_count")
        self.d["quote_count"] = public_metrics.get("quote_count")
        self.d["bookmark_count"] = public_metrics.get("bookmark_count")
        self.d["impression_count"] = public_metrics.get("impression_count")

    def add_user(self, author_id: str):
        self.d["user"] = self.mapper.id2user[author_id]

    def add_media(self, attachments: Dict):
        if not attachments or not attachments.get("media_keys"):
            self.d["media"] = []
            return
        media_keys = attachments.get("media_keys")
        self.d["media"] = [self.mapper.id2media[mk] for mk in media_keys]

    def add_place(self, geo: Dict):
        if not geo:
            self.d["place"] = dict()
            return
        id_ = geo["place_id"]
        self.d["place"] = geo
        place = self.mapper.id2places.get(id_)
        if place:
            self.d["place"].update()

    def add_entities(self, entities: Dict):
        elems = ["annotations", "hashtags", "urls"]
        # entitiesがないなら空で初期化
        if not entities:
            for elem in elems:
                self.d[elem] = list()
        else:
            # entitiesがあれば各属性を探す
            for elem in elems:
                eitities_elem = entities.get(elem)
                if not eitities_elem:
                    self.d[elem] = list()
                else:
                    self.d[elem] = eitities_elem

    def build(self):
        author_id = self.d["user"]["id"]
        tweet_id = self.d["tweet_id"]
        self.d["link"] = f"https://twitter.com/{author_id}/status/{tweet_id}"
        # maskしたテキストを挿入
        text = self.d.get("text")  # type: str
        for user in self.mapper.id2user.values():
            text = text.replace(f'@{user["username"]}', "@USERNAME")
        for u in self.d["urls"]:
            text = text.replace(u["url"], "<URL>")
        self.d["masked"] = text

        return json.dumps(self.d, ensure_ascii=False)


def parse_tweet(disaster: Disaster):
    input_file_path = disaster.output_dir_path.parent / "raw.jsonl"
    output_file_path = disaster.output_dir_path.parent / "parsed.jsonl"
    with open(output_file_path, "w") as out, open(input_file_path) as inp:
        for line in inp:
            doc = Document(line)
            try:
                d = doc.get_pointer("/data")
                if not d:
                    continue
                # mediaやuserの情報を管理するインスタンスを生成
                mapper = IncludesMapper(includes=doc.get_pointer("/includes"))
                builder = CustomTweetBuilder(mapper)
                builder.add_default_info(d)
                builder.add_public_metrics(public_metrics=d.get("public_metrics"))
                builder.add_entities(entities=d.get("entities"))
                builder.add_user(author_id=d["author_id"])
                builder.add_media(attachments=d.get("attachments"))
                builder.add_place(geo=d.get("geo"))
                tweet = builder.build()
                out.write(tweet)
                out.write("\n")
            except:
                print("expt")
                print(doc.get_pointer("/data"))
                print(json.dumps(doc.get_pointer("/includes")))
                print("---")


