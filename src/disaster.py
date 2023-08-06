import pathlib
from dataclasses import dataclass
from datetime import datetime

import dateparser
from dateutil.relativedelta import relativedelta


@dataclass
class Disaster:
    """災害ごとにデータを作成するための設定を保持するクラス"""

    start: datetime
    end: datetime
    prefix: str
    output_dir_path: pathlib.Path
    input_dir_id: str
    query: str


def build_disaster(anchor: str, prefix: str, input_dir_id: str, query: str):
    """Disasterインスタンスの生成関数
    災害発生時に起点となる時間から前後数時間の幅をもたせ開始・終了時刻を決定
    データのフィルタリング用に単一の文字列を受け取る"""
    anchor = dateparser.parse(anchor)
    start = anchor - relativedelta(hours=12)
    end = anchor + relativedelta(hours=24)
    base_dir = pathlib.Path(f"./data/{prefix}")
    output_dir = base_dir / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    return Disaster(start, end, prefix, output_dir, input_dir_id, query)


def get_build_conf(name: str) -> Disaster:
    """実験用に特定のデータソースから各災害ごとにプリセットの設定を提供する
    今回は以下の通り
    地震　令和5年奥能登地震
    豪雨　令和4年8月豪雨
    大雪　令和4年12月大雪
    台風　令和4年台風15号
    山火事　令和5年霧ヶ峰で発生した山火事
    """
    name2conf = {
        "earthquake": build_disaster(
            anchor="2023-05-05 14:40",
            prefix="earthquake",
            input_dir_id="1FiSAzHCuFGCvCIEomNKmbSlHT92sfGIx",
            query="地震",
        ),
        "heavy_rain": build_disaster(
            anchor="2022-08-03 19:10",
            prefix="heavy_rain",
            input_dir_id="1-7GauGOPH44tEdUs4ckOS8ODdVEQgAFB",
            query="大雨",
        ),
        "heavy_snow": build_disaster(
            anchor="2022-12-18 21:20",
            prefix="heavy_snow",
            input_dir_id="1SbjL7zceMrAxQdnbJV5SzQb_eqovHda0",
            query="大雪",
        ),
        "typhoon": build_disaster(
            anchor="2022-09-17 21:40",
            prefix="typhoon",
            input_dir_id="1e_k-wg2QtAbuPpK0xvrda7O-xrUxeBdV",
            query="台風",
        ),
        "wildfire": build_disaster(
            anchor="2023-05-04 13:30",
            prefix="wildfire",
            input_dir_id="1S1_HV2AGUde9LD5PrtPDnMF_T2_NzjUD",
            query="山火事",
        ),
    }
    return name2conf[name]
