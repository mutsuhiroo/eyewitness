from src.disaster import get_build_conf
from src.parse_data import parse_tweet
from src.raw_data import get_raw, aggregate_raw
from src.sample_data import sample_by

conf = get_build_conf("typhoon")
print("start get raw")
if not conf.output_dir_path.exists():
    get_raw(conf)
print("aggregate raw")
if not (conf.output_dir_path.parent / "raw.jsonl").exists():
    aggregate_raw(conf)
print("start parse tweet")
if not (conf.output_dir_path.parent / "parsed.jsonl").exists():
    parse_tweet(conf)
print("start sample")
if not (conf.output_dir_path.parent / "sampled.jsonl").exists():
    sample_by(conf)
