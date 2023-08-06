import pandas

from src.disaster import Disaster


def sample_by(disaster: Disaster, n=4200, n_lines_each_iter=10000):
    input_file_path = disaster.output_dir_path.parent / "parsed.jsonl"
    output_file_path = disaster.output_dir_path.parent / "sampled.jsonl"

    # ファイルが大きすぎる場合はSIGKILLで落ちるため分割してサンプリングする。
    buff = list()
    with pandas.read_json(
        input_file_path, lines=True, orient="records", chunksize=n_lines_each_iter
    ) as reader:
        # chunkに分割してサンプリングする、完全なランダムサンプリングではないことに注意
        for chunk in reader:
            buff.append(chunk.sample(min(n, len(chunk)), random_state=198818))
    merged = pandas.concat(buff)
    deduped = merged.drop_duplicates("masked").sort_values("created_at")
    deduped["masked_len"] = deduped["masked"].apply(lambda x: len(x))
    filtered_by_len = deduped[deduped["masked_len"] <= 280]
    sampled = filtered_by_len.sample(n, random_state=198818)
    sampled.to_json(
        output_file_path,
        lines=True,
        orient="records",
        force_ascii=False,
        date_format="iso",
    )
