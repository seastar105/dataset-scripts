import json
import argparse
from pathlib import Path
import ray
import io
from functools import partial
from audiotools import AudioSignal
import re
import soundfile


def replace_double_term(text):
    pattern = r'\(@([^@)]+)\)/\([^)]*\)'
    return re.sub(pattern, r'\1', text)

def replace_single_term(text):
    pattern = re.compile(r"\(@([^)]+)\)")
    
    def replacement(match):
        return match.group(1)
    
    return pattern.sub(replacement, text)

def check_if_uncertain(text):
    pattern = r"\(\([^)]*\)\)"
    matches = re.findall(pattern, text)
    return len(matches) > 0

def check_if_anonymized(text):
    pattern = r"&.*?&"
    matches = re.findall(pattern, text)
    return len(matches) > 0

def remove_aux(text):
    text = text.replace("/(bgm)", "").replace("/(noise)", "")
    return text

def to_written_form(text):
    return re.sub(r'\(([^)]+)\)\/\(([^)]+)\)', r'\2', text)

def to_phonetic_form(text):
    return re.sub(r'\(([^)]+)\)\/\(([^)]+)\)', r'\1', text)

def remove_event(text):
    text = text.replace("@웃음", "").replace("@박수", "").replace("@목청", "").replace("@노래", "")
    return text

def process_utterance(utt):
    original_form = utt["original_form"]
    if check_if_anonymized(original_form):
        return None
    form = utt["form"]
    if check_if_uncertain(form):
        return None
    repl = replace_double_term(form)
    repl = replace_single_term(repl)
    repl = remove_aux(repl)
    repl = remove_event(repl)
    
    start = float(utt["start"])
    end = float(utt["end"])
    
    return {
        "text": to_written_form(repl),
        "tn_text": to_phonetic_form(repl),
        "original_form": original_form,
        "start": start,
        "end": end
    }


class SegmentActor:
    def __call__(self, item):
        audio_path = item["audio_path"]
        json_path = item["json_path"]
        
        with open(json_path) as f:
            data = json.load(f)
        
        result = []
        signal = AudioSignal(audio_path)
        stem = Path(audio_path).stem
        sample_rate = signal.sample_rate
        for utt_idx, utt in enumerate(data["utterance"]):
            processed_utt = process_utterance(utt)
            if processed_utt is None:
                continue
            
            start = max(0, int(processed_utt.pop("start") * sample_rate))
            end = min(signal.shape[-1], int(processed_utt.pop("end") * sample_rate))
            audio = signal.audio_data[..., start:end].squeeze().numpy()
            
            wav_bytes = io.BytesIO()
            soundfile.write(wav_bytes, audio, sample_rate, format="wav")
            wav_bytes.seek(0)
            wav_bytes = wav_bytes.getvalue()
            
            meta = processed_utt
            key = f"{stem}_{utt_idx:03d}"
            
            result.append({
                "wav": wav_bytes,
                "json": json.dumps(meta, ensure_ascii=False),
                "__key__": key
            })
        return result



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Write to a webdataset')
    
    parser.add_argument("--input_root", type=str, required=True, help="Path to the input jsonl file")
    parser.add_argument("--output_root", type=str, required=True, help="Path to the output root directory")
    parser.add_argument("--min_rows_per_file", type=int, default=10000, help="Minimum number of rows per file")
    
    args = parser.parse_args()
    
    with open(args.input_jsonl, "r") as f:
        data = [json.loads(line) for line in f]
    
    Path(args.output_root).mkdir(parents=True, exist_ok=True)
    if args.save_mp3:
        audio_export_kwargs = {"format": "mp3", "bitrate": "128k"}
    else:
        audio_export_kwargs = {"format": "wav"}
    segment_convert = partial(segment_convert, **audio_export_kwargs)
    convert_fn = segment_convert if args.segment else convert
    
    dataset = ray.data.from_items(data).map(convert_fn).write_webdataset(args.output_root, min_rows_per_file=args.min_rows_per_file)
