# from https://github.com/sooftware/ksponspeech/blob/master/preprocess/preprocess.py
import re
import argparse
from pathlib import Path
import json
from tqdm.auto import tqdm


def bracket_filter(sentence, mode='phonetic'):
    new_sentence = str()

    if mode == 'phonetic':
        flag = False

        for ch in sentence:
            if ch == '(' and flag is False:
                flag = True
                continue
            if ch == '(' and flag is True:
                flag = False
                continue
            if ch != ')' and flag is False:
                new_sentence += ch

    elif mode == 'spelling':
        flag = True

        for ch in sentence:
            if ch == '(':
                continue
            if ch == ')':
                if flag is True:
                    flag = False
                    continue
                else:
                    flag = True
                    continue
            if ch != ')' and flag is True:
                new_sentence += ch

    else:
        raise ValueError("Unsupported mode : {0}".format(mode))

    return new_sentence


def special_filter(sentence, mode='phonetic', replace=None):
    SENTENCE_MARK = ['?', '!', '.']
    NOISE = ['o', 'n', 'u', 'b', 'l']
    EXCEPT = ['/', '+', '*', '-', '@', '$', '^', '&', '[', ']', '=', ':', ';', ',']

    new_sentence = str()
    for idx, ch in enumerate(sentence):
        if ch not in SENTENCE_MARK:
            if idx + 1 < len(sentence) and ch in NOISE and sentence[idx + 1] == '/':
                continue

        if ch == '#':
            new_sentence += '샾'

        elif ch == '%':
            if mode == 'phonetic':
                new_sentence += replace
            elif mode == 'spelling':
                new_sentence += '%'

        elif ch not in EXCEPT:
            new_sentence += ch

    pattern = re.compile(r'\s\s+')
    new_sentence = re.sub(pattern, ' ', new_sentence.strip())
    return new_sentence

PERCENT_FILES = {
    '087797': '퍼센트',
    '215401': '퍼센트',
    '284574': '퍼센트',
    '397184': '퍼센트',
    '501006': '프로',
    '502173': '프로',
    '542363': '프로',
    '581483': '퍼센트'
}

def sentence_filter(raw_sentence, mode, replace=None):
    return special_filter(bracket_filter(raw_sentence, mode), mode, replace)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--script_path", required=True, type=str)
    parser.add_argument("--audio_root", required=True, type=str)
    parser.add_argument("--audio_ext", type=str, default="flac")
    
    args = parser.parse_args()
    
    with open(args.script_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    
    samples = []
    audio_root = Path(args.audio_root)
    for line in tqdm(lines):
        name, label = line.strip().split(" :: ")
        stem = Path(name).stem
        replace = None
        if stem[-6:] in PERCENT_FILES:
            replace = PERCENT_FILES[stem[-6:]]
        text = sentence_filter(label, "spelling", replace)
        tn_text = sentence_filter(label, "phonetic", replace)
        audio_path = audio_root / f"{stem}.{args.audio_ext}"
        assert audio_path.exists(), f"{audio_path} does not exist"
        samples.append({
            "audio_path": str(audio_path),
            "text": text,
            "tn_text": tn_text,
            "raw_text": label.strip()
        })
    
    with open(f"{args.split}.jsonl", "w", encoding="utf-8") as f:
        for sample in samples:
            print(json.dumps(sample, ensure_ascii=False), file=f)
