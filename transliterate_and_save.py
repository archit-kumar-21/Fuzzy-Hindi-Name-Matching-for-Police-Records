# transliterate_and_save.py
# Recommended: run inside your conda env (aenv) after installing dependencies:
# pip install googletrans==4.0.0-rc1 indic-transliteration tqdm openpyxl pandas

import os, time, random
import pandas as pd
from tqdm import tqdm

# try to import googletrans
try:
    from googletrans import Translator
    google_available = True
    translator = Translator()
except Exception:
    google_available = False

# fallback offline transliteration
try:
    from indic_transliteration import sanscript
    from indic_transliteration.sanscript import transliterate as indic_transliterate
    indic_available = True
except Exception:
    indic_available = False

DATA_PATH = os.path.join("data", "MinorProjectDataSet.xlsx")

# manual corrections for names that often transliterate poorly
MANUAL_FIXES = {
    # "Original English": "Correct Hindi",
    "Bablu": "बब्लू",   # example; extend as you see wrong outputs
    "Bhavya Kaushik": "भव्य कौशिक",
    # add more fixes as you discover them
}

# simple cache to avoid repeat web calls (keeps in-memory; we also save to disk after)
CACHE_FILE = os.path.join("data", "translit_cache.csv")

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            cdf = pd.read_csv(CACHE_FILE, dtype=str).fillna("")
            return dict(zip(cdf['eng'].astype(str), cdf['hin'].astype(str)))
        except Exception:
            return {}
    return {}

def save_cache(cache):
    try:
        cdf = pd.DataFrame([{"eng":k, "hin":v} for k,v in cache.items()])
        cdf.to_csv(CACHE_FILE, index=False, encoding="utf-8-sig")
    except Exception as e:
        print("Warning: failed to save cache:", e)

cache = load_cache()

def transliterate_name_google(name, retries=2):
    """Use googletrans to get Hindi text for a name. Retries on failure."""
    if not google_available:
        return None
    try:
        # googletrans may occasionally throw; wrap in try/except
        res = translator.translate(name, src='en', dest='hi')
        text = res.text.strip()
        # sometimes google returns the same string in Latin -> if so, return None to fallback
        return text if text else None
    except Exception:
        if retries > 0:
            time.sleep(0.2)
            return transliterate_name_google(name, retries-1)
        return None

def transliterate_name_indic(name):
    """Offline transliteration fallback (may be less natural for plain English names)."""
    if not indic_available:
        return None
    try:
        # indic_transliterate expects scheme input; still can give reasonable output as fallback
        return indic_transliterate(name, sanscript.ITRANS, sanscript.DEVANAGARI)
    except Exception:
        return None

def transliterate(name):
    """Main wrapper: check manual fixes, cache, google, fallback to indic, fallback to original."""
    if not isinstance(name, str) or not name.strip():
        return ""
    name = name.strip()
    # manual fixes first
    if name in MANUAL_FIXES:
        return MANUAL_FIXES[name]

    # cache
    if name in cache and cache[name].strip():
        return cache[name]

    # try google first
    if google_available:
        hin = transliterate_name_google(name)
        if hin:
            cache[name] = hin
            return hin

    # fallback to offline indic transliteration
    if indic_available:
        hin = transliterate_name_indic(name)
        if hin:
            cache[name] = hin
            return hin

    # last-resort: basic heuristic mapping (very naive) — return original
    cache[name] = name
    return name

def main():
    if not os.path.exists(DATA_PATH):
        print("ERROR: dataset not found at", DATA_PATH)
        return

    print("Reading Excel...")
    df = pd.read_excel(DATA_PATH, engine='openpyxl', dtype=str).fillna("")

    if 'Victim Name' not in df.columns:
        print("ERROR: 'Victim Name' column not found. Columns:", df.columns.tolist())
        return

    if 'Hindi Name' not in df.columns:
        df['Hindi Name'] = ""

    # Build list of rows we will transliterate (only blanks or whitespace)
    to_process_idx = df.index[df['Hindi Name'].astype(str).str.strip() == ""].tolist()
    total = len(to_process_idx)
    print(f"Will transliterate {total} rows (blank Hindi Name entries).")

    if total == 0:
        print("Nothing to do — Hindi Name column already filled.")
        return

    # iterate with progress bar and small sleep to avoid rate-limits
    tqdm.pandas()
    for i in tqdm(to_process_idx, desc="Transliterating"):
        eng = str(df.at[i, 'Victim Name']).strip()
        if not eng:
            df.at[i, 'Hindi Name'] = ""
            continue
        try:
            hin = transliterate(eng)
            df.at[i, 'Hindi Name'] = hin
        except Exception as e:
            print("Error transliterating row", i, eng, e)
            df.at[i, 'Hindi Name'] = eng

        # small sleep only when using google to avoid throttling
        if google_available:
            time.sleep(0.05)  # adjust to 0.05-0.2 if you see rate-limits

    # Save cache
    save_cache(cache)

    # Save backup and overwrite original (valid xlsx backup)
    backup = DATA_PATH.replace(".xlsx", "_backup.xlsx")
    print("Creating backup:", backup)
    df.to_excel(backup, index=False, engine='openpyxl')

    print("Saving updated Excel to:", DATA_PATH)
    df.to_excel(DATA_PATH, index=False, engine='openpyxl')

    # Print sample rows for quick verification
    print("\nSample rows (10 random) after transliteration:")
    sample = df.sample(n=min(10, len(df)), random_state=2)[['Victim Name', 'Hindi Name']].reset_index(drop=True)
    print(sample.to_string(index=False))

    print("\nDone ✅. If outputs look wrong, add manual fixes to MANUAL_FIXES at top and re-run.")

if __name__ == "__main__":
    main()
