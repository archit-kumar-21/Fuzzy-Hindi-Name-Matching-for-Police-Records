# matching/matcher.py
from rapidfuzz import process, fuzz

class FuzzyHindiMatcher:
    def __init__(self, df, english_col='Victim Name', hindi_col='Hindi Name', gender_col=None):
        """
        df: pandas DataFrame
        english_col: column name for English names
        hindi_col: column name for Hindi names
        gender_col: optional gender column name
        """
        self.df = df.copy()
        self.english_col = english_col
        self.hindi_col = hindi_col
        self.gender_col = gender_col

        # Build search lists (tuples: (combined_key, index))
        # Combined key includes English + Hindi to allow searching either script
        self.keys = []
        for idx, row in self.df.iterrows():
            eng = str(row.get(self.english_col, "")).strip()
            hin = str(row.get(self.hindi_col, "")).strip()
            combined = " | ".join([k for k in [eng, hin] if k])
            self.keys.append((combined, idx))

        self.key_strings = [k[0] for k in self.keys]

    def search(self, query, top_k=10, min_score=60):
        """
        query: user query (Hindi or English)
        top_k: number of results
        min_score: minimum similarity (0-100)
        returns list of dicts with keys:
          EnglishName, HindiName, Gender, Score, RowIndex
        """
        if not query or str(query).strip() == "":
            return []

        query = str(query).strip()

        # Use WRatio which combines token sort/partial/etc under the hood for best results
        matches = process.extract(
            query,
            self.key_strings,
            scorer=fuzz.WRatio,
            limit=top_k*3  # get more and filter
        )

        results = []
        seen_idx = set()
        for match_str, score, match_pos in matches:
            if score < min_score:
                continue
            # find index for this combined string
            # we stored key_strings in same order, so match_pos is index
            idx = match_pos
            if idx in seen_idx:
                continue
            seen_idx.add(idx)
            row = self.df.iloc[idx]
            res = {
                "EnglishName": row.get(self.english_col, ""),
                "HindiName": row.get(self.hindi_col, ""),
                "Gender": row.get(self.gender_col, "") if self.gender_col else "",
                "Score": int(score),
                "RowIndex": int(idx)
            }
            results.append(res)
            if len(results) >= top_k:
                break
        return results
