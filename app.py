from flask import Flask, render_template, request, jsonify
import pandas as pd
from rapidfuzz import fuzz
import webbrowser
from threading import Timer
import os
from collections import Counter
import json

app = Flask(__name__)

# -------------------- LOAD DATA --------------------
DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "MinorProjectDataSet_backup.xlsx")
df = pd.read_excel(DATA_PATH)
df.columns = [c.strip() for c in df.columns]

# -------------------- COLUMN DETECTION --------------------
def get_col(options):
    for c in df.columns:
        if c.lower() in [o.lower() for o in options]:
            return c
    return None

col_name = get_col(["Victim Name", "Name"])
col_hindi = get_col(["Hindi Name"])
col_gender = get_col(["Victim Gender", "Gender"])
col_age = get_col(["Victim Age", "Age"])
col_city = get_col(["City", "District"])
col_crime = get_col(["Crime Domain", "Crime Type"])
col_code = get_col(["Crime Code"])
col_closed = get_col(["Case Closed", "Case_Status"])

# -------------------- INDIA CITY COORDINATES --------------------
city_coords = {
    "mumbai": {"lat": 19.0760, "lon": 72.8777},
    "indore": {"lat": 22.7196, "lon": 75.8577},
    "bangalore": {"lat": 12.9716, "lon": 77.5946},
    "bengaluru": {"lat": 12.9716, "lon": 77.5946},
    "surat": {"lat": 21.1702, "lon": 72.8311},
    "pune": {"lat": 18.5204, "lon": 73.8567},
    "ahmedabad": {"lat": 23.0225, "lon": 72.5714},
    "patna": {"lat": 25.5941, "lon": 85.1376},
    "ludhiana": {"lat": 30.9010, "lon": 75.8573},
    "delhi": {"lat": 28.7041, "lon": 77.1025},
    "new delhi": {"lat": 28.6139, "lon": 77.2090},
    "chennai": {"lat": 13.0827, "lon": 80.2707},
    "kolkata": {"lat": 22.5726, "lon": 88.3639},
    "hyderabad": {"lat": 17.3850, "lon": 78.4867},
    "jaipur": {"lat": 26.9124, "lon": 75.7873},
    "lucknow": {"lat": 26.8467, "lon": 80.9462},
    "kanpur": {"lat": 26.4499, "lon": 80.3319},
    "nagpur": {"lat": 21.1458, "lon": 79.0882},
    "thane": {"lat": 19.2183, "lon": 72.9781},
    "bhopal": {"lat": 23.2599, "lon": 77.4126},
    "visakhapatnam": {"lat": 17.6868, "lon": 83.2185},
    "vadodara": {"lat": 22.3072, "lon": 73.1812},
    "ghaziabad": {"lat": 28.6692, "lon": 77.4538},
    "coimbatore": {"lat": 11.0168, "lon": 76.9558},
}

# -------------------- FUZZY SEARCH --------------------
def fuzzy_search(query, top_k=15, min_score=60):
    results = []
    for idx, row in df.iterrows():
        eng = str(row.get(col_name, ""))
        hin = str(row.get(col_hindi, ""))
        score_eng = fuzz.token_sort_ratio(query, eng)
        score_hin = fuzz.token_sort_ratio(query, hin)
        score = max(score_eng, score_hin)

        if score >= min_score:
            results.append({
                "id": idx,
                "English": eng,
                "Hindi": hin,
                "Gender": row.get(col_gender, ""),
                "Age": row.get(col_age, ""),
                "City": row.get(col_city, ""),
                "Crime Domain": row.get(col_crime, ""),
                "Crime Code": row.get(col_code, ""),
                "Case Closed": "Yes" if str(row.get(col_closed, "")).lower() in ["yes","y","true","1"] else "No",
                "Similarity": score
            })
    return sorted(results, key=lambda x: x["Similarity"], reverse=True)[:top_k]

# -------------------- API ROUTE FOR ROW SELECTION --------------------
@app.route("/api/row-data", methods=["POST"])
def get_row_data():
    data = request.json
    selected_ids = data.get("ids", [])
    
    if not selected_ids:
        return jsonify({"error": "No IDs provided"}), 400
    
    # Filter results by selected IDs
    selected_results = [r for r in data.get("all_results", []) if r.get("id") in selected_ids]
    
    # Calculate statistics for selected rows
    charts = {"gender": {}, "age": {}, "crime": {}}
    map_points = []
    
    if selected_results:
        # Gender
        gender_counts = Counter([r["Gender"] for r in selected_results if r["Gender"]])
        charts["gender"] = dict(gender_counts)
        
        # Age
        ages = [int(r["Age"]) for r in selected_results if r["Age"] and str(r["Age"]).isdigit()]
        age_counts = Counter(ages)
        charts["age"] = dict(age_counts)
        
        # Crime
        crime_counts = Counter([r["Crime Domain"] for r in selected_results if r["Crime Domain"]])
        charts["crime"] = dict(crime_counts)
        
        # Map points
        city_crime = {}
        for r in selected_results:
            city = str(r["City"]).lower().strip()
            crime = r["Crime Domain"]
            key = (city, crime)
            city_crime[key] = city_crime.get(key, 0) + 1
        
        for (city, crime), count in city_crime.items():
            coords = city_coords.get(city, {"lat": 22.97, "lon": 78.65})
            map_points.append({
                "city": city.capitalize(),
                "lat": coords["lat"],
                "lon": coords["lon"],
                "count": count,
                "crime": crime
            })
    
    return jsonify({
        "charts": charts,
        "map_points": map_points
    })

# -------------------- MAIN ROUTE --------------------
@app.route("/", methods=["GET", "POST"])
def index():
    query = ""
    results = []
    charts = {"gender": {}, "age": {}, "crime": {}}
    map_points = []

    if request.method == "POST":
        query = request.form.get("query", "").strip()
        top_k = int(request.form.get("top_k", 15))
        threshold = int(request.form.get("threshold", 60))
        results = fuzzy_search(query, top_k, threshold)

        if results:
            # Gender
            gender_counts = Counter([r["Gender"] for r in results if r["Gender"]])
            charts["gender"] = dict(gender_counts)
            
            # Age
            ages = [int(r["Age"]) for r in results if r["Age"] and str(r["Age"]).isdigit()]
            age_counts = Counter(ages)
            charts["age"] = dict(age_counts)
            
            # Crime
            crime_counts = Counter([r["Crime Domain"] for r in results if r["Crime Domain"]])
            charts["crime"] = dict(crime_counts)
            
            # Map points
            city_crime = {}
            for r in results:
                city = str(r["City"]).lower().strip()
                crime = r["Crime Domain"]
                key = (city, crime)
                city_crime[key] = city_crime.get(key, 0) + 1
            
            for (city, crime), count in city_crime.items():
                coords = city_coords.get(city, {"lat": 22.97, "lon": 78.65})
                map_points.append({
                    "city": city.capitalize(),
                    "lat": coords["lat"],
                    "lon": coords["lon"],
                    "count": count,
                    "crime": crime
                })

    return render_template(
        "index.html",
        query=query,
        results=results,
        charts=json.dumps(charts),
        map_points=json.dumps(map_points)
    )

# -------------------- AUTO OPEN APP --------------------
if __name__ == "__main__":
    Timer(1, lambda: webbrowser.open("http://127.0.0.1:5000/")).start()
    app.run(debug=True, use_reloader=False)