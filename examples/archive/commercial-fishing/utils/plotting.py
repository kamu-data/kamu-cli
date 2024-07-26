import json

def to_plain(v):
    if type(v) in [int, float, str]:
        return v
    else:
        return str(v)

# For every row we first combine GeoJson geometry with other columns into a Feature object
# Then we combine all Features into a FeatureCollection
def df_to_geojson(df, geom='geometry', props=None):
    if props is None:
        props = [
            c for c in df.columns
            if c != geom
        ]
    
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": json.loads(row[geom]),
                "properties": {p: to_plain(row[p]) for p in props}
            }
            for _, row in df.iterrows()
        ]
    }