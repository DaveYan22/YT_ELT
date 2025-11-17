from datetime import timedelta
from datetime import datetime

def parse_duration(duration_str):
    duration_str = duration_str.replace("P", "").replace("T", "")

    components = ['D', 'H', 'M', 'S']
    values = {'D': 0, 'H': 0, 'M': 0, 'S': 0}

    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)  
            values[component] = int(value)

    total_duration = timedelta(
        days=values['D'],
        hours=values['H'],
        minutes=values['M'],
        seconds=values['S']
    )

    return total_duration

def transform_data(row):
    # FIXED: Handle both staging (lowercase) and core (uppercase) column names
    # Check if we're working with staging data or core data
    if "duration" in row:
        # Staging schema - lowercase columns
        duration_str = row["duration"]
        duration_id = parse_duration(duration_str)
        
        # Transform for core schema - uppercase columns
        transformed_row = {
            "Video_ID": row["video_id"],
            "Video_Title": row["title"],
            "Upload_Date": row["publishedat"],  # FIXED: lowercase
            "Duration": (datetime.min + duration_id).time(),
            "Video_Type": "Short" if duration_id.total_seconds() <= 60 else "Normal",
            "Video_Views": row["viewcount"],  # FIXED: lowercase
            "Likes_Count": row["likecount"],  # FIXED: lowercase
            "Comments_Count": row["commentcount"]  # FIXED: lowercase
        }
        return transformed_row
    else:
        # Core schema - already has uppercase columns, just update Duration and Video_Type
        duration_id = parse_duration(row["Duration"])
        row["Duration"] = (datetime.min + duration_id).time()
        row["Video_Type"] = "Short" if duration_id.total_seconds() <= 60 else "Normal"
        return row