from datetime import datetime, timedelta

def parse_duration(duration_str):
    duration_str= duration_str.replace("P", "").replace("T", "")

    components = ("D","H","M","S")
    values = {"D":0, "H":0, "M":0, "S":0}

    for component in components:
        if component in duration_str:
            values,duration_str = duration_str.split(component)
            values[component] = int(values)
    total_duration = timedelta(
        days= values["D"],
        hours= values["H"],
        minutes= values["M"],
        seconds= values["S"]
    )

    return total_duration

def transform_data(row):
    duration_tds= parse_duration(row["Duration"])
    row["Duration"] = (datetime.min + duration_tds).time()
    row["Video_Type"] = "Short" if duration_tds <= timedelta(seconds=60) else "Long"
    return row