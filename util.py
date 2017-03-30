import json
from functools import reduce
GRID="melbGrid.json"

def get_block(tweet):
    x=tweet[0]
    y=tweet[1]
    grids = json.load(open(GRID, "r", encoding="utf8"))
    for block in grids["features"]:
        xmax = block['properties']["xmax"]
        xmin = block['properties']["xmin"]
        ymax = block['properties']["ymax"]
        ymin = block['properties']["ymin"]
        if x <= xmax and x >= xmin and y <= ymax and y>=ymin :
            return block['properties']["id"]

def merge(dict1,dict2):
    combined_keys={key for d in [dict1,dict2] for key in d}
    set_new = {key:(dict1.setdefault(key, 0) + dict2.setdefault(key, 0)) for key in combined_keys}
    return set_new




