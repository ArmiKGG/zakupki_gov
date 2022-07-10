from main import *

data = match_all_orgs(es)

sid = data["_scroll_id"]
scroll_size = data["hits"]["total"]["value"]
all_properties = scroll_size
logger(f"All data {scroll_size}")
orgs_all = data["hits"]["hits"]
orgs_all_size = len(orgs_all)
for ind, org in enumerate(orgs_all):
    org = org["_source"]
    last_value = get_last_value(org)
    count = 0
    for i in range(last_value, 1000):
        status = worker(org["org_id"], i)
        if not status:
            count += 1
        if count > 10:
            break
