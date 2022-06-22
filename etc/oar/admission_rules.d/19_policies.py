import math

# cores per cpu = 4
# cpu per nodes = 1
# nodes = 2

acc = []

hy = {
        'network_address':0, 
        'cpu':1, 
        'firsthalf':2, 
        'secondhalf':2, 
        'core':3
}

default = {
        0:8, 
        1:2, 
        2:1, 
        3:4
}

max_hy = 3
min_hy = 0

for rq in resource_request:
    
    hier = {
            0:1, 
            1:1, 
            2:1, 
            3:1
    }

    (rq_list, wt) = rq
    res, prop = rq_list[0]['resources'], rq_list[0]['property']
    logger.info(res)
    
    seen = set()
    for r in res:
        seen.add(hy[r['resource']])
        hier[hy[r['resource']]] = int(r['value'])

    if 2 in seen:
        default[3] //= 2

    bottom = max(seen)
    for lv in range(max_hy, min_hy, -1):
        if lv == bottom:
            break
        if lv not in seen:
            hier[lv] = default[lv]
    
    logger.info(str(hier))
    cores = 1
    for key in hier:
        cores *= hier[key]

    res.clear()
    if 'exclusive' in types:
        nodes = math.ceil(cores / (2 * 4))
        res.append({'resource':'network_address', 'value':nodes})
    elif 'spread' in types:
        half_cpus = math.ceil(2 * cores / 4)
        half_cpus += half_cpus % 2
        res.append({'resource':'firsthalf', 'value':half_cpus})
    else:
        nodes = math.ceil(cores / (2 * 4))
        half_cpus = math.ceil(2 * cores / 4)
        res.append({'resource':'secondhalf', 'value':half_cpus})
        new_rq = ([{'property': prop, 'resources': [{'resource': 'firsthalf', 'value': half_cpus}]}], wt)
        acc.append(new_rq)
        new_rq = ([{'property': prop, 'resources': [{'resource': 'network_address', 'value': nodes}]}], wt)
        acc.append(new_rq)
    logger.info(str(res))

for rq in acc:
    resource_request.append(rq)

logger.info(str(resource_request))
