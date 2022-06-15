import math

nodes = 2
cpu_per_node = 1
cores_per_cpu = 4

if 'allocation_type=spread' in types:
    for rq in resource_request:
        (rq_list, _) = rq
        res = rq_list[0]['resources']
        logger.info(res)
        if len(res) == 1 :
            one = res[0]
            if 'network_address' in one.values():
                res.append({'resource':'cpu', 'value':cpu_per_node})
                res.append({'resource':'core', 'value':cores_per_cpu//2})
            if 'cpu' in one.values():
                res.append({'resource':'core', 'value':cores_per_cpu//2})
            if 'core' in one.values():
                c = int(one['value'])
                cpu = math.ceil(c/(cores_per_cpu//2))
                res[0] = {'resource':'cpu', 'value':cpu}
                res.append({'resource':'core', 'value':cores_per_cpu//2})
        else:
            values = []
            for num,item in enumerate(res):
                values += item.values()
                if 'core' in item.values():
                    cores = int(item['value'])
            if len(res) == 2 :
                if ('network_address' in values) and ('cpu' in values):
                    res.append({'resource':'core', 'value':cores_per_cpu//2})
                else:
                    if cores > cores_per_cpu//2:
                        raise Exception("[ADMISSION RULE] Error: Spread allocation. Can only use half the cpu" + " " + "(" + str(cores_per_cpu//2) + " " + "cores per cpu" + ")")
            else:
                if cores > cores_per_cpu//2:
                    raise Exception("[ADMISSION RULE] Error: Spread allocation. Can only use half the cpu" + " " + "(" + str(cores_per_cpu//2) + " " + "cores per cpu" + ")")
