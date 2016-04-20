from copy import deepcopy


def wrap_dict(d):
    d = deepcopy(d)
    for k, v in d.items():
        if isinstance(v, str):
            d[k] = {'S': v}
        elif isinstance(v, dict):
            d[k] = {'M': wrap_dict(v)}
        elif isinstance(v, int) or isinstance(v, float):
            d[k] = {'N': v}
        else:
            raise TypeError("Type: {} not insertable into dynamodb".format(
                type(v)
            ))
    return d


def unwrap_dict(d):
    d = deepcopy(d)
    for k, v in d.items():
        v = v.popitem()[1]
        if isinstance(v, dict):
            v = unwrap_dict(v)
        d[k] = v
    return d
