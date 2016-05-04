from copy import deepcopy


def wrap_value(v):
    v = deepcopy(v)
    if isinstance(v, str):
        return {'S': v}
    elif isinstance(v, dict):
        return {'M': _wrap_dict(v)}
    elif isinstance(v, int) or isinstance(v, float):
        return {'N': v}
    else:
        raise TypeError('Type: {} not insertable into dynamodb'.format(
            type(v)
        ))


def _wrap_dict(d):
    for k, v in d.items():
        d[k] = wrap_value(v)
    return d


def unwrap_value(v):
    v = deepcopy(v)
    v = v.popitem()[1]
    if isinstance(v, dict):
        v = _unwrap_dict(v)
    return v


def _unwrap_dict(d):
    for k, v in d.items():
        v = v.popitem()[1]
        if isinstance(v, dict):
            v = _unwrap_dict(v)
        d[k] = v
    return d
