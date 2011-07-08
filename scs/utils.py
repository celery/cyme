from importlib import import_module

def maybe_list(l):
    if hasattr(l, "__iter__"):
        return l
    elif l is not None:
        return [l]


def shellquote(v):
    return "\\'".join("'" + p + "'" for p in v.split("'"))


def imerge_settings(a, b):
    orig = importlib.import_module(a.SETTINGS_MODULE)
    for key, value in vars(b).iteritems():
        if not hasattr(orig, key):
            setattr(a, key, value)
