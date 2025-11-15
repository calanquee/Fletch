
def parse_filename(filename):
    # print(filename)
    segs = filename.split("/")
    res = []
    for idx in range(2, len(segs)):
        seg = "/".join(segs[: idx + 1])
        # print(seg)
        res.append(seg)
    return res


def get_single_recirculation(filename):
    # print(filename)
    segs = filename.split("/")
    res = []
    for idx in range(2, len(segs)):
        seg = "/".join(segs[: idx + 1])
        # print(seg)
        res.append(seg)
    return len(res)
