import json

NEW_HOST_1 = {}
for k, v in json.load(open("data/sources.json")).items():
    hostname = k.lower()
    _type = v["type"]
    if _type in ["fake", "conspiracy", "hate"]:
        NEW_HOST_1[hostname] = "FAKE"
    elif _type == "bias":
        NEW_HOST_1[hostname] = "BIAS"

NEW_HOST_2 = {k.lower(): v for k, v in json.load(open("data/mbfc_host_label.json")).items()}


def kind_of_news(ht):
    ht = ht.lower()
    labels = []
    if ht in NEW_HOST_1:
        labels.append(NEW_HOST_1[ht])
    else:
        labels.append("NOT FAKE")

    if ht in NEW_HOST_2:
        labels.extend(NEW_HOST_2[ht])
    else:
        labels.extend([-1, -1])

    return labels


if __name__ == "__main__":
    print(len(NEW_HOST_1))
    print(len(NEW_HOST_2))
    print(kind_of_news("cnn.com"))