import json

class Who_is_fake(object):
    def __init__(self):
        self.NEW_HOST_1 = {}
        for k, v in json.load(open("data/sources.json")).items():
            hostname = k.lower()
            _type = v["type"]
            if _type in ["fake", "conspiracy", "hate"]:
                self.NEW_HOST_1[hostname] = "FAKE"
            elif _type == "bias":
                self.NEW_HOST_1[hostname] = "BIAS"

        self.NEW_HOST_2 = {k.lower(): v for k, v in json.load(open("data/mbfc_host_label.json")).items()}


    def identify(self, ht):
        ht = ht.lower()
        labels = []
        if ht in self.NEW_HOST_1:
            labels.append(self.NEW_HOST_1[ht])
        else:
            labels.append("GOOD")
        if ht in self.NEW_HOST_2:
            labels.extend(self.NEW_HOST_2[ht])
        else:
            labels.extend([-1, -1])
        return labels

    def is_fake(self, ht):
        if self.identify(ht)[0] == "FAKE":
            return True
        else:
            return False

class Are_you_IRA(object):
    def __init__(self):
        self.IRA_user_set = set(json.load(open("data/IRA-users-set.json")))

    def fuck(self, ht):
        return ht in self.IRA_user_set


if __name__ == "__main__":
    who = Who_is_fake()
    print(who.identify("baidu.com"))




