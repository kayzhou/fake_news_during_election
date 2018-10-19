import ujson as json

host_label = json.load(open('data/host_label.json'))
# d = json.load(open('data/sources.json'))
# for k, v in d.items():
#     if v['type'] in ['fake', 'conspiracy', 'hate', 'Conspiracy', 'fake news']:
#         print(k, '-2', sep='\t')
#     if v['type'] == 'bias':
#         print(k, '-1', sep='\t')

# dict_labels = {}
# for line in open('data/hostname_label.txt'):
#     host, label = line.strip().split('\t')
#     dict_labels[host] = label
# print(len(dict_labels))
# json.dump(dict_labels, open('data/host_label.json', 'w'), indent=4)

def how_the_news(host_name):
    '''
    -2  fake
    -1  bias
    0   left
    1   leaning left
    2   center
    3   leaning right
    4   right
    '''
    return host_label[host_name]