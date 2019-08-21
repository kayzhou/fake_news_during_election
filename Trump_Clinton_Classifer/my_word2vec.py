from gensim.models.word2vec import LineSentence
from gensim.models import Word2Vec, TfidfModel, FastText
from tqdm import tqdm


def to_words(s):
    return [w[0] for w in thu.cut(s)]


def get_train_data(in_name):
    """
    加载所有数据
    """
    # stop_word = load_stopword()
    d = []
    for i, line in enumerate(open(in_name)):
        if i % 10000 == 0:
            print(i)
        d.append(line.strip().split(' '))
    return d


if __name__ == '__main__':

    corpus = LineSentence('data/train.txt')
    print(type(corpus))

    print('最终开始训练 ... ...')
    model = Word2Vec(corpus, size=400, window=3, min_count=1, workers=8)
    # model = FastText(corpus, size=300, window=5, min_count=5, iter=10)
    model.save("model/word2vec.mod")
