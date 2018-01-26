import re
import jieba


def cut(content):
    content = re.sub('[A-Za-z]+', '', content)
    content = re.sub('\d+', '', content)
    content = re.sub('<[^>]*>', '', content)
    content = re.sub('[\W]+', '', content)
    jieba.set_dictionary('dict.txt')
    content = list(jieba.cut(content, cut_all=False))
    with open('stop_words.txt', 'r', encoding='UTF-8') as r:
        stopwords = [line.strip("\n") for line in r.readlines()]
    final_content = (" ".join([word for word in content if word not in stopwords]))
    return final_content




