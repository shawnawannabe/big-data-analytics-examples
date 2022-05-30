from mrjob.job import MRJob
# from mrjob.step import MRStep
from nltk.util import ngrams
import re

WORD_RE = re.compile(r'\w+')

class _2gram(MRJob):
    
    def mapper(self, _, line):
        ln = WORD_RE.findall(line)
        ls = []
        _2gram = []
        for word in ln:
            word = word.lower()
            ls.append(word)
        for x in range(len(ls)-1):
            _2gram.append((ls[x], ls[x+1]))
            yield _2gram[x],1

    def reducer(self, word, count):
        yield word, sum(count)

if __name__ == "__main__":
    _2gram.run()