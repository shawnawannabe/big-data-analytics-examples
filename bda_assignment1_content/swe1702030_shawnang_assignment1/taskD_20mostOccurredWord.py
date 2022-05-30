from mrjob.job import MRJob
from mrjob.step import MRStep
from stop_words import get_stop_words
import re

WORD_RE = re.compile(r'\w+')

class mostOccurredWord(MRJob):
    
    def mapper_get_words(self, _, line):
        for word in WORD_RE.findall(line):
            word = word.lower()
            if word not in get_stop_words('english'):
                yield word, 1
        
    def reducer_count_words(self, word, count):
        yield None,(sum(count), word)
        
    def reducer_find_top20(self, _,word_count_pair):
        ls = list(word_count_pair)
        sorted_ls = sorted(ls)
        length = len(sorted_ls)
        for top20 in range(length-1,length-20,-1):
            yield sorted_ls[top20]
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_top20)
        ]

if __name__ == "__main__":
    mostOccurredWord.run()