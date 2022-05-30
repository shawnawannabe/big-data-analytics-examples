from mrjob.job import MRJob
import re

WORD_RE = re.compile(r'\w+')

class MRWordOccuranceCount(MRJob):
    
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(),1
            
    def reducer(self, word, count):
        yield word, sum(count)





if __name__ == "__main__":
    MRWordOccuranceCount.run()