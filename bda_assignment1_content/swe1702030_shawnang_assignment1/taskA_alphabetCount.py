from mrjob.job import MRJob
import re

alphabet_RE = re.compile(r'[A-Za-z]+')


class MRAlphabetCount(MRJob):

    def mapper(self, _, line):
        for word in alphabet_RE.findall(line):
            word = word.lower()
            for char in sorted(set(word)):
                yield char, word.count(char)
                
                
    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRAlphabetCount.run()



