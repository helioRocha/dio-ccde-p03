from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")


class MRDataMining(MRJob):

    def mapper_get_words(self, _, line):
        for word in WORD_RE.findall(line): yield (word.lower(), 1)

    def combiner_count_words(self, word, counts): yield (word, sum(counts))

    def reducer_count_words(self, word, counts): yield sum(counts), word

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words)
        ]

if __name__ == '__main__':
    MRDataMining.run()
