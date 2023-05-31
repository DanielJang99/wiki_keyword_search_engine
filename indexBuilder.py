from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
import math
from normalizeText import stem_words, remove_punctuation

class MRIndexBuilder(MRJob):

    def mapper_get_keyword_document_pairs(self, _, line):
        """Output (keyword, document) pair as key"""
        filename = jobconf_from_env('map.input.file')[7:]
        for word in stem_words(remove_punctuation(line)):
            yield ((word.lower(),filename), 1)
    
    def combiner_count_keyword_document_pairs(self, word_doc_pair, cnt):
        """Count the occurence of each (keyword, document) pair"""
        yield word_doc_pair, sum(cnt)
    
    def reducer_count_keyword_document_pairs(self, word_doc_pair, cnt):
        """Send all (document, frequency) pairs for each word to the same reducer"""
        word = word_doc_pair[0]
        doc = word_doc_pair[1]
        yield (word, (doc, sum(cnt)))
    
    def reducer_calculate_tfidf(self, word, doc_freq_pairs):
        """Send all (document, tfidf_value) pairs for each word to the same reducer"""
        doc_freq_list = list(doc_freq_pairs)
        word_containing_docs = len(doc_freq_list)

        # Get the total number of documents from command line, or use 10000 as default 
        corpus_size = int(jobconf_from_env('job.corpus_size')) if jobconf_from_env('job.corpus_size') is not None else 10000

        for doc_freq in doc_freq_list:
            doc_name, word_freq = doc_freq[0], doc_freq[1]
            # calculate tfidf = frequency * log(total_documents/documents_with_keyword)
            tf_idf = word_freq * math.log(corpus_size/word_containing_docs)
            yield word, (doc_name, tf_idf)
    
    def reducer_sort_by_tfidf(self, word, doc_tfidf):
        """For each word, sort its relevant documents by their TFIDF values"""
        l = list(map(lambda tup: tup[0], sorted(list(doc_tfidf), key=lambda x:x[1], reverse=True)))
        yield word, "|".join(l)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_keyword_document_pairs, 
                   combiner= self.combiner_count_keyword_document_pairs, 
                   reducer=self.reducer_count_keyword_document_pairs), 
            MRStep(reducer=self.reducer_calculate_tfidf),
            MRStep(reducer=self.reducer_sort_by_tfidf),
        ]

if __name__ == '__main__':
    MRIndexBuilder.run()