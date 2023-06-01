import sys
import os
from indexBuilder import MRIndexBuilder
from normalizeText import stem_word

class invertedIndex:
    def __init__(self, mr_input_path, mr_output_file, k):
        self.mr_input_path = mr_input_path      # Directory with txt files
        self.mr_output_file = mr_output_file    # MapReduce output file containing data to load to inverted index 
        self.doc_index = {}     # inverted index 
        self.k = k  # number of documents to display for every search
    
    def find_corpus_size(self) -> int:
        """Return the number of txt files inside MR input path directory"""
        try:
            _, _, files = next(os.walk(self.mr_input_path))
            return len(list(filter(lambda x:x[-4:] == ".txt",files)))
        except Exception as e:
            return 0
    
    def check_MR_output(self) -> bool:
        """Check whether mr_output_file exists. 
        If not, the program will automatically run MapReduce to generate the output file.
        """
        try: 
            with open(self.mr_output_file, "r") as data_file:
                return len(data_file.readline()) > len(self.mr_input_path)
        except Exception as e:
            return False
        
    def load_data_from_MR_output(self) -> bool:
        """Parse mr_output_file and load data into inverted index.
        Return True if data loading was successful, else False.
        """
        try: 
            data_file = open(self.mr_output_file, "r")
            lines = data_file.readlines()
            for line in lines:
                word_index = line.split("\t")
                word = word_index[0].strip('""')
                documents = word_index[1].rstrip().strip('""').split("|")
                self.doc_index[word] = documents
            data_file.close()
            return True
        except Exception as e:
            print("Error in loading data from MR output file: ", e)
            return False    

    def load_data_from_MR_execution(self) -> bool:
        """Execute MapReduce with txt files in mr_input_path. 
        Save the output to mr_output_file and load data into inverted index.
        Return True if data loading was successful, else False. 
        """
        try:
            corpus_size = self.find_corpus_size()
            if corpus_size == 0:
                raise Exception("Missing/Wrong input path for MapReduce.")
            print("Executing MapReduce. This may take a while.")
            MR_index_builder = MRIndexBuilder(args=[self.mr_input_path+'/*.txt', '--jobconf', 'job.corpus_size='+str(corpus_size)])
            with MR_index_builder.make_runner() as runner:
                runner.run()
                with open(self.mr_output_file, "a") as output_file:
                    for word, documents in MR_index_builder.parse_output(runner.cat_output()):
                        output_file.write(f"{word}\t{documents}\n")
                        self.doc_index[word] = documents.split("|")
            return True
        except Exception as e:
            print("Error in MR execution: ", e)
            return False 

    def __find_keyword_and_print(self, document: str, keyword: str):
        """Open a document and print snippet containing the keyword"""
        try: 
            doc_file = open(document, "r")
            # parse title 
            title = doc_file.readline().strip("Title:").strip()
            # parse document content and print snippet 
            body = doc_file.read()
            body_split = body.split(" ")
            for i, word in enumerate(body_split):
                if keyword == stem_word(word):
                    snippet_start_idx = max(0, i-3)
                    snippet_end_idx = min(len(body_split)-1, i+4)
                    snippet = " ".join(body_split[snippet_start_idx:snippet_end_idx]).replace("\n", "")
                    print(f"({document.split('/')[-1]}) [{title}] \"... {snippet} ...\"")
                    break
            doc_file.close()
        except Exception as e:
            print("Error in finding keyword in relevant document: ", e)


    def keyword_search(self):
        """Take user input as keyword and search for relevant documents in the inverted index(doc_index)"""
        while True:
            inp = input("Enter Search Term, or '/q' to exit: ")
            if inp == "/q":
                break
            if ' ' in inp:
                print("Only single keyword is permitted")
                continue
            try:
                stemmed_input = stem_word(inp)
                documents = self.doc_index[stemmed_input]
                doc_iter_range = min(self.k, len(documents))
                for i in range(doc_iter_range):
                    self.__find_keyword_and_print(documents[i], stemmed_input)
            except KeyError as e:
                print("Keyword Not Found")

    def run(self):
        """Load the inverted index with data and launch interface for keyword search"""
        data_loaded = self.load_data_from_MR_output() if self.check_MR_output() else self.load_data_from_MR_execution()
        if not data_loaded:
            print("Failed to load data. Terminating program.")
            return
        self.keyword_search()
                
def main():
    # default parameters 
    mr_input_path, mr_output_file, k = "./documents10k", "./output.txt", 10

    # override default parameters if specified in command line 
    for arg in sys.argv:
        if "mr_input_path" in arg: 
            mr_input_path = arg.split("=")[1]
        elif "mr_output_file" in arg:
            mr_output_file = arg.split("=")[1]
        elif "k" in arg:
            tmp = int(arg.split("=")[1])
            k = tmp if tmp > 0 else 10
    
    # Launch program
    invertedIndex(mr_input_path, mr_output_file, k).run()

if __name__ == '__main__':
    main()