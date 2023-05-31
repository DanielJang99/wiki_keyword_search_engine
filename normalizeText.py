import string
from nltk.stem.porter import PorterStemmer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

stemmer = PorterStemmer()

def remove_punctuation(text):
    translator = str.maketrans('', '', string.punctuation)
    return text.translate(translator)

def stem_words(text):
    stop_words = set(stopwords.words("english"))
    word_tokens = word_tokenize(text)
    return [stemmer.stem(word) for word in word_tokens if word not in stop_words]

def stem_word(word):
    return stemmer.stem(word)
