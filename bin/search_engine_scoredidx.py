import re
import collections
import math
import os
import redis
from nltk.stem import PorterStemmer
from metaphone import doublemetaphone
from redis.exceptions import ConnectionError

NON_WORDS = re.compile("[^a-z0-9' ]")

# stop words pulled from the below url
# http://www.textfixer.com/resources/common-english-words.txt
STOP_WORDS = set('''a able about across after all almost also am
among an and any are as at be because been but by can cannot
could dear did do does either else ever every for from get got
had has have he her hers him his how however i if in into is it
its just least let like likely may me might most must my neither
no nor not of off often on only or other our own rather said say
says she should since so some than that the their them then
there these they this tis to too twas us wants was we were what
when where which while who whom why will with would yet you
your'''.split())

class ScoredIndexSearch(object):
    def __init__(self, prefix, *redis_settings):
        self.prefix = prefix.lower().strip(':') + ':'
        self.connection = redis.Redis(*redis_settings)
        self.stemmer = PorterStemmer()
    
    @staticmethod
    def get_index_keys(content, add=True):
        words = NON_WORDS.sub(' ', content.lower()).split()
        words = [word.strip("'") for word in words]
        words = [word for word in words if word not in STOP_WORDS and len(word) > 1]

        # Apply the Porter Stemmer
        stemmed_words = [PorterStemmer().stem(word) for word in words]

        # Apply the Double Metaphone algorithm
        phonetic_words = []
        for word in stemmed_words:
            primary, secondary = doublemetaphone(word)
            phonetic_words.extend([primary, secondary] if secondary else [primary])

        # Remove any None values from phonetic_words
        phonetic_words = [word for word in phonetic_words if word]

        if not add:
            return phonetic_words

        counts = collections.defaultdict(float)
        for word in phonetic_words:
            counts[word] += 1
        wordcount = len(phonetic_words)
        tf = dict((word, count / wordcount) for word, count in counts.items())
        return tf

    def handle_content(self, id, content, add=True):
        keys = self.get_index_keys(content)
        prefix = self.prefix
        pipe = self.connection.pipeline(False)
        if add:
            pipe.sadd(prefix + 'indexed:', id)
            for key, value in keys.items():
                pipe.zadd(prefix + key, {id: value})
        else:
            pipe.srem(prefix + 'indexed:', id)
            for key in keys:
                pipe.zrem(prefix + key, id)
        pipe.execute()
    
        return len(keys)

    def add_indexed_item(self, id, content):
        return self.handle_content(id, content, True)
    
    def remove_indexed_item(self, id, content):
        return self.handle_content(id, content, False)
    
    def search(self, query_string, offset=0, count=10):
        keys = [self.prefix + key for key in self.get_index_keys(query_string, False)]
    
        if not keys:
            return [], 0
        
        def idf(count):
            if not count:
                return 0
            return max(math.log(total_docs / count, 2), 0)
    
        total_docs = max(self.connection.scard(self.prefix + 'indexed:'), 1)

        pipe = self.connection.pipeline(False)
        for key in keys:
            pipe.zcard(key)
        sizes = pipe.execute()
    
        idfs = list(map(idf, sizes))
        weights = dict((key, idfv) for key, size, idfv in zip(keys, sizes, idfs) if size)
        
        if not weights:
            return [], 0

        temp_key = self.prefix + 'temp:' + os.urandom(8).hex()
        try:
            known = self.connection.zunionstore(temp_key, weights)
            ids = self.connection.zrevrange(temp_key, offset, offset + count - 1, withscores=True)
        finally:
            self.connection.delete(temp_key)
        return ids, known
    
def main():
    try:
        t = ScoredIndexSearch('search', 'localhost')
        t.connection.ping()  # Check if the Redis server is available

        # Get the list of keys and delete them properly
        keys = t.connection.keys('search*')
        if keys:
            t.connection.delete(*keys)
        
        t.add_indexed_item(1, 'This is a test')
        t.add_indexed_item(2, 'I am testing this search engine that I wrote')

        print(t.search('test'))
        print(t.search('search'))

        t.remove_indexed_item(1, 'This is a test')
        print(t.search('test'))
        print(t.search('search'))
    except ConnectionError:
        print("Failed to connect to Redis. Please ensure that the Redis server is running.")

if __name__ == '__main__':
    main()
