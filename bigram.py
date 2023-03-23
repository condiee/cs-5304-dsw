# outputs the bigram model (conditional bigram distribution) for the document
# output should list all the bigrams and their corresponding conditional frequency distributions

# find the probability of a word based only on its previous word
# consider all words as lower case
# ignore all non-alphabetic characters

import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf()
sc = SparkContext(conf=conf)

# sys.argv is automatically a list of strings representing the arguments on the command-line
# ['/Users/emmacondie/Desktop/DSW/bigram.py', './wiki.txt', './output']

# flatMap: Similar to map (transformation), but each input item can be mapped to
# 0 or more output items (so func should return a Seq rather than a single item).
sentences = sc.textFile(sys.argv[1]).flatMap(lambda line: line.strip().split("."))
wordList = sentences.flatMap(lambda line: line.strip().split(" "))
wordCounts = wordList.map(lambda word: (word,1)).reduceByKey(lambda a,b:a+b) # k,v pair so need ()
# ('word1', count)

words = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split(".")) \
            .map(lambda line: line.strip().split(" "))
bigrams = words.flatMap(lambda word: ((word[i],word[i+1]) for i in range(0,len(word)-1)))
# ('word1', 'word2')
biCounts = bigrams.map(lambda bigram: (bigram, 1)).reduceByKey(lambda a,b:a+b)
# (('word1', 'word2'), biCount)

f = biCounts.map(lambda set: (set[0][0], set))
# ('word1', (('word1', 'word2'), biCount)) # k,v
count = f.join(wordCounts)
# ('word1', ((('word1', 'word2'), biCount), count))

# probability = count(bigram)/count(word1)
output = count.map(lambda set: ((set[1][0][0], set[1][0][1]), (set[1][0][1]/set[1][1])))
# ((('word1', 'word2'), biCount), probability)

output.coalesce(1, shuffle=True).saveAsTextFile(sys.argv[2])
# coalesce: Decrease the number of partitions in the RDD to numPartitions.
# Useful for running operations more efficiently after filtering down a large dataset.
sc.stop()

# spark-submit ./bigram.py ./wiki.txt ./out
# spark-submit ./bigram.py ./short.txt