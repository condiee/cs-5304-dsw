To run my code on the wikidump, run ```spark-submit path/to/bigram.py path/to/wiki.txt path/to/output_directory``` or ```spark-submit path/to/wordcount.py path/to/wiki.txt path/to/output_directory```. Make  sure there is no output folder with the same name created in the output directory before running.

The output file to step 4 of the assignment (Word Count) is included in the zip file as "wordcount_output".

The output file to step 5 of the assignment (Bigrams) is included in the zip file as "bigram_output". Both the count of all bigram words and the conditional bigram frequency distribution are included in that file in the format ```((('word1', 'word2'), biCount), probability)``` for each bigram. The python code file is called "bigram.py". 