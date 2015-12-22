# bigdata

This is the bigdata project of our group. We use cosine similarity and pagerank algorithm for our project. The input dataset is comments downloaded from Amazon and Reddit.

The program will first compare the simiarity of words of each comment and make a graph and pagerank the most similar comment shown on the result.



There are three files on this page. They are ClusterSimilarity.py, pagerank.py and similarity.py.

The main file is similarity.py.
The ClusterSimilarity.py and pangerank.py are sub-files for similarity.py.
We have MySQL for database where we put plenty of comments into MySQL.
For MySQL, we have two column(id,comment); id is 1,2,3,4... assign for each comment.
Comment which is text file is each comment downloaded from Amazon and Reddit.





The result has three columns which has(ID, Type, Typerank).
Id is where the comment located on MySQL.
Type is which Type for the comment.
TypeRank is which rank for specific comment type.








