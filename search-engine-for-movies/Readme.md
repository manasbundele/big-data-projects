# Search Engine for Movie Plot Summaries

In this part, we worked with a dataset of movie plot summaries that is available from the
Carnegie Movie Summary Corpus site. We built a search engine for the plot
summaries using Scala. We used the tf-idf technique to accomplish the above task.

## Details:
1. Extract and upload the file plot summaries.txt from http://www.cs.cmu.edu/~ark/personas/
data/MovieSummaries.tar.gz to Databricks. Also, upload a file containing user's search terms
one per line.
2. Remove stopwords.
3. Create a tf-idf for every term and every document (represented by Wikipedia movie ID)
using the MapReduce method.
4. Output results relevant to user query:	
	* If user enters a single term: It will output the top 10 documents with the highest tf-idf
values for that term.
	* If user enters a query consisting of multiple terms: An example could be "Funny
movie with action scenes". In this case, it evaluates the cosine similarity between
the query and all the documents and return top 10 documents having the highest cosine
similarity values.

The attached file movie_search_engine.html contains the databricks file with the scala code.
