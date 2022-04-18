# Page Rank Algorithm with Example in java:   
When you go and type some keywords in Google Search Engine a list of Web Pages will be displayed ,but how does the search engine know which page to be shown first to the user ? To solve this problem a  algorithm called PageRank was developed at Stanford university by Larry Page and Sergey Brin in 1996.The PageRank Algorithm uses probabilistic distribution to calculate rank of a Web page and using this rank display the search results to the user. The Pagerank is recalculated every time  the search engine crawls the web.

The original Page Rank algorithm which was described by Larry Page and Sergey Brin is :

PR(A) = (1-d) + d (PR(W1)/C(W1) + ... + PR(Wn)/C(Wn))

Where :
PR(A) – Page Rank of page A
PR(Wi) – Page Rank of pages Wi which link to page A
C(Wi) - number of outbound links on page Wi
d - damping factor which can be set between 0 and 1

To calculate PageRank for the n Webpages ,First we initialise all Webpages with equal page rank of 1/n each.Then Step by Step we calculate Page Rank for each Webpage one after the other.

### Let us take one example :


![](https://github.com/aneela123/Big-Data-Group6Project/blob/main/Priyanka/PageRank.PNG)   

There are 5 Web pages represented by Nodes A, B, C , D, E .The hyperlink from each webpage to the other is represented by the arrow head.
Java Program to Implement Simple PageRank Algorithm

At 0th Step we have all Webpages PageRank values 0.2 that is 1/5 (1/n) . To get PageRank of Webpage A ,consider all the incoming links to A .So we have 1/4th the Page Rank of C is pointed to A. So it will be (1/5)*(1/4) which is (1/20) or 0.05 the Page Rank of A. 

Similarly the Page Rank of B will be  (1/5)*(1/4)+(1/5)*(1/1) which is (5/20) or 0.25 because A's PageRank value is 1/5 or 0.2 from Step 0 . Even though we got 0.05 of A's PageRank in Step 1 we are considering 0.05 when we are Calculating Page Rank of B in Step 2.


[reference](https://codispatch.blogspot.com/2015/12/java-program-implement-google-page-rank-algorithm.html?m=1)
