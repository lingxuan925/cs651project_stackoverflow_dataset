# cs651 Project - Stack Overflow dataset
<h2>Abstract</h2>
<p>This project investigates the Stack Overflow dataset obtained from the public dataset repository available on Google Cloud. It shows if there is a relationship between 10 specific features provided in the dataset and whether the question posted was answered or not. These 10 features with respect to a question post include, title, body, creation date, answer count, favourite count, score, tags, view count, reputation of the user that posted the question, and vote type.
</p>

<h2>Three Main Steps</h2>
<p>
<h3>Data cleaning and staging</h3>
<p>We used dataframes and spark SQL function to perform most of our data processing</p>
<h3>Data Normalization and Standardization</h3>
<p>We used MinMaxScaler and CountVectorizer methods to normalize the data. And perform naive NLP approach to extract key words from features 'body' and 'title' using RegexTokenizer and StopWordsRemover</p>
<h3>Data Mining</h3>
<p>Built a simple pipeline using the spark MLlib library. The loss function we chose is logistic regression with parameter lambda 0.01 and l2 regularization</p>
</p>
<h2>Result</h2>

<p>The model performed very well in predicting the labels with an accuracy of approximately 97%. the mean absolute error is around 0.1195, which is very good. Interestingly, the true positive rate for label 0 (unanswered questions) was approximately close to 100% while for label 1 (answered questions), it was around 88%. This 10% gap may be due to the fact that label 0 had 4 times as many rows of data as label 1. This imbalance in data may have over trained the label 0 class causing the model to think that the best thing to do is to always predict label 0, thus the extremely high accuracy. It is very likely that the model is predicting label 0 class regardless of the data that it should be predicting for.</p>



