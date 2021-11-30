import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.neighbors import NearestNeighbors
import pickle

#load data
users = pd.read_csv('datasets/BX-Users.csv', sep=";", error_bad_lines=False, encoding='latin-1')
books = pd.read_csv('datasets/BX-Books.csv', sep=";", error_bad_lines=False, encoding='latin-1')
rating = pd.read_csv('datasets/BX-Book-Ratings.csv', sep=";", error_bad_lines=False, encoding='latin-1')
#pre-processing data
books = books[['ISBN', 'Book-Title', 'Book-Author', 'Year-Of-Publication', 'Publisher']]

books.rename(
    columns={'Book-Title': 'title', 'Book-Author': 'author', 'Year-Of-Publication': 'year', 'Publisher': 'publisher'},
    inplace=True)

users.rename(columns={'User-ID': 'user_id', 'Location': 'location', 'Age': 'age'}, inplace=True)

rating.rename(columns={'User-ID': 'user_id', 'Book-Rating': 'rating'}, inplace=True)

#Exploratory Data Analysis

#extract users with ratings of more than 200

x = rating['user_id'].value_counts() > 200
y = x[x].index
rating = rating[rating['user_id'].isin(y)]

#merge ratings with books

rating_with_books = rating.merge(books, on='ISBN')

number_rating = rating_with_books.groupby('title')['rating'].count().reset_index()  ## total rating of a book

number_rating.rename(columns={'rating': 'number of rating'},
                     inplace=True)  # feature engineering : changing the column names

final_ratings = rating_with_books.merge(number_rating, on='title')

#extract books that have received more than 50 ratings

final_ratings = final_ratings[
    final_ratings['number of rating'] >= 50]

final_ratings.drop_duplicates(['user_id', 'title'], inplace=True)

#creating pivot table

book_pivot = final_ratings.pivot_table(columns='user_id', index='title', values='rating')
book_pivot.fillna(0, inplace=True)

#Modeling

book_sparse = csr_matrix(book_pivot)

model = NearestNeighbors(algorithm='brute')  #train the nearest neighbors algorithm.

model.fit(book_sparse)

distances, suggestions = model.kneighbors(book_pivot.iloc[237, :].values.reshape(1, -1))

for i in range(len(suggestions)):
    print(book_pivot.index[suggestions[i]])

def recommend(ID):
    distances, suggestions = model.kneighbors(book_pivot.iloc[ID,:].values.reshape(1,-1))
    suggestions = suggestions[0]
    for i in range(len(suggestions)-1):
        print(book_pivot.index[suggestions[i+1]])

recommend(1)

pickle_out = open("book_recommender.pkl","wb")
pickle.dump(model,pickle_out)
pickle_out.close()
