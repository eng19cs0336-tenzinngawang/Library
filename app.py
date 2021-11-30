from flask import Flask, render_template, request, session, redirect, Markup, jsonify
import pandas as pd
import pickle
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import requests
from xml.etree import ElementTree

#app initializatio
app = Flask(__name__)

app.secret_key = 'key'
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"

engine = create_engine('postgres://gmtanwnptdxhbk:4e51670c23af5fb63220fe4b138c0a0dde83390935f29e6ec8e914efbdf8b1db'
                       '@ec2-34-193-46-89.compute-1.amazonaws.com:5432/dcldsd80gdqt3n')
db = scoped_session(sessionmaker(bind=engine))


@app.route('/', methods=['GET', 'POST'])
def index():

    if session.get('username') is None:
        return redirect('/login')

    if request.method == 'GET':

        return render_template('index.html', navbar=True)

    else:

        query = request.form.get('query').lower()
        query_like = '%' + query + '%'

        books = db.execute('SELECT * FROM books WHERE (LOWER(isbn) LIKE :query) OR (LOWER(title) LIKE :query) '
                           'OR (LOWER(author) LIKE :query)',
                           {'query': query_like}).fetchall()

        if not books:
            return render_template('error.html', message='No Books were Found!', navbar=True)

        return render_template('result.html', query=query, books=books, navbar=True)


@app.route('/login', methods=['GET', 'POST'])
def login():

    session.clear()

    if request.method == 'POST':

        username = request.form.get('username')
        password = request.form.get('password')

        user = db.execute('SELECT * FROM users WHERE (username=:username AND password=:password)',{'username': username, 'password': password}).fetchone()

        if user is None:
            return render_template('error.html', message='Entered credentials not valid!')

        session["username"] = username

        return redirect('/')

    else:
        return render_template('login.html', navbar=False)


@app.route('/logout')
def logout():

    session.clear()

    return redirect('/')


@app.route('/signup', methods=['GET', 'POST'])
def signup():

    session.clear()

    if request.method == 'POST':

        username = request.form.get('username')
        password = request.form.get('password')
        retype_password = request.form.get('retype_password')

        # check if passwords are the same

        if not password == retype_password:
            return render_template('error.html', message='Passwords do not match')

        # check if user is available

        avail = db.execute('SELECT username FROM users WHERE username=:username',
                           {'username': username}).fetchone()

        if avail:
            return render_template('error.html', message='Username Already Exists')

        # Write username and password to database

        db.execute('INSERT INTO users(username, password) VALUES(:username, :password)',
                   {'username': username, 'password': password})
        db.commit()

        session['username'] = username

        return redirect('/')

    else:
        return render_template('signup.html', navbar=False)


@app.route('/books/<isbn>')
def book(isbn):

    book = db.execute('SELECT * FROM books WHERE isbn=:isbn',
                      {'isbn': isbn}).fetchone()

    if book is None:
        return render_template('error.html', message='This book is not available', navbar=True)

    url = "https://www.googleapis.com/books/v1/volumes?q={}".format(isbn)
    res = requests.get(url)
    tree = ElementTree.fromstring(res.content)

    try:
        description = tree[1][16].text
        imageLinks = tree[1][8].text
        previewLink = tree[1][24].text

    except IndexError:
        return render_template('book.html', book=book, link=None, navbar=True)

    description_markup = Markup(description)

    return render_template('book.html', book=book, link=previewLink, description=description_markup,
                           image_url=imageLinks, navbar=True)


@app.route('/api/<isbn>')
def book_api(isbn):

    book = db.execute('SELECT * FROM books WHERE isbn=:isbn',
                      {'isbn': isbn}).fetchone()

    if book is None:
        api = jsonify({'error': 'This book is not available'})
        return api

    url = "https://www.googleapis.com/books/v1/volumes?q={}".format(isbn)
    res = requests.get(url)
    tree = ElementTree.fromstring(res.content)

    try:
        description = tree[1][16].text
        imageLinks = tree[1][8].text
        previewLink = tree[1][24].text

    except IndexError:
        api = jsonify({
            'title': book.title,
            'author': book.author,
            'year': book.year,
            'isbn': book.isbn,
            'link': '',
            'description': '',
            'book_cover': ''
        })

        return api

    api = jsonify({
        'title': book.title,
        'author': book.author,
        'year': book.year,
        'isbn': book.isbn,
        'link': previewLink,
        'description': description,
        'book_cover': imageLinks,
    })

    return api




@app.route('/recommend', methods=['GET', 'POST'])
def recommend():

    recommend = 0
    loaded_model = pickle.load(open("book_recommender.pkl", "rb"))
    images = pd.read_csv('datasets/images.csv')
    book_pivot = pd.read_csv('datasets/book_pivot.csv')
    book_pivot.set_index('title', inplace=True)
    book_names = list(book_pivot.index)

    if request.method == 'POST':
        Id = int(request.form['book'])
        distances, suggestions = loaded_model.kneighbors(
            book_pivot.iloc[Id, :].values.reshape(1, -1))
        suggestions = suggestions[0]
        authors = []
        years = []
        publishers = []
        titles = []
        isbn_no = []
        recommend = 1
        choice = []

        name = book_names[Id]
        choice.append(name)
        choice.append(images[images['title'] == name]['author'].values[0])
        choice.append(images[images['title'] == name]['year'].values[0])
        choice.append(images[images['title'] == name]['publisher'].values[0])
        choice.append(images[images['title'] == name]['ISBN'].values[0])

        for i in range(len(suggestions) - 1):
            name = book_pivot.index[suggestions[i + 1]]
            author = images[images['title'] == name]['author'].values[0]
            yr = images[images['title'] == name]['year'].values[0]
            publish = images[images['title'] == name]['publisher'].values[0]
            isbn = images[images['title'] == name]['ISBN'].values[0]

            authors.append(author)
            years.append(yr)
            publishers.append(publish)
            titles.append(name)
            isbn_no.append(isbn)
        return render_template('recommend.html', book_names=book_names, choice=choice, titles=titles, author=authors,
                               year=years, publisher=publishers, recommend=recommend, isbn_no=isbn_no)
    return render_template('recommend.html', book_names=book_names)

if __name__ == '__main__':
    app.run(debug=True)
