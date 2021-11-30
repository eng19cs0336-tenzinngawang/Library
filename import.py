from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import DataError, InternalError, IntegrityError
import csv

Base = declarative_base()


if __name__ == "__main__":

    # Create the database
    engine = create_engine('postgres://gmtanwnptdxhbk:4e51670c23af5fb63220fe4b138c0a0dde83390935f29e6ec8e914efbdf8b1db'
                           '@ec2-34-193-46-89.compute-1.amazonaws.com:5432/dcldsd80gdqt3n')
    Base.metadata.create_all(engine)

    # Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    f = open('datasets/books.csv')
    reader = csv.reader(f)
    limit = int(input('Limit of books dataset: '))
    count = 0

    done_tuple = s.execute('SELECT isbn FROM books').fetchall()
    done_list = [book.isbn for book in done_tuple]

    for isbn, title, author, year in reader:

        if count == limit:
            break

        if isbn in done_list:
            continue

        try:
            s.execute('INSERT INTO books(isbn, title, author, year) VALUES(:isbn, :title, :author, :year)',
                      {'isbn': isbn, 'title': title, 'author': author, 'year': year})
            count += 1

        except DataError as e:
            print(str(e))

        except InternalError as e:
            print(str(e))
            break

        except IntegrityError as e:
            print(str(e))
            continue

    s.commit()
    print('Successfully Added {} Books Dataset'.format(count))
    s.close()

    userf = open('datasets/BX-Users.csv')
    ureader = csv.reader(userf)
    bookf = open('datasets/BX-Books.csv')
    breader = csv.reader(bookf)
    ratingf = open('datasets/BX-Book-Ratings.csv')
    rreader = csv.reader(ratingf)

    def users(uid):
        bxusers = s.execute('SELECT * FROM bxusers WHERE uid=:uid',
                            {'uid': uid}).fetchone()

    def ratings(uid):
        bxratings = s.execute('SELECT * FROM bxratings WHERE uid=:uid',
                              {'uid': uid}).fetchone()

    def books(isbn):
        bxbooks = s.execute('SELECT * FROM bxbooks WHERE isbn=:isbn',
                            {'isbn': isbn}).fetchone()

    limit = int(input('Limit of BXUsers dataset: '))
    count = 0

    users_tuple = s.execute('SELECT uid FROM bxusers').fetchall()
    #users_list = [users.uid for rating in users_tuple]

    for uid, location, age in ureader:

        if count == limit:
            break

        #if uid in users_list:
            #continue

        try:
            s.execute('INSERT INTO bxusers(uid, isbn, rating)'
                       'VALUES(:uid, :location, :age)',
                       {'uid': uid, 'location': location, 'age': age})
            count += 1

        except DataError as e:
            print(str(e))

        except InternalError as e:
            print(str(e))
            break

        except IntegrityError as e:
            print(str(e))
            continue

    s.commit()
    print('Successfully Added {} BXUsers Dataset'.format(count))

    limit = int(input('Limit of BXBooks dataset: '))
    count = 0

    book_tuple = s.execute('SELECT isbn FROM bxbooks').fetchall()
    #book_list = [books.isbn for user in book_tuple]

    for isbn, title, author, year, publisher, imageurls, imageurlm, imageurll in breader:

        if count == limit:
            break

        #if isbn in book_list:
           # continue

        try:
            s.execute('INSERT INTO bxbooks(isbn, title, author, year, publisher, imageurls, imageurlm, imageurll)'
                      'VALUES(:isbn, :title, :author, :year, :publisher, :imageurls, :imageurlm, :imageurll)',
                      {'isbn': isbn, 'title': title, 'author': author, 'year': year, 'publisher': publisher,
                       'imageurls': imageurls, 'imageurlm': imageurlm, 'imageurll': imageurll})
            count += 1

        except DataError as e:
            print(str(e))

        except InternalError as e:
            print(str(e))
            break

        except IntegrityError as e:
            print(str(e))
            continue

    s.commit()

    print('Successfully Added {} BXBooks Dataset'.format(count))

    limit = int(input('Limit of BXRatings dataset: '))
    count = 0

    rating_tuple = s.execute('SELECT uid FROM bxrating').fetchall()
    #rating_list = [ratings.uid for rating in rating_tuple]

    for uid, isbn, rating in ureader:

        if count == limit:
            break

        #if uid in rating_list:
            #continue

        try:
            s.execute('INSERT INTO ratings(uid, isbn, rating)'
                      'VALUES(:uid, :isbn, :rating)',
                      {'uid': uid, 'isbn': isbn, 'rating': rating})
            count += 1

        except DataError as e:
            print(str(e))

        except InternalError as e:
            print(str(e))
            break

        except IntegrityError as e:
            print(str(e))
            continue

    s.commit()

    print('Successfully Added {} BXRatings Dataset'.format(count))

    s.close()