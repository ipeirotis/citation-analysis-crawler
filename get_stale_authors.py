# Database models
from models import Author

from sqlalchemy.orm import sessionmaker, load_only
from sqlalchemy import create_engine, func

import sys, os, datetime

class AuthorIDFetcher(object):

    def __init__(self, sql_url):
        self.sql_url = sql_url
        self.engine = create_engine(self.sql_url)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()


    def get_stale_authors_ids(self, number, days_old=30):

        now = datetime.datetime.now()

        # First, get the authors that have never been updated.
        #
        # Note: Only authors with a name are crawled.  Authors without
        # a name are only co-authors, and are not refreshed.
        q = self.session.query(Author)
        q = q.filter(Author.scholar_id != None,
                     Author.name != None,
                     Author.organization_id != None,
                     Author.retrieved_at == None)
        q = q.options(load_only('scholar_id'))
        q = q.order_by(func.rand()).limit(number)

        authors = q.all()

        # Add stale authors (that haven't been updated for
        # a given amount of time).
        #
        # Note: Only authors with a name are crawled.  Authors without
        # a name are only co-authors, and are not refreshed.
        if len(authors) < number:
            rest = number - len(authors)
            q2 = self.session.query(Author)
            q2 = q2.filter(
                Author.scholar_id != None,
                Author.name != None,
                Author.organization_id != None,
                Author.retrieved_at < func.subdate(func.curdate(), days_old))
            q2 = q2.options(load_only('scholar_id'))
            q2 = q2.order_by(func.rand()).limit(rest)
            authors.extend(q2.all())

        return [p.scholar_id for p in authors]

if __name__ == '__main__':
    try:
        number = int(sys.argv[1])
    except IndexError:
        number = 1000 # default

    sql_url = os.getenv('SQL_URL')
    if sql_url is None:
        print "Please provide sqlalchemy's SQL_URL in an environment variable"
        sys.exit(1)

    fetcher = AuthorIDFetcher(sql_url)
    ids = fetcher.get_stale_authors_ids(number)

    for id in ids:
        print id
