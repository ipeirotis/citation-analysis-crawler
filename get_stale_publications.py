# Database models
from models import Publication

from sqlalchemy.orm import sessionmaker, load_only
from sqlalchemy import create_engine, func

import sys, os, datetime

class PublicationIDFetcher(object):

    def __init__(self, sql_url):
        self.sql_url = sql_url
        self.engine = create_engine(self.sql_url)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()


    def get_stale_publications_ids(self, number, days_old=7):

        now = datetime.datetime.now()

        # First, get the publications that have never been updated.
        q = self.session.query(Publication)
        q = q.filter(Publication.scholar_id != None,
                     Publication.retrieved_at == None)
        q = q.options(load_only('scholar_id'))
        q = q.order_by(func.rand()).limit(number)

        publications = q.all()

        # Add stale publications (that haven't been updated for
        # a given amount of time).
        if len(publications) < number:
            rest = number - len(publications)
            q2 = self.session.query(Publication)
            q2 = q2.filter(
                Publication.scholar_id != None,
                Publication.retrieved_at < func.subdate(func.curdate(), days_old))
            q2 = q2.options(load_only('scholar_id'))
            q2 = q2.order_by(func.rand()).limit(rest)
            publications.extend(q2.all())

        return [p.scholar_id for p in publications]

if __name__ == '__main__':
    try:
        number = int(sys.argv[1])
    except IndexError:
        number = 1000 # default

    sql_url = os.getenv('SQL_URL')
    if sql_url is None:
        print "Please provide sqlalchemy's SQL_URL in an environment variable"
        sys.exit(1)

    fetcher = PublicationIDFetcher(sql_url)
    ids = fetcher.get_stale_publications_ids(number)

    for id in ids:
        print id
