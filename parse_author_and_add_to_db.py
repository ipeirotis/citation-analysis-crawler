import datetime
import traceback
import json
import os
import re

from lxml import html
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

# Database models
import models
from crawl_utils import get_file_offline, get_author_filenames
from parse_and_add_to_db import Publication, maybe_list_type

def get_author_from_db(scholar_id, session):
    to_add = session.query(models.Author).filter_by(scholar_id = scholar_id).first()
    if to_add is None:
        to_add = models.Author()
    return to_add


def add_author_to_db(author, session, org=None):
    to_add = get_author_from_db(author.scholar_id, session)

    to_add.scholar_id = author.scholar_id
    to_add.name = author.name
    to_add.email_domain = author.email_domain
    to_add.total_citations = author.total_citations
    to_add.h_index = author.h_index
    to_add.i10_index = author.i10_index

    # Try to assign organization by a tag in the html/Google Org_id
    if not to_add.organization and org:
        candidate_org = session.query(models.Organization).filter_by(
            scholar_org_id = org).first()
        if candidate_org:
            to_add.organization = candidate_org
            to_add.auto_org_assignment = True

    # Try to assign organization by email domain
    if not to_add.organization and author.email_domain:
        clg = session.query(models.Author).filter_by(
            email_domain = author.email_domain).first()
        candidate_org = clg.organization if clg else None
        if candidate_org and candidate_org.parent:
            to_add.organization = candidate_org.ancestors()[-1]
            to_add.auto_org_assignment = True
        elif candidate_org:
            to_add.organization = candidate_org
            to_add.auto_org_assignment = True


    # # LIST:
    # citations_per_year
    author_citations_per_year = []
    for cpy in author.citations_per_year:
        citations_per_year = session.query(models.AuthorCitationsPerYear).filter_by(author_id = to_add.id, year = cpy.year).first()
        if citations_per_year is None:
            citations_per_year = models.AuthorCitationsPerYear()
        citations_per_year.year = cpy.year
        citations_per_year.citations = cpy.citations
        author_citations_per_year.append(citations_per_year)


    # # LIST:
    # coauthors
    author_coauthors = []
    for co in author.coauthors:
        coauthor = session.query(models.Author).filter_by(scholar_id = co.scholar_id).first()
        if coauthor is None:
            coauthor = models.Author()
        coauthor.scholar_id = co.scholar_id
        author_coauthors.append(coauthor)
    to_add.coauthors = author_coauthors


    # # LIST:
    # publications
    author_publications = []
    for pub in author.publications:
        publication = session.query(models.Publication).filter_by(scholar_id = pub.scholar_id).first()
        if publication is None:
            publication = models.Publication()
        publication.scholar_id = pub.scholar_id
        publication.title = pub.title
        publication.year_of_publication = pub.year_of_publication
        publication.total_citations = pub.total_citations
        author_publications.append(publication)
    to_add.publications = author_publications

    to_add.retrieved_at = datetime.datetime.now()

    session.add(to_add)
    session.commit()

AUTHORS_DIR='authors_cache'

def extract_author(scholar_id):
    author = Author()

    pub_pages, histo_page, coauthors_page = get_author_filenames(AUTHORS_DIR, scholar_id)


    with get_file_offline(AUTHORS_DIR, pub_pages[0]) as f:
        doc = html.parse(f)

    no_content = doc.xpath('.//div[contains(text(), "Sorry, no content found for this URL")]')
    if len(no_content):
        print 'Author ' + scholar_id + ' not found.'
        return 'Done.'

    author.scholar_id = scholar_id

    rxpr = re.compile(r'view_op=view_org.*org=(?P<org>[^s+\&]+)').search
    norgs = [rxpr(x.get('href', '')).group('org') for x in doc.xpath('.//a')
             if rxpr(x.get('href', ''))]

    org = norgs[0] if norgs else None

    nname = doc.find('.//div[@id="gsc_prf_in"]')
    if nname is not None:
        # The name of the author.
        author.name = unicode(nname.text_content())

    nemaildomain = doc.find('.//div[@id="gsc_prf_ivh"]')
    if nemaildomain is not None:
        # The domain where the author has an email.
        author.email_domain = nemaildomain.text_content().split(" - ")[0].split()[-1]

    ncitations = doc.find('.//table[@id="gsc_rsb_st"]')
    if ncitations is not None:

        # The total citations for the author.
        author.total_citations = ncitations.xpath('.//tr[2]/td')[1].text

        # The h-index for the author.
        author.h_index = ncitations.xpath('.//tr[3]/td')[1].text

        # The i10-index for the author.
        author.i10_index = ncitations.xpath('.//tr[4]/td')[1].text


    with get_file_offline(AUTHORS_DIR, histo_page) as f:
        doc = html.parse(f)

    # The citations per year for the author.
    author_citations_per_year = []
    nhistogram = doc.find('.//div[@id="gsc_md_hist_b"]')
    if nhistogram is not None:
        years = [x.text for x in nhistogram.xpath('.//span[@class="gsc_g_t"]')]
        for a in nhistogram.xpath('.//a[@class="gsc_g_a"]'):
            i = int(a.get('style').split('z-index:')[1])
            year = int(years[-i])
            citations_per_year = AuthorCitationsPerYear()
            citations_per_year.year = int(years[-i])
            citations_per_year.citations = int(a.xpath('./span[@class="gsc_g_al"]')[0].text)
            author_citations_per_year.append(citations_per_year)
    author.citations_per_year = author_citations_per_year


    with get_file_offline(AUTHORS_DIR, coauthors_page) as f:
        doc = html.parse(f)

    # The co-authors of the author.
    author_coauthors = []
    for a in doc.xpath('.//h3[@class="gsc_1usr_name"]//a'):
        co_scholar_id = a.get('href').split('user=')[1].split('&hl')[0]
        coauthor = Author()
        coauthor.scholar_id = co_scholar_id
        author_coauthors.append(coauthor)
    author.coauthors = author_coauthors


    # The publications.
    author_publications = []

    for pub_page in pub_pages:
        with get_file_offline(AUTHORS_DIR, pub_page) as f:
            doc = html.parse(f)

        for tr in doc.xpath('.//tr[@class="gsc_a_tr"]'):
            a = tr.find('.//td[@class="gsc_a_t"]//a')
            # NOTE: When there are no publications, there is a single tr.
            # <tr class="gsc_a_tr"><td class="gsc_a_e" colspan="3">There are no articles in this profile.</td></tr>
            if a is None:
                continue
            purl = a.get('href')

            # The ID of the publication in Google Scholar.
            pub_scholar_id = purl.split('citation_for_view=')[1]

            # Retrieve the publication with that ID (if any).
            publication = Publication()
            publication.scholar_id = pub_scholar_id

            # The title of the publication.
            publication.title = unicode(a.text_content())

            pub_nyear = tr.find('.//td[@class="gsc_a_y"]//span')
            if pub_nyear is not None:
                year_of_publication = pub_nyear.text_content().strip()
                if year_of_publication:
                    # The year of the publication.
                    publication.year_of_publication = int(year_of_publication)
                else:
                    publication.year_of_publication = None

            pub_ncitations = tr.find('.//a[@class="gsc_a_ac"]')

            if pub_ncitations is not None:
                total_citations = pub_ncitations.text_content().strip()
                if total_citations:
                    # The total citations for the publication.
                    publication.total_citations = int(total_citations)
                else:
                    publication.total_citations = None

            author_publications.append(publication)

    author.publications = author_publications

    return author, org


class Author(object):
    def __init__(self):
        self.name = None
        self.title = None
        self.organization_id = None
        self.year_of_phd = None
        self.tenured = None
        self.scholar_id = None
        self.website_url = None
        self.email_domain = None
        self.total_citations = None
        self.h_index = None
        self.i10_index = None
        self.retrieved_at = None
        # Lists
        self.coauthors = []
        self.citations_per_year = []
        self.publications = []

    def tostring(self):
        ret = []
        def out(s):
            ret.append(s)

        out(u'author.scholar_id: %s' % self.scholar_id)
        out(u'author.name: %s' % self.name)
        out(u'author.title: %s' % self.title)
        out(u'author.organization_id: %s' % self.organization_id)
        out(u'author.year_of_phd: %s' % self.year_of_phd)
        out(u'author.tenured: %s' % self.tenured)
        out(u'author.website_url: %s' % self.website_url)
        out(u'author.email_domain: %s' % self.email_domain)
        out(u'author.total_citations: %s' % self.total_citations)
        out(u'author.h_index: %s' % self.h_index)
        out(u'author.i10_index: %s' % self.i10_index)
        out(u'author.retrieved_at: %s' % self.retrieved_at)

        out(u'author.citations_per_year: [%s]' % ', '.join(cpy.tostringtuple() for cpy in self.citations_per_year))

        out(u'author.coauthors: [%s]' % ', '.join(co.tostringtuple() for co in self.coauthors))

        out(u'author.publications: [%s]' % ', '.join(pub.tostringtuple() for pub in self.publications))

        return u'\n'.join(ret)

    def tostringtuple(self):
        ret = []
        def out(name, value):
            if value is not None:
                ret.append((name,value))

        out(u'scholar_id', self.scholar_id)
        out(u'name', self.name)
        out(u'title', self.title)
        out(u'organization_id', self.organization_id)
        out(u'year_of_phd', self.year_of_phd)
        out(u'tenured', self.tenured)
        out(u'website_url', self.website_url)
        out(u'email_domain', self.email_domain)
        out(u'total_citations', self.total_citations)
        out(u'h_index', self.h_index)
        out(u'i10_index', self.i10_index)
        out(u'retrieved_at', self.retrieved_at)

        return '(%s)' % ','.join('%s=%s' % (name,value) for name,value in ret)


    def totypetuple(self):
        ret = []
        def out(name, value):
            if isinstance(value, list):
                value_type = maybe_list_type(value)
            else:
                value_type = type(value)
            ret.append((name,value_type))

        out(u'scholar_id', self.scholar_id)
        out(u'name', self.name)
        out(u'title', self.title)
        out(u'organization_id', self.organization_id)
        out(u'year_of_phd', self.year_of_phd)
        out(u'tenured', self.tenured)
        out(u'website_url', self.website_url)
        out(u'email_domain', self.email_domain)
        out(u'total_citations', self.total_citations)
        out(u'h_index', self.h_index)
        out(u'i10_index', self.i10_index)
        out(u'retrieved_at', self.retrieved_at)

        out(u'citations_per_year', self.citations_per_year)
        out(u'coauthors', self.coauthors)
        out(u'publications', self.publications)

        return '(%s)' % ','.join('%s=%s' % (name, value) for name,value in ret)


    def inspect_fields(self):
        print 'author.scholar_id:', type(self.scholar_id)
        print 'author.name:', type(self.name)
        print 'author.title:', type(self.title)
        print 'author.organization_id:', type(self.organization_id)
        print 'author.year_of_phd:', type(self.year_of_phd)
        print 'author.tenured:', type(self.tenured)
        print 'author.website_url:', type(self.website_url)
        print 'author.email_domain:', type(self.email_domain)
        print 'author.total_citations:', type(self.total_citations)
        print 'author.h_index:', type(self.h_index)
        print 'author.i10_index:', type(self.i10_index)
        print 'author.retrieved_at:', type(self.retrieved_at)

        print 'author.citations_per_year: [%s]' % ', '.join(cpy.totypetuple() for cpy in self.citations_per_year)
        print 'author.coauthors: [%s]' % ', '.join(co.totypetuple() for co in self.coauthors)
        print 'author.publications: [%s]' % ', '.join(pub.totypetuple() for pub in self.publications)


class AuthorCitationsPerYear(object):
    def __init__(self):
        self.year = None
        self.citations = None

    def tostring(self):
        return (('citations_per_year.year: %s\n' +
                 'citations_per_year.citations: %s') % (self.year, self.citations))

    def tostringtuple(self):
        return '(%s,%s)' % (self.year, self.citations)

    def totypetuple(self):
        return '(%s,%s)' % (type(self.year), type(self.citations))


def main(scholar_id, sql_url):
    author, org = extract_author(scholar_id)

    # print author.tostring()

    engine = create_engine(sql_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    add_author_to_db(author, session, org)



if __name__ == '__main__':
    import sys

    try:
        scholar_id = sys.argv[1]
    except IndexError:
        print 'USAGE: python %s SCHOLAR_ID'
        sys.exit(1)

    try:
        command = sys.argv[2]
    except IndexError:
        command = 'default'

    if command == 'debug':
        show_crawled_data(scholar_id)
        sys.exit(0)


    sql_url = os.getenv('SQL_URL')
    if sql_url is None:
        print "Please provide sqlalchemy's SQL_URL in an environment variable"
        sys.exit(1)

    try:
        main(scholar_id, sql_url)
    except Exception as e:
        print 'Error (exception %s)' % e
        exp = traceback.format_exc()
        print 'Traceback:'
        print exp
        error = {
            'traceback':exp,
            'scholar_id':scholar_id,
        }
        now = datetime.datetime.now()
        error_json_fname = 'error_author_%s_%s.json' % (now.strftime('%Y-%m-%d_%H%M%S'), scholar_id)
        print 'Will write full traceback to file %s' % error_json_fname
        with open(error_json_fname, 'w') as g:
            json.dump(error, g)
