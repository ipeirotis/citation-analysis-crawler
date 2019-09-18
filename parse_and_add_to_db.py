import os
import datetime
import traceback
import json

from lxml import html
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

# Database models
import models
from crawl_utils import get_file_offline


def add_publication_to_db(publication, session):
    to_add = session.query(models.Publication).filter_by(scholar_id = scholar_id).first()
    if to_add is None:
        to_add = models.Publication()

    to_add.scholar_id = publication.scholar_id
    to_add.title = publication.title
    to_add.type = publication.type
    to_add.year_of_publication = publication.year_of_publication
    to_add.total_citations = publication.total_citations
    to_add.author_names = publication.author_names
    to_add.retrieved_at = publication.retrieved_at
    # New additions
    to_add.venue = publication.venue
    to_add.volume = publication.volume
    to_add.issue = publication.issue
    to_add.publisher = publication.publisher
    to_add.pages = publication.pages


    publication_citations_per_year = []
    for cpy in publication.citations_per_year:
        citations_per_year = session.query(models.PublicationCitationsPerYear).filter_by(publication_id = to_add.id, year = cpy.year).first()
        if citations_per_year is None:
            citations_per_year = models.PublicationCitationsPerYear()
        citations_per_year.year = cpy.year
        citations_per_year.citations = cpy.citations
        publication_citations_per_year.append(citations_per_year)

    if publication_citations_per_year:
        to_add.citations_per_year = publication_citations_per_year

    session.add(to_add)
    session.commit()

def maybe_list_type(items):
    if isinstance(items, list):
        return ('[%s]' % items[0].totypetuple() if items else '[]')
    else:
        return type(items)



class Publication(object):
    def __init__(self):
        self.scholar_id = None
        self.title = None
        self.type = None
        self.year_of_publication = None
        self.total_citations = None
        self.author_names = None
        self.citations_per_year = None
        self.retrieved_at = None

       # New Additions
        self.venue = None
        self.volume = None
        self.issue = None
        self.publisher = None
        self.pages = None


    def tostring(self):
        ret = []
        def out(s):
            ret.append(s)

        out(u'publication.scholar_id: %s' % self.scholar_id)
        out(u'publication.title: %s' % self.title)
        out(u'publication.type: %s' % self.type)
        out(u'publication.year_of_publication: %s' % self.year_of_publication)
        out(u'publication.total_citations: %s' % self.total_citations)
        out(u'publication.author_names: %s' % self.author_names)
        out(u'publication.citations_per_year: [%s]' % ', '.join(cpy.tostringtuple() for cpy in self.citations_per_year))
        out(u'publication.retrieved_at: %s' % self.retrieved_at)
        out(u'publication.venue %s' % self.venue)
        out(u'publication.volume %s' % self.volume)
        out(u'publication.issue %s' % self.issue)
        out(u'publication.publisher %s' % self.publisher)
        out(u'publication.pages %s' % self.pages)

        return u'\n'.join(ret)

    def tostringtuple(self):
        ret = []
        def out(name,value):
            ret.append((name,value))

        out(u'scholar_id', self.scholar_id)
        out(u'title', self.title)
        out(u'type', self.type)
        out(u'year_of_publication', self.year_of_publication)
        out(u'total_citations', self.total_citations)
        out(u'author_names', self.author_names)
        out(u'citations_per_year', '[...]')
        out(u'retrieved_at', self.retrieved_at)
        out(u'venue %s', self.venue)
        out(u'volume %s', self.volume)
        out(u'issue %s', self.issue)
        out(u'publisher %s', self.publisher)
        out(u'pages %s', self.pages)


        return '(%s)' % ','.join('%s=%s' % (name,value) for name,value in ret)

    def totypetuple(self):
        ret = []
        def out(name,value):
            ret.append((name,value))

        out(u'scholar_id', type(self.scholar_id))
        out(u'title', type(self.title))
        out(u'type', type(self.type))
        out(u'year_of_publication', type(self.year_of_publication))
        out(u'total_citations', type(self.total_citations))
        out(u'author_names', type(self.author_names))
        out(u'citations_per_year', maybe_list_type(self.citations_per_year))
        out(u'retrieved_at', type(self.retrieved_at))
        out(u'venue %s', type(self.venue))
        out(u'volume %s', type(self.volume))
        out(u'issue %s', type(self.issue))
        out(u'publisher %s', type(self.publisher))
        out(u'pages %s', type(self.pages))


        return '(%s)' % ','.join('%s=%s' % (name,value) for name,value in ret)


    def inspect_fields(self):
        print 'publication.scholar_id:', type(self.scholar_id)
        print 'publication.title:', type(self.title)
        print 'publication.type:', type(self.type)
        print 'publication.year_of_publication:', type(self.year_of_publication)
        print 'publication.total_citations:', type(self.total_citations)
        print 'publication.author_names:', type(self.author_names)
        print 'publication.citations_per_year: [%s]' % ', '.join(cpy.totypetuple() for cpy in self.citations_per_year)
        print 'publication.retrieved_at:', type(self.retrieved_at)
        print 'publication.venue %s', type(self.venue)
        print 'publication.volume %s', type(self.volume)
        print 'publication.issue %s', type(self.issue)
        print 'publication.publisher %s', type(self.publisher)
        print 'publication.pages %s', type(self.pages)


class PublicationCitationsPerYear(object):
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


def show_crawled_data(scholar_id):
    f = get_file_offline('publications_cache', scholar_id)

    publication = handle_file(f, scholar_id)
    print 'Publication object:'
    print publication.tostring().encode('utf8')
    print 'Types of the data in its fields:'
    print publication.inspect_fields()


def crawl_publication(scholar_id, sql_url):
    """
    Crawls Google Scholar in order to retrieve information about a publication.
    """

    print 'Show data for ' + scholar_id + '.'

    f = get_file_offline('publications_cache', scholar_id)

    publication = handle_file(f, scholar_id)

    engine = create_engine(sql_url)
    Session = sessionmaker(bind=engine)

    session = Session()
    add_publication_to_db(publication, session)

def handle_file(res, scholar_id):
    doc = html.parse(res)

    publication = Publication()

    publication.scholar_id = scholar_id

    ntitle = doc.find('.//a[@class="gsc_title_link"]')
    if ntitle is not None:

        # The title of the publication.
        ptitle = ntitle.text_content()
        ptitle = unicode(ptitle)
        publication.title = ptitle

    ntype = doc.find('.//div[@class="gs_scl"][3]//div[@class="gsc_field"]')
    if ntype is not None:

        # The type of the publication.
        ntype_str = unicode(ntype.text_content(), encoding="utf-8")
        publication.type = ntype_str
        if publication.type == 'Description':
            publication.type = 'Other'

    nyear = doc.xpath('.//div[text()="Publication date"]/ancestor::div[@class="gs_scl"]//div[@class="gsc_value"]')
    if nyear is not None and len(nyear):

        # The year of the publication.
        publication.year_of_publication = int(nyear[0].text.split('/')[0])

    ncitations = doc.xpath('.//div[text()="Total citations"]/ancestor::div[@class="gs_scl"]//div[@class="gsc_value"]//a')
    if ncitations is not None and len(ncitations):

        # The total citations for the publication.
        publication.total_citations = ncitations[0].text.split(' ')[-1]

    nauthors = doc.xpath('.//div[text()="Authors"]/ancestor::div[@class="gs_scl"]//div[@class="gsc_value"]')
    if nauthors is not None and len(nauthors):

        # The authors of the publication.
        publication.author_names = nauthors[0].text

    # The citations per year for the publication.
    publication_citations_per_year = []
    nhistogram = doc.find('.//div[@id="gsc_vcd_graph_bars"]')

    citations_per_year = None

    if nhistogram is not None:
        years = [x.text for x in nhistogram.xpath('.//span[@class="gsc_vcd_g_t"]')]
        for a in nhistogram.xpath('.//a[@class="gsc_vcd_g_a"]'):
            i = int(a.get('style').split('z-index:')[1])
            year = int(years[-i])
            citations_per_year = PublicationCitationsPerYear()
            citations_per_year.year = int(years[-i])
            citations_per_year.citations = int(a.xpath('./span[@class="gsc_vcd_g_al"]')[0].text)
            publication_citations_per_year.append(citations_per_year)
        if publication_citations_per_year:
            publication.citations_per_year = publication_citations_per_year

    # New additions
    nvenue = doc.xpath('.//div[text()="Journal"]/ancestor::div[@class="gs_scl"]//div[@class="gsc_value"]')
    if nvenue is not None and len(nvenue):
        publication.venue = nvenue[0].text

    # New additions
    nvolume = doc.xpath('.//div[text()="Volume"]/ancestor::div[@class="gs_scl"]//div[@class="gsc_value"]')
    if nvolume is not None and len(nvolume):
        try:
            publication.volume = int(nvolume[0].text)
        except ValueError:
            pass

    # New additions
    nissue = doc.xpath('.//div[text()="Issue"]/ancestor::div[@class="gs_scl"]//div[@class="gsc_value"]')
    if nissue is not None and len(nissue):
        try:
            publication.issue = int(nissue[0].text)
        except ValueError:
            pass

    # New additions
    npublisher = doc.xpath('.//div[text()="Publisher"]/ancestor::div[@class="gs_scl"]//div[@class="gsc_value"]')
    if npublisher is not None and len(npublisher):
        publication.publisher = npublisher[0].text

    # New additions
    npages = doc.xpath('.//div[text()="Pages"]/ancestor::div[@class="gs_scl"]//div[@class="gsc_value"]')
    if npages is not None and len(npages):
        publication.pages = npages[0].text


    # When information about the author was retrieved from Google
    # Scholar.
    # TODO: Get timestamp from file.
    publication.retrieved_at = datetime.datetime.now()

    print 'Crawled publication with id == ' + scholar_id
    # print 'Publication object:'
    # print publication.tostring()
    # print 'Types of the data in its fields:'
    # print publication.inspect_fields()

    # TODO: We could do some checking of the crawled data here.
    # For example, if we got the title:
    # if publication.title is None:
    #         print 'No title means error for sure.'
    #         sys.exit(1)

    return publication

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
        crawl_publication(scholar_id, sql_url)
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
        error_json_fname = 'error_publication_%s_%s.json' % (now.strftime('%Y-%m-%d_%H%M%S'), scholar_id)
        print 'Will write full traceback to file %s' % error_json_fname
        with open(error_json_fname, 'w') as g:
            json.dump(error, g)
