import sys
import urlparse

from lxml import html

from crawl_utils import get_file_offline


def main(dirname, fname):
    f = get_file_offline(dirname, fname)
    doc = html.parse(f)
    canonical_url = doc.xpath('.//link[@rel="canonical"]')[0].get("href")
    parsed = urlparse.urlsplit(canonical_url)
    query = urlparse.parse_qs(parsed.query)
    print query['user'][0]

if __name__ == '__main__':
    #main('', 'example_author_page')
    main(sys.argv[1], sys.argv[2])
