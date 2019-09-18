import sys

from lxml import html

from crawl_utils import get_file_offline


def main(dirname, fname):
    f = get_file_offline(dirname, fname)
    doc = html.parse(f)

    if doc.xpath('.//button[@id="gsc_bpf_next"]')[0].get("disabled"):
        sys.stdout.write('DONE')
    else:
        sys.stdout.write('GO ON')


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
