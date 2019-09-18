import gzip
import os
import glob

def get_file_offline(dirname, scholar_id):
    cached_fname = os.path.join(dirname, '%s.html' % scholar_id)
    if os.path.isfile(cached_fname):
        return open(cached_fname)
    cached_gzipped_fname = os.path.join(dirname, '%s.html.gz' % scholar_id)
    if os.path.isfile(cached_gzipped_fname):
        return gzip.open(cached_gzipped_fname, 'rb')

    raise RuntimeError("Webpage for scholar id %s is not in the cache." % scholar_id)

COAUTHORS_TAG = 'colleagues'
HISTOGRAM_TAG = 'histogram'

def get_author_filenames(dirname, scholar_id):
    fnames = [fname for fname in os.listdir(dirname) if fname.startswith(scholar_id)]
    ret = []
    histo_fname = None
    coauthors_fname = None
    for fname in fnames:
        if '-' not in fname:
            continue

        part = fname
        if part.endswith('.gz'):
            part = part[:-3]
        if part.endswith('.html'):
            part = part[:-5]
        scholar_id, _, tag = part.rpartition('-')

        if tag == COAUTHORS_TAG:
            coauthors_fname = part
        elif tag == HISTOGRAM_TAG:
            histo_fname = part
        else:
            count = int(tag)
            ret.append((count,part))
    ret = sorted(ret, key=lambda (count, _): count)
    ret = [part for (_, part) in ret]

    return ret, histo_fname, coauthors_fname
