import argparse
import re
import os
import requests
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import time
import pandas as pd
from functools import wraps

description = """This is the auto-fetcher of papers from Libgen.
The assumptions are that the Libgen regular mirror is used and at the second
position in the mirror list.
Multiple runs on the XLSX input file with a Downloaded column will
skip already downloaded files."""

# Parse arguments
parser = argparse.ArgumentParser(description=description)
parser.add_argument('-xi', '--xlsx-in', metavar='FILE', dest='xlsx_in',
                    help='Excel file with data to download. Must have columns DOI and Title')
parser.add_argument('-d', '--dir', metavar='DIR', dest='dpath',
                    default="./fetched/", help='Download directory [./fetched/].')
args = parser.parse_args()
print(args)

max_len = 200

def retry(exceptions=(Exception,), tries=3, delay=3, backoff=1.5, logger=None, no_fail=True):
    """
    Retry calling the decorated function using an exponential backoff.

    Args:
        exceptions: The exception to check. may be a tuple of
            exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
        logger: Logger to use. If None, print.
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    msg = '{}, Retrying in {} seconds...'.format(e, mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            try:
                result = f(*args, **kwargs)
            except Exception as e:
                if no_fail:
                    result = None
                    print(e)
                else:
                    raise
            return result

        return f_retry  # true decorator

    return deco_retry


@retry()
def download(download_link, path):
    #request.urlretrieve(download_link, filename=path)
    response = requests.get(download_link, allow_redirects=True, timeout=3000)
    if response.status_code == requests.codes.ok:
        with open(path, 'wb') as f:
            f.write(response.content)
        return os.path.getsize(path)
    else:
        return 0


def search_papers(term, page=1):
    params = urlencode({'q': term, 'page': page})
    url = "https://libgen.is/scimag/?{}".format(params)
    tries = 0
    while tries < 3:

        try:
            source = requests.get(url, timeout=10)
            soup = BeautifulSoup(source.text, 'lxml')
            papers_found = re.search(r'(\d+) files found', str(soup))
            break

        except Exception as e:
            print(e)
            tries += 1
            time.sleep(2)
    else:
        papers_found = []


    if papers_found:
        n_papers = int(papers_found.groups()[0])

        page_papers = soup.find_all('tr')
        return page_papers, n_papers

    return None, 0


def save_book(download_link, file_name):
        if os.path.exists(args.dpath) and os.path.isdir(args.dpath):
            bad_chars = '\/:*?"<>|'
            for char in bad_chars:
                file_name = file_name.replace(char, " ")
            print('Downloading...')
            path = os.path.join(args.dpath, file_name)
            if os.name == "nt":
                path = "\\\\?\\" + path.replce("/","\\")  # prevent long Windows filename
            size = download(download_link, path)
            if size and size != 0:
                print('Paper downloaded to {}'.format(os.path.abspath(path)))
                return size
            else:
                try:
                    os.remove(path)
                except:
                    pass
                print("{} download failed...".format(download_link))
        elif os.path.isfile(args.dpath):
            print('The download path is not a directory.')
        else:
            print('The download path does not exist.')

#with open("test_doi.txt","r") as fin:
#    dois = [line.strip() for line in fin]
df = pd.read_excel(args.xlsx_in)
if 'Downloaded' not in df.columns:
    df["Downloaded"] = False

try:
    for index, row in df.iterrows():
        if row["Downloaded"] == True:
            print("Skipping downloaded {}".format(row["Title"]))
            continue
        doi = row["DOI"]
        if pd.isna(doi):
            #print("No DOI for {}".format(row["Title"]))
            continue
        page = 1  # for now only one page is needed
        raw_papers, n_papers = search_papers(doi, page)
        if raw_papers:
            raw_paper = raw_papers[1]
            #def process_paper(raw_paper):
            book_attrs = raw_paper.find_all('td')
            mirrors = book_attrs[4].find_all('a')
            link = mirrors[1]["href"]
            #fname = book_attrs[1].text.strip().split('\n')[0] + ".pdf"
            fname = row["Title"][:max_len] + '.pdf'
            size = save_book(link, fname)
            if size:  # only if the book was downloaded (has size)
                df.at[index, "Downloaded"] = True
        time.sleep(5)
finally:
    df.to_excel("output.xlsx", index=False)
