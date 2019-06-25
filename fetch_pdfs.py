import argparse
import re
import os
import requests
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import chromedriver_binary
from selenium import webdriver
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
YOUR_EMAIL = "ognjen011@gmail.com" # you should put your own email address here
pause = 5
bad_chars = '\/:*?"<>|'

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
            print('Downloading...')
            path = os.path.join(args.dpath, file_name)
            if os.name == "nt":
                path = "\\\\?\\" + path.replace("/","\\")  # prevent long Windows filename
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


def libgen_io_dl(mirrors, fname):
    link = mirrors[1]["href"]
    size = save_book(link, fname)
    return size


def chrome_file(f):
    return f.endswith("crdownload") or f.startswith(".com.google.Chrome") or \
    f.endswith("tmp")


def booksc_dl(mirrors, fname):
    links = []
    prefix = "http://booksc.xyz"
    try:
        links.append(mirrors[2]["href"])
        source = requests.get(links[-1])
        soup = BeautifulSoup(source.text, 'lxml')
        links.append(prefix + soup.find("h3").find("a")["href"])
    except:
        print("Something went wrong with following links on BookSC")
        return 0
    #source = requests.get(links[-1])
    #soup = BeautifulSoup(source.text, 'lxml')
    #sublink = soup.find("a", {"class":"btn btn-primary dlButton"})["href"]
    #links.append(prefix + sublink)

    # Get files before download
    for (dirpath, dirnames, filenames) in os.walk(args.dpath):
        before = [f for f in filenames if not chrome_file(f)]
        break

    # Grab by Selenium
    driver.get(links[-1])
    el = driver.find_elements_by_xpath("//a[@class='btn btn-primary dlButton']")
    if len(el) != 1:
        print("BOOKSC WEB LAYOUT COULD BE CHANGED!!!")
        return 0
    el[0].click()

    # Wait until the file is downloaded
    timeout = 300
    t = time.time()
    while(time.time()-t) < timeout:
        for (dirpath, dirnames, filenames) in os.walk(args.dpath):
            after = [f for f in filenames if not chrome_file(f)]
            break
        if len(after) - len(before) == 1:
            new_file = (set(after)-set(before)).pop()
            print("Found download: {}".format(new_file))
            break
        if len(after) - len(before) > 1:
            raise ValueError("More than one file downloaded!")
    else:
        print("Timeout for download on BookSC reached! Continuing...")
        return 0

    # Find and rename file
    if os.name == "nt":
        curr_name = "\\\\?\\" + os.path.join(args.dpath, new_file).replace("/","\\")
        final_fname = "\\\\?\\" + os.path.join(args.dpath, fname).replace("/","\\")
    else:
        curr_name = os.path.join(args.dpath, new_file)
        final_fname = os.path.join(args.dpath, fname)
    try:
        os.rename(curr_name, final_fname)
    except:
        print("File already exists or no permission?")
    return os.path.getsize(final_fname)


def unpaywall(doi, retry=0, pdfonly=True):
    """Find legal open access version of paper"""


    r = requests.get("https://api.unpaywall.org/v2/{}".format(doi), params={"email":YOUR_EMAIL})

    if r.status_code == 404:
        print("Invalid/unknown DOI {}".format(doi))
        return None

    if r.status_code == 500:
        print("Unpaywall API failed. Try: {}/3".format(retry+1))

        if retry < 3:
            return unpaywall(doi, retry+1)
        else:
            print("Retried 3 times and failed. Giving up")
            return None

    best_loc = None
    try:
        best_loc = r.json()['best_oa_location']
    except json.decoder.JSONDecodeError:
        print("Response was not json")
        print(r.text)
    except KeyError:
        print("best_oa_location not set")
        print(r.text)
    except:
        print("Something weird happened")
        print(r.text)
        return None


    if not r.json()['is_oa'] or best_loc is None:
        print("No OA paper found for {}".format(doi))
        return None

    if(best_loc['url_for_pdf'] is None and pdfonly is True):
        print("No PDF found..")
        print(best_loc)
        return None
    else:
        return best_loc['url']

    return best_loc['url_for_pdf']


mirror_funs = [booksc_dl, libgen_io_dl]

# Prepare Chrome Selenium
options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument("--test-type")
prefs = {"download.default_directory" : os.path.abspath(args.dpath)}
options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(options=options)
#driver = webdriver.Chrome("C:/Users/Korisnik/Code/chromedriver.exe", options=options)

# Log in Booksc.xyz
email = "ognjen.milicevic@med.bg.ac.rs"
password = "statistika1"
driver.get("http://singlelogin.org/?from=booksc.xyz")
driver.find_element_by_id("username").send_keys(email)
driver.find_element_by_id("password").send_keys(password)
driver.find_element_by_xpath("//button[@type='submit']").click()

# Start processing
df = pd.read_excel(args.xlsx_in)
if 'Downloaded' not in df.columns:
    df["Downloaded"] = False

try:
    for index, row in df.iterrows():
        if row["Downloaded"] == True:
            print("Skipping downloaded {}".format(row["Title"]))
            continue
        doi = row["DOI"]
        basename = row["Authors"].split(",")[0] + " - " + row["Title"]
        fname = basename[:max_len] + '.pdf'
        for char in bad_chars:
            fname = fname.replace(char, " ")
        if pd.isna(doi):
            #print("No DOI for {}".format(row["Title"]))
            continue

        # Try one of the free services
        print("DOI")
        print(doi)
        link = unpaywall(doi)
        if link:
            size = save_book(link, fname)
            if size:
                df.at[index, "Downloaded"] = True
                time.sleep(pause)
                continue

        # Try Libgen
        page = 1  # for now only one page is needed
        raw_papers, n_papers = search_papers(doi, page)
        if raw_papers:
            raw_paper = raw_papers[1]
            #def process_paper(raw_paper):
            book_attrs = raw_paper.find_all('td')
            mirrors = book_attrs[4].find_all('a')
            for mirror_fun in mirror_funs:  # for each mirror
                #fname = book_attrs[1].text.strip().split('\n')[0] + ".pdf"
                size = mirror_fun(mirrors, fname)
                if size:  # only if the book was downloaded (has size)
                    df.at[index, "Downloaded"] = True
                    break  # if successful, stop checking mirrors
        time.sleep(pause)
finally:
    df.to_excel("output.xlsx", index=False)
