import logging
import lzma

from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


logfile = Path(__file__).parent / '.logs'
logging.basicConfig(
    filename=logfile.name, level=logging.INFO, format='%(asctime)s  %(message)s'
)

compressed_webpages_dir = Path(__file__).parent / 'compressed-webpages'
compressed_webpages_dir.mkdir(exist_ok=True)

options = Options()
options.add_argument('--headless=new')

driver = webdriver.Chrome(options=options)

last_fetched_limit_file = Path(__file__).parent / '.last-fetched'
last_fetched_limit_file.touch()

with last_fetched_limit_file.open('r') as f:
    try:
        limit = int(f.read())
    except Exception as e:
        limit = 0

try:
    driver.get(f'https://myanimelist.net/topanime.php?limit={limit}')

    while limit < 26000:

        with lzma.open(
            compressed_webpages_dir / f'webpage-{limit}-{limit + 50}.xz', 'w'
        ) as cf:
            page_source_bytes = driver.page_source.encode()
            cf.write(page_source_bytes)
            logging.info(
                f'{limit:>6} Written {len(page_source_bytes):>6}'
                f' bytes to webpage-{limit}-{limit + 50}.xz'
            )

            with last_fetched_limit_file.open('w') as f:
                f.write(str(limit))

        next_button = driver.find_element(
            by=By.CLASS_NAME, value='link-blue-box.next'
        )
        next_button.click()

        limit += 50

        driver.implicitly_wait(1)

except Exception as e:
    driver.close()
    exit(0)

driver.close()
