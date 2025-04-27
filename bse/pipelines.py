# pipelines.py

import sqlite3
from scrapy.exceptions import DropItem
from pymongo import MongoClient

class PdfLinkDedupPipeline:
    def open_spider(self, spider):
        self.conn = sqlite3.connect('seen_pdfs.db')
        self.cur  = self.conn.cursor()
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS seen_pdfs (
                pdf_url TEXT PRIMARY KEY
            )
        """)
        self.conn.commit()

        # 2) MongoDB setup
        self.client = MongoClient('') #USE YOUR MONGODB URI HERE
        self.db     = self.client['bse']
        self.col    = self.db['announcements']

        # ensure pdf_url is unique in Mongo too
        self.col.create_index('pdf_url', unique=True)

    def close_spider(self, spider):
        self.conn.close()
        self.client.close()

    def process_item(self, item, spider):
        pdf_url = item.get('pdf_url')
        if not pdf_url:
            # no PDF → drop or decide your logic
            raise DropItem("No pdf_url in item")

        # 3) Check SQLite for this pdf_url
        self.cur.execute("SELECT 1 FROM seen_pdfs WHERE pdf_url = ?", (pdf_url,))
        if self.cur.fetchone():
            # we’ve already processed this PDF
            raise DropItem(f"Duplicate PDF link {pdf_url}")

        # 4) Mark as seen in SQLite
        self.cur.execute("INSERT INTO seen_pdfs(pdf_url) VALUES (?)", (pdf_url,))
        self.conn.commit()

        # 5) Insert into MongoDB (will fail if duplicate due to unique index,
        #     so you could catch that if you want to skip silently)
        try:
            self.col.insert_one(dict(item))
        except Exception as e:
            spider.logger.warning(f"Mongo insert failed for {pdf_url}: {e}")

        return item