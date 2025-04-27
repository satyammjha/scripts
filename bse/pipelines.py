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

        # MongoDB setup
        self.client = MongoClient('mongodb+srv://admin:Wt9cPRsB3eZazNeA@cluster0.hmuzu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
        self.db     = self.client['bse']
        self.success_col = self.db['announcements']
        self.failed_col  = self.db['failed_downloads']

        # Ensure pdf_url is unique in both MongoDB collections
        self.success_col.create_index('pdf_url', unique=True)
        self.failed_col.create_index('pdf_url', unique=True)

    def close_spider(self, spider):
        self.conn.close()
        self.client.close()

    def process_item(self, item, spider):
        pdf_url = item.get('pdf_url')
        if not pdf_url:
            # no PDF → drop or decide your logic
            raise DropItem("No pdf_url in item")

        # Check SQLite for this pdf_url
        self.cur.execute("SELECT 1 FROM seen_pdfs WHERE pdf_url = ?", (pdf_url,))
        if self.cur.fetchone():
            # We’ve already processed this PDF
            raise DropItem(f"Duplicate PDF link {pdf_url}")

        # Mark as seen in SQLite
        self.cur.execute("INSERT INTO seen_pdfs(pdf_url) VALUES (?)", (pdf_url,))
        self.conn.commit()

        # Insert into MongoDB collections based on status
        if item['status'] == "success":
            try:
                self.success_col.insert_one(dict(item))
            except Exception as e:
                spider.logger.warning(f"Mongo insert failed for {pdf_url}: {e}")
        elif item['status'] == "failed":
            try:
                self.failed_col.insert_one(dict(item))
            except Exception as e:
                spider.logger.warning(f"Mongo insert failed for failed download {pdf_url}: {e}")

        return item

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
        self.client = MongoClient('mongodb+srv://admin:Wt9cPRsB3eZazNeA@cluster0.hmuzu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
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