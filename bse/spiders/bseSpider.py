import scrapy
import json
import fitz  # PyMuPDF
from datetime import datetime, timedelta

class BseSpider(scrapy.Spider):
    name = "bse_spider"
    allowed_domains = ["bseindia.com"]

    custom_headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://www.bseindia.com",
        "Referer": "https://www.bseindia.com/",
        "Accept-Language": "en-US,en;q=0.9"
    }

    def __init__(self, start_date=None, end_date=None, *args, **kwargs):
        super(BseSpider, self).__init__(*args, **kwargs)
        today = datetime.today()

        # parse or default start_date
        if start_date:
            self.start_date = datetime.strptime(start_date, "%Y%m%d")
        else:
            self.start_date = today

        # parse or default end_date
        if end_date:
            self.end_date = datetime.strptime(end_date, "%Y%m%d")
        else:
            self.end_date = today

        if self.start_date > self.end_date:
            raise ValueError("start_date must be before or equal to end_date")

    def start_requests(self):
        current = self.start_date
        while current <= self.end_date:
            date_str = current.strftime("%Y%m%d")
            url = (
                f"https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/"
                f"w?pageno=1&strCat=-1&strPrevDate={date_str}"
                f"&strScrip=&strSearch=P&strToDate={date_str}"
                f"&strType=C&subcategory=-1"
            )
            yield scrapy.Request(
                url,
                headers=self.custom_headers,
                callback=self.parse_page,
                meta={"date_str": date_str, "pageno": 1},
            )
            current += timedelta(days=1)

    def parse_page(self, response):
        date_str = response.meta["date_str"]
        pageno  = response.meta["pageno"]
        data = json.loads(response.text)
        rows = data.get("Table", [])

        if not rows:
            return

        for item in rows:
            pdf_rel = item.get("ATTACHMENTNAME")
            if not pdf_rel:
                continue

            pdf_url = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{pdf_rel}"
            meta = {
                "news_id":           item.get("NEWSID"),
                "company_name":      item.get("SLONGNAME"),
                "headline":          item.get("HEADLINE"),
                "announcement_type": item.get("ANNOUNCEMENT_TYPE"),
                "news_date":         item.get("NEWS_DT"),
                "pdf_url":           pdf_url,
                "company_url":       item.get("NSURL"),
            }

            yield scrapy.Request(
                pdf_url,
                headers=self.custom_headers,
                callback=self.parse_pdf,
                meta={"announcement": meta},
                dont_filter=True
            )

        # next page
        next_page = pageno + 1
        next_url = (
            f"https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/"
            f"w?pageno={next_page}&strCat=-1&strPrevDate={date_str}"
            f"&strScrip=&strSearch=P&strToDate={date_str}"
            f"&strType=C&subcategory=-1"
        )
        yield scrapy.Request(
            next_url,
            headers=self.custom_headers,
            callback=self.parse_page,
            meta={"date_str": date_str, "pageno": next_page},
        )

    def parse_pdf(self, response):
        ann = response.meta["announcement"]
        pdf_url = ann["pdf_url"]

    # Extract text (but don't print it)
        text = self.extract_text_from_pdf(response.body, pdf_url)

    # If extraction failed, set the failure flag
        if not text:
            ann["status"] = "failed"
        else:
            ann["status"] = "success"

    # format date/time
        try:
            dt = datetime.fromisoformat(ann["news_date"])
            date_fmt = dt.strftime("%d %b %Y")
            time_fmt = dt.strftime("%H:%M")
        except ValueError:
            date_fmt, time_fmt = ann["news_date"], ""

    # Add the status and other necessary fields for pipeline
        yield {
            "news_id":      ann["news_id"],
            "company":      ann["company_name"],
            "announcement": ann["headline"],
        "date":         date_fmt,
        "time":         time_fmt,
        "company_url":  ann["company_url"],
        "pdf_url":      pdf_url,
        "pdf_text":     text.strip() if text else "",
        "status":       ann["status"],  # Success or failed
    }

    def extract_text_from_pdf(self, pdf_bytes, pdf_url):
        try:
            text = ""
            with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
                for page in doc:
                    text += page.get_text()

            if text.strip():
                self.logger.info(f"✅ Extracted text from PDF: {pdf_url}")
            else:
                self.logger.warning(f"⚠️ No text found in PDF: {pdf_url}")

            return text

        except Exception as e:
            self.logger.error(f"❌ Failed to extract PDF ({pdf_url}): {e}")
            return ""