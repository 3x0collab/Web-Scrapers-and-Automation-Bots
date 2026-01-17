# 🕷️ Web Scraping & Automation Suite

**Status:** ✅ Active / Maintenance

### 📖 Overview
This repository houses a collection of **custom bots and scrapers** designed to automate repetitive digital tasks and extract data from complex, dynamic websites.

Unlike basic HTML parsers, these scripts are built to handle:
* **Dynamic Rendering:** scraping JavaScript-heavy sites (React/Angular) using Selenium/Playwright.
* **Anti-Bot Evasion:** User-Agent rotation and proxy integration to avoid IP bans.
* **Data Pipeline:** Automatically cleaning and exporting data to JSON/CSV or pushing directly to a PostgreSQL database.

### 📂 Modules Included

| Script Name | Function | Target Use Case |
| :--- | :--- | :--- |
| **Price_Tracker_Bot** | Monitors e-commerce/crypto prices every 30s. | Arbitrage & Deal Hunting |
| **Social_Auto_Reply** | Automates engagement on social platforms. | Marketing Automation |
| **Lead_Harvester** | Extracts emails and contact info from directories. | B2B Lead Gen |
| **News_Aggregator** | Scrapes headlines from 50+ financial news sites. | Market Sentiment Analysis |

### 🛠️ Tech Stack

* **Core:** Python 3.10+
* **Browser Automation:** Selenium WebDriver, Playwright
* **Parsing:** BeautifulSoup4, lxml
* **Data Handling:** Pandas (for structuring CSV/Excel exports)
* **Network:** Requests, URLLib

### 🚀 Setup & Usage

**1. Clone the Repo**
```bash
git clone [https://github.com/yourusername/scraper-bot-codes.git](https://github.com/yourusername/scraper-bot-codes.git)
cd scraper-bot-codes

pip install -r requirements.txt

# Example: Running the price tracker
python scripts/price_tracker.py --target "BTC-USD" --interval 60


⚙️ Configuration
Create a .env file to store your credentials and proxy keys:

PROXY_URL=[http://user:pass@proxy-provider.com:8080](http://user:pass@proxy-provider.com:8080)
API_KEY=your_2captcha_key
DB_URL=postgres://user:pass@localhost:5432/scraped_data
