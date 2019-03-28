import feedparser
import psycopg2
import logging
from telegram.ext import Updater, CommandHandler

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:

    def __init__(self):
        self.conn = psycopg2.connect(dbname='tg_db', user='postgres', password='qqq')
        self.cur = self.conn.cursor()

    def insert_rss_url(self, record):
        self.cur.execute('INSERT INTO "rssUrls" VALUES (%s) ON CONFLICT DO NOTHING', (record,))
        self.conn.commit()
        return

    def insert_news(self, news_list):
        fetch = []
        for item in news_list:
            self.cur.execute('INSERT INTO newslist(url) VALUES (%s) ON CONFLICT DO NOTHING RETURNING url', (item,))
            new = self.cur.fetchone()
            if new is not None:
                fetch.append(new[0])
            self.conn.commit()
        print('insert_news: news: ', fetch)
        return fetch

    def load_rss_urls(self):
        self.cur.execute('SELECT * FROM "rssUrls"')
        all_urls = self.cur.fetchall()
        return all_urls


class NewsParser:

    def __init__(self):
        self.db_worker = DatabaseManager()
        return

    def check_news(self):
        logger.info("NewsParser.check_news: ")
        news_list = []
        rss_urls = self.db_worker.load_rss_urls()
        for url in rss_urls:
            feed = feedparser.parse(url[0])
            for post in feed.entries:
                news_list.append(post.link)
            checker = self.db_worker.insert_news(news_list)
            if checker:
                print("отправить в тг:", checker)
        return

    def get_current_news(self, rss_url):
        news_list = []
        current_feed = feedparser.parse(rss_url)
        for post in current_feed.entries:
            news_list.append(post.link)
        print(news_list)
        return news_list


class TelegramBot:
    def __init__(self):
        REQUEST_KWARGS = {
            'proxy_url': 'socks5://163.172.152.192:1080',
        }

        self.database_worker = DatabaseManager()
        self.newsparser_worker = NewsParser()

        self.__updater_worker = Updater("731319505:AAEU1_KWlT_eXxL0BNGMvngwZhA7mkGT8Go", request_kwargs=REQUEST_KWARGS)
        self.__dispatcher = self.__updater_worker.dispatcher

        self.__dispatcher.add_handler(CommandHandler("add", self.__add_rss, pass_args=True))
        self.__dispatcher.add_handler(CommandHandler("check", self.__check_now))
        self.__dispatcher.add_error_handler(self.__error)

        self.__updater_worker.start_polling()
        self.__updater_worker.idle()

    def __add_rss(self, bot, update, args):
        if args:
            self.database_worker.insert_rss_url(args[0])
        else:
            update.message.reply_text("IDI NAHUY")
        return

    def __check_now(self, bot, update):
        print('checking now...')

    def __error(self, bot, update, error):
        logger.warning('Update "%s" caused error "%s"', update, error)


if __name__ == '__main__':
    TelegramBot()
