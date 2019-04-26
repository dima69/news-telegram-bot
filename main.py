import feedparser
import psycopg2
import logging
import configparser
from psycopg2 import extras
from telegram.ext import Updater, CommandHandler

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        self.conn = psycopg2.connect(dbname='tg_db', user='postgres', password='qqq')
        self.cur = self.conn.cursor()

    def insert_rss_url(self, record):
        self.cur.execute('INSERT INTO rss_urls (url) VALUES (%s) ON CONFLICT DO NOTHING', (record,))
        self.conn.commit()
        return

    def insert_news(self, news_list, rss_url):
        prepared_sql = "INSERT INTO news (url, rss_url) VALUES %s ON CONFLICT DO NOTHING RETURNING url"
        prepared_news_list = [(x, rss_url) for x in news_list]
        aaa = extras.execute_values(self.cur, prepared_sql, prepared_news_list, fetch=True)
        self.conn.commit()
        return aaa

    def load_rss_urls(self):
        self.cur.execute('SELECT url FROM rss_urls')
        all_urls = self.cur.fetchall()
        return all_urls


class NewsParser:
    def __init__(self):
        self.db_worker = DatabaseManager()
        return

    def check_news(self):
        logger.info("Checking news..")
        news_list = []
        checker = []
        rss_urls = self.db_worker.load_rss_urls()
        for url in rss_urls:
            feed = feedparser.parse(url[0])
            for post in feed.entries:
                news_list.append(post.link)
                # news_list.append({'title': post.link, 'link': post.link}) !!!!!!!!
            checker = self.db_worker.insert_news(news_list, url)
            if checker:
                logger.info('We have news %s!', checker)
            else:
                logger.info('No news there..')
        return checker

    def get_current_news(self, rss_url):
        news_list = []
        current_feed = feedparser.parse(rss_url)
        for post in current_feed.entries:
            news_list.append(post.link)
        if news_list:
            self.db_worker.insert_news(news_list, rss_url)
        logger.info('Current news_list: "%s"', news_list)
        return


class TelegramBot:
    def __init__(self):
        proxy_tg = {
            'proxy_url': 'socks5://163.172.152.192:1080',
        }

        config = configparser.ConfigParser()
        config.read("config.ini")
        token = config['DEFAULT']['Token']

        self.database_worker = DatabaseManager()
        self.newsparser_worker = NewsParser()
        self.__updater_worker = Updater(token, request_kwargs=proxy_tg)

        self.__dispatcher = self.__updater_worker.dispatcher
        self.__job_worker = self.__updater_worker.job_queue

        self.__dispatcher.add_handler(CommandHandler("add", self.__add_rss, pass_args=True))
        self.__dispatcher.add_handler(CommandHandler("parse", self.__parse_now))
        self.__dispatcher.add_handler(CommandHandler("check", self.__check_news))
        self.__dispatcher.add_handler(CommandHandler("startx", self.__start_job))
        self.__dispatcher.add_handler(CommandHandler("stopx", self.__stop_job))
        self.__dispatcher.add_error_handler(self.__error)

        self.__updater_worker.start_polling()
        self.__start_job()
        self.__updater_worker.idle()

    def __add_rss(self, bot, update, args):
        if args:
            logger.info('New RSS here! "%s"', args[0])
            self.database_worker.insert_rss_url(args[0])
            self.newsparser_worker.get_current_news(args[0])
        else:
            update.message.reply_text('Usage: /add rss_url')
        return

    def __parse_now(self, bot, update):
        total = self.newsparser_worker.check_news()
        msg = f'Total news parsed: {len(total)}'
        print(msg)
        bot.send_message(chat_id='772208009', text=msg, disable_notification=True)
        return

    def __check_news(self, bot, update):
        new_news_list = self.newsparser_worker.check_news()
        if new_news_list:
            logger.info('Trying to send: "%s" amount of news', len(new_news_list))
            for item in new_news_list:
                bot.send_message(chat_id='772208009', text=item[0], disable_notification=True)
        else:
            logger.info('Nothing to send')
        return

    def __start_job(self):
        logger.info("Job start! Next check in 60 sec...")
        self.checker_job = self.__job_worker.run_repeating(self.__check_news, interval=600, first=60)
        return

    def __stop_job(self, bot, update):
        self.checker_job.enabled = False
        return

    def __error(self, bot, update, error):
        logger.warning('Update "%s" caused error "%s"', update, error)
        return


if __name__ == '__main__':
    TelegramBot()
