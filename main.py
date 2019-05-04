import configparser
import logging
import feedparser
import psycopg2
from psycopg2 import extras
from telegram.ext import Updater, CommandHandler

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

PROXY_TG = {'proxy_url': 'socks5://163.172.152.192:1080'}
USERID = '772208009'


class DatabaseManager:
    def __init__(self):
        self.conn = psycopg2.connect(dbname='tg_db', user='postgres', password='qqq')
        self.cur = self.conn.cursor()

    def add_rss_url(self, rss_url):
        self.cur.execute('INSERT INTO rss_urls (rss_url) VALUES (%s) ON CONFLICT DO NOTHING', (rss_url,))
        self.conn.commit()

    def insert_news(self, news_list):
        prepared_sql = 'INSERT INTO news_feed (title, news_url, publish_time, rss_url) VALUES %s ON CONFLICT DO NOTHING RETURNING title, news_url'
        # if date < current_date:
        inserted_records = extras.execute_values(self.cur,
                                                 prepared_sql,
                                                 news_list,
                                                 template='(%(title)s, %(link)s, %(published_time)s, %(rss_id)s)',
                                                 fetch=True)
        self.conn.commit()
        return inserted_records

    def load_rss_urls(self):
        self.cur.execute('SELECT rss_url FROM rss_urls')
        all_urls = self.cur.fetchall()
        return all_urls


class NewsParser:
    def __init__(self):
        self.db_worker = DatabaseManager()

    def check_news(self):
        logger.info("def check_news(): Checking news..")
        news_list = []
        checker = []
        rss_urls = self.db_worker.load_rss_urls()
        for url in rss_urls:
            feed = feedparser.parse(url[0])
            self.db_worker.cur.execute('SELECT rss_id FROM rss_urls WHERE rss_url = (%s)', (url[0],))
            rss_id = self.db_worker.cur.fetchone()[0]
            for post in feed.entries:
                news_list.append({'title': post.title, 'link': post.link, 'published_time': post.published, 'rss_id': rss_id})
            checker.append(self.db_worker.insert_news(news_list))
            del feed
        if checker:
            return checker


    def get_current_news(self, rss_url):
        news_list = []
        current_feed = feedparser.parse(rss_url)
        self.db_worker.cur.execute('SELECT rss_id FROM rss_urls WHERE rss_url = (%s)', (rss_url,))
        rss_id = self.db_worker.cur.fetchone()[0]
        for post in current_feed.entries:
            news_list.append({'title': post.title, 'link': post.link, 'published_time': post.published, 'rss_id': rss_id})
        if news_list:
            self.db_worker.insert_news(news_list)


def add_rss(bot, update, args):
    if args:
        logger.info('New RSS here! "%s"', args[0])
        database_worker.add_rss_url(args[0])
        newsparser_worker.get_current_news(args[0])
    else:
        update.message.reply_text('Usage: /add rss_url')


def parse_now(bot, update):
    total = newsparser_worker.check_news()
    msg = f'Total news parsed: {len(total)}'
    print(f'bot.send_message: {msg}')
    #bot.send_message(chat_id=USERID, text=msg, disable_notification=True)


def get_news(bot, update):
    news_list = None or newsparser_worker.check_news()
    if news_list:
        logger.info('Trying to send: "%s" amount of news', len(news_list[0]))
        for item in news_list[0]:
            msg = f'{item[0]}\n{item[1]}'
            print(f'bot.send_message: {item[0]} {item[1]}')
            #bot.send_message(chat_id=USERID, text=item[0], disable_notification=True)
    else:
        logger.info('Nothing to send')


def job_handler(job_queue, user_data, start_job=None):
    if start_job:
        current_job = job_queue.run_repeating(get_news, interval=600, first=5)
        user_data['job'] = current_job
        print('job is starting')
    else:
        current_job = user_data['job']
        current_job.schedule_remove()
        del user_data['job']
        print('job canceled')


def error(bot, update, error):
    logger.warning('Update "%s" caused error "%s"', update, error)


def main():
    config = configparser.ConfigParser()
    config.read("config.ini")
    token = config['DEFAULT']['Token']

    updater_worker = Updater(token, request_kwargs=PROXY_TG)
    job_worker = updater_worker.job_queue
    dispatcher = updater_worker.dispatcher

    dispatcher.add_handler(CommandHandler("add", add_rss, pass_args=True))
    dispatcher.add_handler(CommandHandler("parse", parse_now, pass_user_data=True))
    dispatcher.add_handler(CommandHandler("get", get_news, pass_user_data=True, pass_job_queue=True))
    dispatcher.add_handler(CommandHandler("stopj", job_handler, pass_user_data=True, pass_job_queue=True))
    dispatcher.add_error_handler(error)

    updater_worker.start_polling()
    job_handler(job_worker, dispatcher.user_data, start_job=True)
    updater_worker.idle()


if __name__ == '__main__':
    database_worker = DatabaseManager()
    newsparser_worker = NewsParser()
    main()
