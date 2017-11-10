# coding: utf-8

# TODO
# в твиттере - не отправлять ретвиты, которые ссылаются на сообщения пользователей на которых мы подписаны
# получается мы дважды одно и то же выводим, или нет? надо проверить
#
# TODO
# можно обернуть блок с попытками соединения, отправки сообщений в одну функцию аля retry(...)
# https://stackoverflow.com/questions/4606919/in-python-try-until-no-error
#
# TODO
# можно обернуть вывод текста, всегда к нему приписывать время, а не выводить как сейчас через принт
#
# TODO
# профильтровывать статьи на предмет индикаторов, индикаторы проверять в базах данных, если они свежие, отправлять в qRadar
#
# TODO
# профильтровывать статьи на предмет содержания ключевых слов, проверять слова, если они там есть, то выводить куда-то на экран, куда?
#
# TODO
# ограничить поле дескрипшн для выгрузки в бд, там слишком много данных! (https://www.us-cert.gov/ncas/alerts.xml)

import json
import multiprocessing
import re
import sqlite3
import time
import feedparser
import telebot
import requests
from datetime import datetime
from urllib.parse import urljoin
from urllib.parse import urlsplit
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

import config
from bot import bot_exceptions, telegrambot


def DBExecute(sql_query, query_data):

    DBConnection = sqlite3.connect(config.SQLDatabase)
    DBcursor = DBConnection.cursor()

    if query_data:

        # data = (query_data.encode('utf8'),)
        # DBcursor.execute( sql_query, data )

        # TODO
        if not(type(query_data) == tuple):
            query_data = (query_data,)

        DBcursor.execute(sql_query, query_data)

    else:

        DBcursor.execute(sql_query)

    sql_answer = DBcursor.fetchone()
    DBConnection.commit()
    DBcursor.close()

    return sql_answer


class TwitterProcess:

    # This is a basic listener that just prints received tweets to stdout.
    class TwitterStdOutListener(StreamListener):

        @staticmethod
        def data_filter(tweetMsg):

            if 'friends' in tweetMsg:
                return True

            if 'event' in tweetMsg:
                return True

            if 'user' not in tweetMsg:
                return True

            else:

                return False

        @staticmethod
        def get_tweet(tweetMsg):

            def unique_url(url_to_check, twitter_link):
                if url_to_check == twitter_link:
                    return False
                else:
                    return True

            def remove_url(text):
                result = re.sub(r"http\S+", "", text)
                return result

            def unshorten_url(url):
                return requests.head(url, allow_redirects=True).url

            tweet = {
                'add_time': None,
                'pub_time': None,
                'text': None,
                'hashes': None,
                'user_name': None,
                'retweet_user_name': None,
                'link': None,
                'sources': None
            }

            tweet['add_time'] = datetime.now()
            tweet['pub_time'] = tweetMsg['created_at']
            tweet['user_name'] = tweetMsg['user']['name']
            tweet['text'] = remove_url(tweetMsg['text'])

            if 'id' in tweetMsg:
                # пример ->> https://twitter.com/HackAlertNews/status/900443914670411777
                tweet['link'] = 'https://twitter.com/i/web/status/' + str(tweetMsg['id'])

            # if tweetMsg.has_key('retweeted_status'):
            #     tweet['retweet_user_name'] = tweetMsg['retweeted_status']['user']['name']

            if 'retweeted_status' in tweetMsg:
                tweet['retweet_user_name'] = tweetMsg['retweeted_status']['user']['name']

            if 'entities' in tweetMsg:

                if 'hashtags' in tweetMsg['entities']:
                    if len(tweetMsg['entities']['hashtags']) >= 1:

                        tweet['hashes'] = []
                        for hashtag in tweetMsg['entities']['hashtags']:
                            # добавить в список хэштеги
                            tweet['hashes'].append(hashtag['text'].upper())

                if 'urls' in tweetMsg['entities']:
                    if len(tweetMsg['entities']['urls']) >= 1:

                        tweet['sources'] = []
                        for url in tweetMsg['entities']['urls']:
                            expanded_url = url['expanded_url']
                            # проверить ссылку - она внешняя или нет
                            if unique_url(expanded_url, tweet['link']):
                                # разрешить внешнюю ссылку из укороченной
                                expanded_url = unshorten_url(expanded_url)
                                # добавить внешнюю ссылку в список ссылок
                                tweet['sources'].append(expanded_url)
                            else:
                                break

            if 'extended_tweet' in tweetMsg:
                if 'urls' in tweetMsg['extended_tweet']:
                    if len(tweetMsg['extended_tweet']['urls']) >= 1:

                        tweet['sources'] = []
                        for source in tweetMsg['extended_tweet']['urls']:
                            # добавить в список хэштеги
                            tweet['sources'].append(source['expanded_url'])

            return tweet

        @staticmethod
        def check_data_match(tweetMsg):

            # как-то тут оно короче сравнивает, хз как
            # по идее надо рассмотреть три варианта: это копипаст, это дополнение, это новый контент
            # расставлены заглушки:

            # это новый контент - добавлять в БД
            if True:
                return tweetMsg

            # это дополнение - добавлять в БД
            if True:
                return tweetMsg + 'addedInfo'

            # это копипаст - не добавлять в БД
            if True:
                return False

        @staticmethod
        def db_write_tweet(tweetMsg):

            TweetQueries = {
                'insertTweet':          "INSERT into feeds values (Null, ?, ?, ?, ?, ?);",
                'insertTweetHash':      "INSERT into feed_to_hash values ( ?, ?);",
                'insertTweetSource':    "INSERT into feed_to_source values ( ?, ?, ?);",
                'insertSource':         "INSERT into sources values (Null, ?, ?);",
                'insertHash':           "INSERT into hashes values (Null, ?);",
                'selectSourceId':       "SELECT source_id FROM sources WHERE source_link=?",
                'selectHashId':         "SELECT hash_id FROM hashes WHERE hash_text=?",
                'selectSourceLast':     "SELECT source_id FROM sources ORDER BY source_id DESC LIMIT 1;",
                'selectFeedLast':       "SELECT feed_id, link_to_feed FROM feeds ORDER BY feed_id DESC LIMIT 1;",
                'selectHashLast':       "SELECT hash_id FROM hashes ORDER BY hash_id DESC LIMIT 1;",

            }

            hashes_id = sources_id  = None

            def check_hash(hash_list):

                # for row in con.execute("select rowid, name, ingredients from recipe where name match 'pie'"):
                #     print row
                #
                # cur.execute("select * from people where name_last=:who and age=:age", {"who": who, "age": age})
                #
                # print cur.fetchone()
                # TweetQueries['']

                temp_list = []

                # вытащить все хэши
                # проверить каждый хэш из списка
                # записать хэш/id в список
                # список передать дальше

                for hash in hash_list:

                    sql_answer = DBExecute(TweetQueries['selectHashId'], hash)

                    if not sql_answer:

                        DBExecute(TweetQueries['insertHash'], hash)

                        new_hash_id = DBExecute(TweetQueries['selectHashLast'], None)

                        temp_list.append(new_hash_id[0])

                    else:

                        temp_list.append(sql_answer[0])

                return temp_list

            def check_source(source_list):

                def get_link(bare_link):

                    clear_link = urljoin(bare_link, '/')

                    return clear_link

                temp_list = []

                # вытащить все хэши
                # проверить каждый хэш из списка
                # записать хэш/id в список
                # список передать дальше

                for source in source_list:

                    source_link = get_link(source)

                    sql_answer = DBExecute(TweetQueries['selectSourceId'], source_link)

                    if not sql_answer:

                        query_data = (source_link, 'tweet')

                        DBExecute(TweetQueries['insertSource'], query_data)

                        new_source_link = DBExecute(TweetQueries['selectSourceLast'], None)

                        temp_list.append([new_source_link[0], source])

                    else:

                        temp_list.append([sql_answer[0], source])

                return temp_list

            # хэштеги
            # найти или создать id хэштега,
            if tweetMsg['hashes']:
                hashes_id = check_hash(tweetMsg['hashes'])

            # источники
            # найти или создать id источника,
            if tweetMsg['sources']:
                sources_id = check_source(tweetMsg['sources'])

            # записать в таблицу твит
            tweet_data = ( tweetMsg['add_time'], tweetMsg['pub_time'], tweetMsg['text'], tweetMsg['link'], 'tweet')
            DBExecute(TweetQueries['insertTweet'], tweet_data)
            tweet_id = DBExecute(TweetQueries['selectFeedLast'], None)

            # связать твит и хэштеги
            if hashes_id:
                for hash_id in hashes_id:
                    hash_data = (tweet_id[0], hash_id)
                    DBExecute(TweetQueries['insertTweetHash'], hash_data)
            else:
                hash_data = (tweet_id[0], None)
                DBExecute(TweetQueries['insertTweetHash'], hash_data)

            # связать твит и источники
            if sources_id:
                for source_id in sources_id:
                    source_data = (tweet_id[0], source_id[0], source_id[1])
                    DBExecute(TweetQueries['insertTweetSource'], source_data)
            else:
                source_data = (tweet_id[0], None, tweet_id[1])
                DBExecute(TweetQueries['insertTweetSource'], source_data)

            return True

        def on_data(self, jsonDataString):

            data = json.loads(jsonDataString)

            # надо профильтровать данные от твиттера, чтобы приходили нужные данные
            # отбросить левак
            if self.data_filter(data): return True

            # левак отброшен, можно обрабатывать данные
            tweet = self.get_tweet(data)

            # tweet = {
            #     'add_time': None,
            #     'pub_time': None,
            #     'text': None,
            #     'hashes': None,
            #     'user_name': None,
            #     'retweet_user_name': None
            #     'link': None
            #     'sources': None
            # }

            print(datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + tweet['user_name'] + ':' + tweet['text'] + "\n=====")

            tweet_filtered = {
                'add_time':             tweet['add_time'],
                'pub_time':             tweet['pub_time'],
                'text':                 telegrambot.escape_markdown(tweet['text']),
                'hashes':               tweet['hashes'],
                'user_name':            tweet['user_name'],
                'retweet_user_name':    tweet['retweet_user_name'],
                'link':                 tweet['link'],
                'sources':              tweet['sources'],
            }


            news_text = '[' + tweet_filtered['user_name'] + ']' + '(' + tweet_filtered['link'] + ')' + ': ' + "\n" \
                        + tweet_filtered['text'] + "\n\n"

            if tweet_filtered['sources']:
                news_text += "Links: \n"
                for source in tweet_filtered['sources']:
                    news_text += '[' + urljoin(source, '/') + ']' + '(' + source + ')' + "\n"

            for x in range(1, 6):

                try:
                    telegrambot.bot.send_message(config.telebot_id,
                                                 news_text,
                                                 parse_mode="Markdown",
                                                 disable_web_page_preview="True")

                except telebot.apihelper.ApiException:
                    print("Telegram. Исключение. Плохой запрос")
                    print(datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') +
                          "Telegram. Безуспешно. Сообщение не отправлено twitter\n"
                          + news_text)
                    break

                except Exception:
                    print(datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') +
                          "Telegram. Исключение. Неизвестное исключение")
                    print(datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') +
                          "Telegram. Безуспешно. Повторная попытка", x)
                    time.sleep(2)  # wait for 2 seconds before trying to fetch the data again
                    continue

                break

            # здесь так же перед загрузкой в бд надо проверить новость на уникальность
            # как? пока хз в любом случае если совпадений нет, то надо подгрузить информацию
            tweet_filtered = self.check_data_match(tweet_filtered)
            if not tweet_filtered:
                return True

            # возможно здесь стоит использовать хранимые процедуры
            # получая задачу на выполнение процедуры мы просто будем передавать в функцию данные
            # и она сама по поданым данным будет исполнять что ей надо - применять хранимую процедуру
            # рассматриваем загрузку твитов, загрузку новостей, выгрузку твитов, выгрузку новостей -  4 процедуры
            self.db_write_tweet(tweet_filtered)

            # затем из базы данных? выгрузить данные в телеграм
            # возможно это стоит делать не тут, а вешать листнер на БД
            # - когда пришла запись нам так и так надо ее отправить в телеграм, возможно предобработав информацию

            return True

        def on_error(self, status_code):

            if status_code == 420:
                # returning False in on_data disconnects the stream
                raise bot_exceptions.Twitter420Exception()

    def __init__(self):

        telegrambot.bot.send_message(config.telebot_id, 'TwitterProcess Started...')

        auth = OAuthHandler(config.consumer_key,
                            config.consumer_secret)

        auth.set_access_token(config.access_token,
                              config.access_token_secret)

        stream = Stream(auth, self.TwitterStdOutListener())

        # This line to capture data from twitter user wall - messages from followed people
        while True:
            try:
                stream.userstream(_with=['followings'])

            except bot_exceptions.Twitter420Exception:
                print(datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + 'Twitter. Исключение. Error 420 / Twitter Rate Limited. ')

            except requests.ConnectionError:
                print(datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + 'Twitter. Исключение. Ошибка HTTP соединения.')

            else:
                break
            finally:
                i = 5
                while i:
                    print(datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + 'Twitter. Безуспешно. Повторная попытка соединения через ...', i)
                    i -= 1
                    time.sleep(1)

        # This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
        # stream.filter(track=['python', 'javascript', 'ruby'])


class RssProcess:


    @staticmethod
    def db_write_feed(feed):

        FeedQueries = {
            'insertFeed':       "INSERT into feeds values (Null, ?, ?, ?, ?, ?);",
            'insertFeedSource': "INSERT into feed_to_source values ( ?, ?, ?);",
            'insertSource':     "INSERT into sources values (Null, ?, ?);",
            'selectSourceId':   "SELECT source_id FROM sources WHERE source_link=?",
            'selectSourceLast': "SELECT source_id FROM sources ORDER BY source_id DESC LIMIT 1;",
            'selectFeedLast':   "SELECT feed_id, link_to_feed FROM feeds ORDER BY feed_id DESC LIMIT 1;",
            'selectFeedWhereText':   "SELECT text FROM feeds WHERE text LIKE ?"

        }

        # And this is the named style:
        # cur.execute("select * from people where name_last=:who and age=:age", {"who": who, "age": age})

        def check_feed( feed_title):

            feed_title += "%"

            if DBExecute(FeedQueries['selectFeedWhereText'], feed_title):
                return True
            else:
                return False

        def check_source(feed_link):

            def get_link(bare_link):

                clear_link = urljoin(bare_link, '/')

                return clear_link

            temp_list = []

            # вытащить все хэши
            # проверить каждый хэш из списка
            # записать хэш/id в список
            # список передать дальше

            source_link = get_link(feed_link)

            sql_answer = DBExecute(FeedQueries['selectSourceId'], source_link)

            if not sql_answer:

                query_data = (source_link, 'feed')
                DBExecute(FeedQueries['insertSource'], query_data)
                new_source_link = DBExecute(FeedQueries['selectSourceLast'], None)

                # list.append([new_source_link[0], feed_link])
                temp_list.append(new_source_link[0])
                temp_list.append(feed_link)

            else:

                temp_list.append(sql_answer[0])
                temp_list.append(feed_link)

            return temp_list

        source_id = None

        # message.append((news['title'], news['summary'], news['link']))

        if check_feed(feed['title']):
            return False

        if feed['link']:
            source_id = check_source(feed['link'])

        # записать в таблицу фид
        feed_data = (feed['add_time'], feed['pub_time'], feed['title'] + '. ' + feed['summary'], feed['link'], 'feed')
        DBExecute(FeedQueries['insertFeed'], feed_data)
        # TODO исправить ошибку - ид может быть в случае конкуренции разный!
        feed_id = DBExecute(FeedQueries['selectFeedLast'], None)

        # связать твит и источники
        source_data = (feed_id[0], source_id[0], source_id[1])
        DBExecute(FeedQueries['insertFeedSource'], source_data)

        return True

    def __init__(self, feedlistFile):

        telegrambot.bot.send_message(config.telebot_id, 'RssProcess Started...')

        def get_top_domain(url):

            def get_domain(url):
                u = urlsplit(url)
                return u.netloc

            domain = get_domain(url)
            domain_parts = domain.split('.')
            if len(domain_parts) < 2:
                return domain
            top_domain_parts = 2
            # if a domain's last part is 2 letter long, it must be country name
            if len(domain_parts[-1]) == 2:
                if domain_parts[-1] in ['uk', 'jp']:
                    if domain_parts[-2] in ['co', 'ac', 'me', 'gov', 'org', 'net']:
                        top_domain_parts = 3
                else:
                    if domain_parts[-2] in ['com', 'org', 'net', 'edu', 'gov']:
                        top_domain_parts = 3
            return '.'.join(domain_parts[-top_domain_parts:])

        def get_filtered_data(feedlistFile):

            file = open(feedlistFile, 'r')
            feedlist = file.readlines()
            file.close()

            i = 0
            for source in feedlist:
                # удалили перенос строки
                feedlist[i] = re.sub('\n', '', feedlist[i])
                # удалили пробелы
                feedlist[i] = re.sub(' ', '', feedlist[i])
                # удалили пустые строки
                if not feedlist[i]:
                    feedlist.remove(feedlist[i])
                else:
                    i += 1

            file.close()

            return feedlist

        wait_time = 20

        while True:

            # чтобы можно было добавлять в файл новые ссылки, на каждем круге открываем его снова
            feedlist = get_filtered_data(feedlistFile)

            for source in feedlist:

                print(datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + 'recheck ' + source[:25] + '...')

                this_news_new = True
                data = feedparser.parse(source)

                for news in data['entries']:

                    if 'summary' in news:
                        None
                    else:
                        news['summary'] = ''

                    if 'published' in news:
                        None
                    else:
                        news['published'] = ''

                    news_filtered = {
                        'domain':       get_top_domain(source),
                        'pub_time':     news['published'],
                        'add_time':     datetime.now(),
                        'source':       telegrambot.escape_markdown(urljoin(news['link'], '/')),
                        'link':         news['link'],
                        # здесь не нужно проверять все, есть знаки жирного текста **
                        'title':        telegrambot.escape_markdown(re.sub(r'<[^>]*>', '', news['title']), 'title'),
                        'summary':      ' '.join(telegrambot.escape_markdown(re.sub(r'<[^>]*>', '', news['summary'])).split())[:250] + '...'
                    }

                    # TODO Отделить проверку фида, от его непосредственной записи
                    # TODO много накладных рассчетов получается при создании переменной news_filtered
                    this_news_new = self.db_write_feed(news_filtered)

                    if not this_news_new:

                        # подобное сообщение уже было, не надо проверять следующие за ним
                        # not check it and next in feed, it also was
                        continue

                    else:

                        # нет такого сообщения и по этому его уже записала фукция db_write_feed в базу данных
                        # + будут проверены следующие сообщения
                        # + надо написать сообщение в телеграмм

                        print(datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') +  news_filtered['title'] + ' ' + news_filtered['summary'] + "\n======")

                        news_text = '*' + news_filtered['title'] + ".*\n" \
                                    + news_filtered['summary'] + "\n\n" \
                                    + "Links:\n" \
                                    + '[' + news_filtered['source'] + '...' + ']' + '(' + news_filtered['link'] + ')'

                        for x in range(1, 6):

                            try:
                                telegrambot.bot.send_message(config.telebot_id,
                                                             news_text,
                                                             parse_mode="Markdown",
                                                             disable_web_page_preview="True")

                            except telebot.apihelper.ApiException:
                                print("Telegram. Исключение. Плохой запрос")
                                print(datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') +
                                      "Telegram. Безуспешно. Сообщение не отправлено rss\n"
                                      + news_text)
                                break

                            except Exception:
                                print(datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') +
                                      "Telegram. Исключение. Неизвестное исключение")
                                print(datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') +
                                      "Telegram. Безуспешно. Повторная попытка", x)
                                time.sleep(2)  # wait for 2 seconds before trying to fetch the data again
                                continue

                            break

                time.sleep(wait_time)

    #
    # тут надо взять каждое сообщение из рсс, и для каждого сообщения начиная с самого свежего
    # - проверить есть ли оно уже в базе данных
    # - если оно есть в базе, то остановиться и дальше не проверять (проверять по титлам)
    # - если оного в базе нету, то надо добавить оно в базу и взять следующий
    #
    #


    # авыа


if __name__ == '__main__':
    # multiprocessing.freeze_support()

    RssProcess('rsslinks.txt')
    # TwitterProcess()


    # нужно делать процессы, работающие параллельно
    # - первый слушает твиттер, пишет изменения в БД
    # - второй по циклу обновляет rss ленту, пишет изменения в БД
    # - третий отправляет сообщения в телеграм

    # здесь идет обработка твиттера
    # twitterProc = multiprocessing.Process(target=TwitterProcess)
    # twitterProc.start()

    # здесь идет обработка rss лент
    # RssProc = multiprocessing.Process(target=RssProcess, args=('rsslinks.txt',))
    # RssProc.start()
    #
    # twitterProc.join()
    # RssProc.join()
