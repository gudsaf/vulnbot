# coding: utf-8

import config
import telebot

bot = telebot.TeleBot(config.telebot_token)

# bot.send_message(config.telebot_id, 'Started...')

# text = "[текст ссылки](http://example.com/url)"
# bot.send_message(config.telebot_id, text, parse_mode="Markdown")


@bot.message_handler(content_types=["text"])


# функция обработки входящих сообщений
def repeat_all_messages(message): # Название функции не играет никакой роли, в принципе
    bot.send_message(message.chat.id, message.text)


def escape_markdown(text, opts=None):

    if opts == 'title':
        text = text.replace('[', '')
        text = text.replace(']', '')
        text = text.replace('_', '')
        text = text.replace('`', '')
        text = text.replace('*', '')
    else:
        text = text.replace('[', '\[')
        text = text.replace('_', '\_')
        text = text.replace('`', '\`')
        text = text.replace('*', '\*')

    return text

if __name__ == '__main__':
     bot.polling(none_stop=True)