from cx_Freeze import setup, Executable
import sys, os

os.environ['TCL_LIBRARY'] = "C:\\Python\\Python36-32\\tcl\\tcl8.6"
os.environ['TK_LIBRARY'] = "C:\\Python\\Python36-32\\tcl\\tk8.6"

base=None

packages = [
    "idna",
    "telebot",
    "feedparser",
    "urllib3",
    "tweepy",
    "requests",
    "sqlite3",
    ]

include_files = [
    "bot/bot_exceptions.py",
    "bot/telegrambot.py",
    "database/ParserDB.db",
    "config.py",
    "rsslinks.txt"
    ]

target = Executable(
    script='parser.py',
    base=base,
    icon="icon.ico"
    )

options = {
    "packages":packages,
    "include_files":include_files
    }

#-------------------------------------------------------------------------

if sys.platform=='win32':
    base= None

setup(
    name = 'FeedParser',
    version = '0.1.1',
    author = "Dontsov Stas",
    description = "Parser for collecting RSS and Twitter feeds",
    options = {"build_exe": options},
    executables = [target]
    )
