from configparser import ConfigParser
from argparse import ArgumentParser

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler
from scraper import open_shelves, close_shelves


def main(config_file, restart):
    cparser = ConfigParser()
    cparser.read(config_file)           # read config file
    config = Config(cparser)            # make Config object out of config file (look at config.py under utils)
    config.cache_server = get_cache_server(config, restart)
    open_shelves()
    crawler = Crawler(config, restart)
    crawler.start()
    close_shelves()

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
