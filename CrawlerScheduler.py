__author__ = 'Ali'

import pysolr
import configparser
import os
import schedule
import time
import logging
from logging.handlers import RotatingFileHandler
from InstagramCrawler import InstagramCrawler


class CrawlerScheduler(object):
    def __init__(self, SettingFilePath='', Config=None):
        self.script_path = os.path.abspath(__file__)  # return path of file
        root_path = self.script_path.replace('CrawlerScheduler.py', '')  # replace 'CrawlerScheduler.py' with ''

        # create a new instance of configparser if it didn't exist
        if Config is None:
            self.__Config = configparser.ConfigParser()
        else:
            self.__Config = Config


        if SettingFilePath is '':
            file_path = self.script_path.replace('CrawlerScheduler.py', 'preprocess/config.ini')
            self.__Config.read(file_path)
        else:
            self.__Config.read(SettingFilePath)

        section = self.__Config.sections()[0]
        options = self.__Config.options(section)
        self.__LogFilePath = self.__Config.get(section, options[10])

        log_formatter = logging.Formatter('[%(asctime)s] %(filename)s %(lineno)d %(levelname)s %(name)s %(message)s')
        handler = RotatingFileHandler(root_path + self.__LogFilePath, mode='a', maxBytes=2 * 1024 * 1024, backupCount=2)
        handler.setFormatter(log_formatter)
        handler.setLevel(logging.INFO)
        self.logger = logging.getLogger('root')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(handler)

        self.scheduleDelayTime = self.__Config.getint(section, options[0])
        self.SolrPostsIndex = self.__Config.get(section, options[1])
        self.SolrUsersInfoIndex = self.__Config.get(section, options[2])
        self.SolrUsersHistory = self.__Config.get(section, options[3])
        self.SolrCommentsText = self.__Config.get(section, options[4])
        self.__MaxPage = self.__Config.get(section, options[5])
        self.CrawlerType = self.__Config.get(section, options[6])
        self.InputType = self.__Config.get(section, options[7])
        self.__EntityFilePath = self.__Config.get(section, options[8])
        self.SessionID = self.__Config.get(section, options[9])
        self.AllEntity = []
        self.__Language = self.__Config.get(section, options[11])
        self.UsersHistory = self.__Config.get(section, options[12])
        self.PostsComments = self.__Config.get(section, options[13])
        self.Client = InstagramCrawler(self.logger, SolrUsersInfoIndex=self.SolrUsersInfoIndex,
                                       SolrPostsIndex=self.SolrPostsIndex, MaxPage=self.__MaxPage,
                                       SolrUsersHistory=self.SolrUsersHistory, SolrCommentsText=self.SolrCommentsText)
        if self.SessionID is not '':
            self.Client.Client.session.cookies.set('sessionid', self.SessionID)


    def ScheduleAllEntities(self, **kwargs):
        try:
            logging.info('Start Scheduler...')

            if self.CrawlerType == 'user':
                self.logger.info('Scheduler Type = Users Crawler')
                self.logger.info('Crawler Repeat Period (min) = {}'.format(self.scheduleDelayTime))
                # run UsersCrawler normally based on UserDelayTime
                schedule.every(int(self.scheduleDelayTime)).minutes.do(self.UsersCrawler)

            if self.CrawlerType == 'hashtag':
                self.logger.info('Scheduler Type = {0} Crawler'.format(self.__EntityFilePath))
                self.logger.info('Crawler Repeat Period (min) = {}'.format(self.scheduleDelayTime))
                # run HashtagsCrawler based on HashtagDelayTime
                schedule.every(int(self.scheduleDelayTime)).minutes.do(self.HashtagsCrawler)

            if self.CrawlerType == 'follower':
                self.logger.info('Scheduler Type = Followers Crawler')
                self.logger.info('Crawler Repeat Period (min) = {}'.format(self.scheduleDelayTime))
                schedule.every(int(self.scheduleDelayTime)).minutes.do(self.FollowsCrawler, 'follower')

            if self.CrawlerType == 'following':
                self.logger.info('Scheduler Type = Followings Crawler')
                self.logger.info('Crawler Repeat Period (min) = {}'.format(self.scheduleDelayTime))
                schedule.every(int(self.scheduleDelayTime)).minutes.do(self.FollowsCrawler, 'following')

            while True:
                schedule.run_pending()
                time.sleep(1)
        except Exception as err:
            self.logger.error("Scheduler Exception")
            self.logger.error("Casuse: %s" % err)


    def UsersCrawler(self):
        try:
            # check type of input and get entities based on type (solr/file)
            if self.InputType == 'solr':
                self.__getSolrAllEntities(self.SolrUsersInfoIndex, 'user_name', 1000)
            elif self.InputType == 'file':
                self.__getFileAllEntities(self.__EntityFilePath)
            print(self.AllEntity.__len__())
            self.Client.getUsersAllPosts(self.AllEntity, False,  self.__str_to_bool(self.PostsComments), self.__str_to_bool(self.UsersHistory))
        except Exception as err:
            self.logger.error("Users Crawler Exception")
            self.logger.error("Casuse: %s" % err)


    def HashtagsCrawler(self):
        try:
            if self.InputType == 'solr':
                self.__getSolrAllEntities(self.SolrPostsIndex, 'hashtags', 60)
            elif self.InputType == 'file':
                self.__getFileAllEntities(self.__EntityFilePath)
            self.Client.getHashtagsAllPosts(self.AllEntity, True, self.__str_to_bool(self.PostsComments), self.__str_to_bool(self.UsersHistory))
        except Exception as err:
            self.logger.error("Hashtags Crawler Exception")
            self.logger.error("Casuse: %s" % err)


    def FollowsCrawler(self, type):
        try:
            if self.InputType == 'solr':
                self.__getSolrAllEntities(self.SolrUsersInfoIndex, 'user_id', 10)
            elif self.InputType == 'file':
                self.__getFileAllEntities(self.__EntityFilePath)
            self.Client.getUsersAllFollow(type, self.AllEntity, True)
        except Exception as err:
            self.logger.error("Followa Crawler Exception")
            self.logger.error("Casuse: %s" % err)


    def __getSolrAllEntities(self, SolrIndex, field, rows):
        solr = pysolr.Solr(SolrIndex, timeout=10)
        result = solr.search('+framework:instagram', fq='active_in_crawler:true', fl=field, rows=rows)
        for doc in result.docs:
            self.AllEntity.append(doc[field])


    def __getFileAllEntities(self, path):
        try:
            lines = open(path, 'r', encoding='utf8').readlines()
            for line in lines:
                self.AllEntity.append(line.strip())
            self.logger.info('All Users Size = {}'.format(len(self.AllEntity)))
        except Exception as err:
            self.logger.error("Get File All Entities Exception")
            self.logger.error("Casuse: %s" % err)


    def __str_to_bool(self, str):
        if str == 'True':
            return True
        elif str == 'False':
            return False