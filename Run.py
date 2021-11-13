#!/usr/bin/env python3
__author__ = 'Ali'

import sys
from CrawlerScheduler import CrawlerScheduler

if __name__ == '__main__':
    if len(sys.argv) > 1:
        schedule = CrawlerScheduler(SettingFilePath=sys.argv[1])
    else:
        scheduler = CrawlerScheduler()

    scheduler.ScheduleAllEntities()

