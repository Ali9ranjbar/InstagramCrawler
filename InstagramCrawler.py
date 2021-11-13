__author__ = 'Ali'

import datetime
import json
import logging
import re
import sys
import time
import random
import pysolr
from instagram_scraper.app import InstagramScraper
from instagram_scraper.constants import QUERY_MEDIA_VARS, QUERY_MEDIA, QUERY_HASHTAG, QUERY_HASHTAG_VARS, USER_INFO



class InstagramCrawler(object):
    def __init__(self, logger,
                 SolrUsersInfoIndex='http://localhost:8983/solr/insta_users',
                 SolrPostsIndex='http://localhost:8983/solr/insta_general',
                 MaxPage=10,
                 SolrUsersHistory='http://localhost:8983/solr/insta_users_history',
                 SolrCommentsText='http://localhost:8983/solr/insta_comments'):

        self.Client = InstagramScraper()
        self.Client.logger = logger
        self.logger = logger
        self.utc_format = '%Y-%m-%dT%H:%M:%SZ'
        self.__SolrUsersInfoIndex = SolrUsersInfoIndex
        self.__SolrPostsIndex = SolrPostsIndex
        self.__SolrUsersHistory = SolrUsersHistory
        self.__SolrCommentsText = SolrCommentsText
        self.__MaxPage = int(MaxPage)
        self.BASE_URL = 'https://www.instagram.com/'
        self.QUERY_POST_VARS = '{{"shortcode":"{0}","include_reel":true,"include_logged_out":false}}'
        self.QUERY_POST = 'https://www.instagram.com/graphql/query/?query_hash=6ff3f5c474a240353993056428fb851e&variables={0}'
        self.QUERY_More_Comments = 'https://www.instagram.com/graphql/query/?query_hash=97b41c52301f77ce508f55e66d17620e&variables={0}'
        self.QUERY_More_Comments_VARS = '{{"shortcode":"{0}","first":50,"after":"{1}"}}'
        self.QUERY_Following_VARS = '{{"id":"{0}","first":50,"after":"{1}"}}'
        self.QUERY_Following = 'https://www.instagram.com/graphql/query/?query_hash=d04b0a864b4b54837c0d870b0e77e076&variables={0}'
        self.QUERY_Follower = 'https://www.instagram.com/graphql/query/?query_hash=c76146de99bb02f6415203be841dd25a&variables={0}'
        self.UserPostCodeLimit = 1
        self.comments_requests = 1


    def ExtractPostCodeFields(self, post_code, user_id):
        # usr_docs = []
        user_name = ''
        try:
            resp = self.getEntityJSON(url=self.QUERY_POST, url_var=self.QUERY_POST_VARS, entity=post_code,
                                      end_cursor='')
            owner_json = resp['data']['shortcode_media']['owner']['reel']['owner']
            user_name = owner_json['username']
            user_doc = {
                'user_id': owner_json['id'],
                'user_name': user_name,
                'user_is_private': False,
                'user_profile_pic': owner_json['profile_pic_url'],
            }
            # usr_docs.append(user_doc)
            return user_doc
        except Exception as err:
            self.logger.error("Extract User Fields Exception")
            self.logger.error("Casuse: %s" % err)
        # commented
        # self.SolrCommit(usr_docs, self.__SolrUsersInfoIndex)
        return

    def ExtractUserFields(self, user_json, user_type, users_history):
        try:
            Is_private = user_json['is_private']
            user_name = user_json['username']
            external_url = ''
            if external_url in user_json:
                external_url = user_json['external_url']
            if user_type is 'user_info':
                user_id = user_json['pk']
                following_count = user_json['following_count']
                follower_count = user_json['follower_count']
                media_count = user_json['media_count']
            else:
                user_id = user_json['id']
                following_count = user_json['edge_follow']['count']
                follower_count = user_json['edge_followed_by']['count']
                media_count = user_json['edge_owner_to_timeline_media']['count']
            user_doc = {
                'user_id': user_id,
                'user_name': user_name,
                'user_full_name': suser_json['full_name'],
                'user_is_private': Is_private,
                'user_is_verified': user_json['is_verified'],
                'user_profile_pic': user_json['profile_pic_url'],
                'user_bio': user_json['biography'],
                'external_url': external_url,
                'user_following': following_count,
                'user_followers': follower_count,
                'user_posts': media_count
            }
            usr_docs = []
            usr_docs.append(user_doc)
            self.SolrCommit(usr_docs, self.__SolrUsersInfoIndex)

            if users_history:
                user_hist = {
                    'id': int(str(user_id) + str(int(datetime.datetime.now().timestamp()))),
                    'crawl_date': self.__utc_to_local(datetime.datetime.now().timestamp()).strftime(self.utc_format),
                    'user_followers': follower_count,
                    'user_following': following_count,
                    'user_id': user_id,
                    'user_name': user_name,
                    'user_posts': media_count
                }
                usr_hist = []
                usr_hist.append(user_hist)
                self.SolrCommit(usr_hist, self.__SolrUsersHistory)

        except Exception as err:
            self.logger.error("Extract User Fields Exception")
            self.logger.error("Casuse: %s" % err)
        return usr_docs

    def getCompleteUserInfo(self, user_id):
        try:
            response = self.Client.get_json(USER_INFO.format(user_id))
            user_json = json.loads(response)
            documents = self.ExtractUserFields(user_json['user'], 'user_info')
            return documents[0]['user_name']
        except Exception as err:
            self.logger.error("get User Info Exception")
            self.logger.error("Casuse: %s" % err)

    def getUserInfo(self, post_code, user_id):
        self.logger.info('User Post_Code Limit: {}'.format(self.UserPostCodeLimit))
        self.UserPostCodeLimit += 1
        if self.UserPostCodeLimit % 150 == 0:
            time.sleep(15*60)
            self.UserPostCodeLimit = 1
        return self.ExtractPostCodeFields(post_code, user_id)


    def ExtractPostFields(self, user_json, user, next_page, post_type):
        documents = []
        if post_type is 'user_post':
            media = user_json['edge_owner_to_timeline_media']
            if next_page:
                user_id = user['id']
                user_name = user['username']
            else:
                user_id = user_json['id']
                user_name = user_json['username']
        elif post_type is 'hashtag':
            media = user_json['edge_hashtag_to_media']

        posts = media['edges']
        counter = 0
        user_data_documents = []
        for pst in posts:
            try:
                post = pst['node']
                if 'accessibility_caption' in post and post['accessibility_caption']:
                    text = post['accessibility_caption'].split(",")
                    del text[0]
                    text = " ".join(text)
                    mentions = self.__extract_mentions(text)

                else:
                    mentions = set()
                if post_type is 'hashtag':
                    user_id = int(post['owner']['id'])
                    self.logger.info('post_code = {}, userName = {}'.format(post['shortcode'], user_id))
                    solr = pysolr.Solr(self.__SolrUsersInfoIndex, auth=('solr', 'Solr@123'), timeout=15)
                    insta_user = solr.search("*:*", fq="user_id:{}".format(user_id), fl="user_name")
                    # if user_id in self.Users_Info:
                    if insta_user.docs.__len__() > 0:
                        # user_name = self.Users_Info[user_id]
                        user_name = insta_user.docs[0]['user_name']
                    else:
                        user_data = self.getUserInfo(post['shortcode'], user_id)
                        user_data_documents.append(user_data)
                        user_name = user_data['user_name']
                date = post['taken_at_timestamp']
                type = 'image'
                if post_type is 'user_post':
                    if post['__typename'] == 'GraphVideo':
                        type = 'video'
                    elif post['__typename'] == 'GraphSidecar':
                        type = 'carousel'
                elif post_type is 'hashtag' and post['is_video']:
                    type = 'video'
                Extra_Data = {}
                if post['is_video'] or post['comments_disabled']:
                    try:
                        Extra_Data = self.getCommentsText(user_name, post['shortcode'], post['is_video'])
                    except Exception as err:
                        self.logger.error('getCommentsText Error')
                        self.logger.error("Casuse: %s" % err)
                cpt = post['edge_media_to_caption']['edges']
                caption = ''
                if len(cpt) > 0:
                    caption = cpt[0]['node']['text']
                    mentions.update(self.__extract_mentions(caption))
                    caption = caption
                comments_count = 0
                if len(post['edge_media_to_comment']) > 0:
                    comments_count = post['edge_media_to_comment']['count']

                likes_count = 0
                if not next_page and len(post['edge_liked_by']) > 0:
                    likes_count = post['edge_liked_by']['count']
                elif next_page:
                    likes_count = post['edge_media_preview_like']['count']

                doc = {
                    'id': post['id'],
                    'post_code': post['shortcode'],
                    'hashtags': list(self.__ExtractHashtags(caption)),
                    'comment_disabled': post['comments_disabled'],
                    'user_id': user_id,
                    'user_name': user_name,
                    'type': type,
                    'date': self.__utc_to_local(date).strftime(self.utc_format),
                    'image_url': post['display_url'],
                    'caption': caption,
                    'comments_count': comments_count,
                    'like_count': likes_count,
                }
                if mentions.__len__() > 0:
                    doc.update({"mentions": list(mentions)})
                doc.update(Extra_Data)
                documents.append(doc)
            except Exception as err:
                self.logger.error("Extract Post Fields Exception")
                self.logger.error("Casuse: %s" % err)
        self.logger.info('{}: {}'.format(user_name, counter))
        self.SolrCommit(user_data_documents, self.__SolrUsersInfoIndex)
        return documents

    def get_additional_data(self, url=''):
        try:
            resp = self.Client.session.get('https://www.instagram.com/' + url)
            if resp.status_code == 200 and '__additionalDataLoaded' in resp.text:
                response = resp.text.split("window.__additionalDataLoaded(")[1].split(',', 1)[1].split(");</script>")[0]
                return json.loads(response)
            return
        except Exception as err:
            self.logger.error("error while getting additional data")
            self.logger.error("Casuse: %s" % err)


    def getCommentsText(self, user_name, post_code, is_video):
        url = 'p/' + post_code + '/?taken-by=' + user_name
        Extra_Data = {'location': ''}
        try:
            resp = self.Client.session.get('https://www.instagram.com/' + url)
            shared_data = resp.text.split("window._sharedData = ")[1].split(";</script>")[0]
            post_data = json.loads(shared_data)
            # print(post_data)
            post = post_data['entry_data']['PostPage'][0]['graphql']['shortcode_media']
        except:
            post_data = self.get_additional_data(url)
            if post_data:
                post = post_data['graphql']['shortcode_media']
            else:
                post = None
        if post:
            if is_video:
                Extra_Data['video_url'] = post['video_url']
                Extra_Data['video_views'] = post['video_view_count']

            Extra_Data['location'] = str(post['location'])
        return Extra_Data


    def __utc_to_local(self, utc_dt):
        return datetime.datetime.fromtimestamp(utc_dt)

    def __ExtractHashtags(self, input):
        if input is None:
            return set()
        return set(re.findall(r"#(\w+)", input))

    def __extract_mentions(self, text):
        final_mentions = set()
        if text:
            mentions = re.findall("@([^:\s]+)", text)
            for indx, item in enumerate(mentions):
                item = item.strip()
                if item.__len__() < 3:
                    del mentions[indx]
                    continue
                if item.endswith("."):
                    item = item[:-1]
                final_mentions.add(item)
        return final_mentions


    def SolrExtractFields(self, user_json, user, next_page, post_type, users_history):
        documents = []
        # user_doc = dict()
        if user_json is None:
            self.logger.info('This user not found in Instagram.')
            return documents
        if post_type in ['user_post', 'user_info']:
            if not next_page:
                documents = self.ExtractUserFields(user_json, post_type, users_history)
        if post_type in ['hashtag', 'user_post']:
            documents = self.ExtractPostFields(user_json, user, next_page, post_type)
        return documents

    def SolrCommit(self, documents, solr_address):
        solr = pysolr.Solr(solr_address, auth=('solr', 'Solr@123'), timeout=15)
        solr.add(documents, commit=False, softCommit=True, )
        self.logger.info('Solr {} documents Commited'.format(solr_address))


    def getUsersAllPosts(self, user_names, next_page, users_history):
        counter = 0
        users_id = []

        for user_name in user_names:
            self.logger.info('UserName: {}\tPage No: 0'.format(user_name))
            try:
                user = {'id': 0, 'username': user_name}
                user_data = self.Client.get_shared_data(user_name)
                # print(json.dumps(user_data, ensure_ascii=False))

                if user_data is None:
                    self.logger.info('User with [{}] user name is not exist in Instagram.'.format(user_name))
                    continue
                user_json = user_data['entry_data']['ProfilePage'][0]['graphql']['user']
                documents = self.SolrExtractFields(user_json, user, False, 'user_post', users_history)
                users_id.append(documents[0]['user_id'])

                self.SolrCommit(documents, self.__SolrPostsIndex)
                if next_page and len(documents) > 0:
                    user.update({'id': documents[0]['user_id']})
                    has_next_page = user_json['edge_owner_to_timeline_media']['page_info']['has_next_page']
                    Page_Counter = 1
                    while has_next_page and Page_Counter < self.__MaxPage:
                        self.logger.info('UserName: {}\tPage No: {}'.format(user_name, Page_Counter))
                        try:
                            end_cursor = user_json['edge_owner_to_timeline_media']['page_info']['end_cursor']
                            self.Client.rhx_gis = ''
                            node = self.getEntityJSON(url=QUERY_MEDIA, url_var=QUERY_MEDIA_VARS, entity=user['id'],
                                                      end_cursor=end_cursor)
                            user_json = node['data']['user']
                            documents = self.SolrExtractFields(user_json, user, True, 'user_post', users_history)
                            self.SolrCommit(documents, self.__SolrPostsIndex)
                            has_next_page = user_json['edge_owner_to_timeline_media']['page_info']['has_next_page']
                            Page_Counter += 1
                        except Exception as err:
                            self.logger.error("get Next Posts Exception with end_cursor= " + end_cursor)
                            self.logger.error("Casuse: %s" % err)

            except Exception as err:
                self.logger.error("get Last Posts Exception")
                self.logger.error("Casuse: %s" % err)

        return users_id


    def getHashtagsAllPosts(self, hashtags, next_page, users_history):
        counter = 0
        self.Client.rhx_gis = ''
        for hashtag in hashtags:
            self.logger.info('Hashtag: {}\tPage No: 0'.format(hashtag))
            try:
                user = {'id': 0, 'username': hashtag}
                user_data = self.getEntityJSON(url=QUERY_HASHTAG, url_var=QUERY_HASHTAG_VARS, entity=hashtag, end_cursor='')
                user_json = user_data['data']['hashtag']
                documents = self.SolrExtractFields(user_json, user, False, 'hashtag', users_history)
                self.SolrCommit(documents, self.__SolrPostsIndex)
                counter += 1
                self.logger.info("Hashtags Count is : %s " % counter)
                if next_page and documents.__len__() > 0:
                    has_next_page = user_json['edge_hashtag_to_media']['page_info']['has_next_page']
                    Page_Counter = 1
                    while has_next_page and Page_Counter < self.__MaxPage:
                        self.logger.info('Hashtag: {}\tPage No: {}'.format(hashtag, Page_Counter))
                        try:
                            end_cursor = user_json['edge_hashtag_to_media']['page_info']['end_cursor']
                            self.Client.rhx_gis = ''
                            node = self.getEntityJSON(url=QUERY_HASHTAG, url_var=QUERY_HASHTAG_VARS, entity=hashtag, end_cursor=end_cursor)
                            user_json = node['data']['hashtag']
                            documents = self.SolrExtractFields(user_json, user, True, 'hashtag', users_history)
                            self.SolrCommit(documents, self.__SolrPostsIndex)
                            has_next_page = user_json['edge_hashtag_to_media']['page_info']['has_next_page']
                            Page_Counter += 1
                        except Exception as err:
                            self.logger.error("get Next Posts Exception with end_cursor= " + end_cursor)
                            self.logger.error("Casuse: %s" % err)

            except Exception as err:
                self.logger.error("get Hashtags All Posts Exception")
                self.logger.error("Casuse: %s" % err)


    def ExtractFollowFields(self, user_list):
        usr_docs = []
        for user in user_list:
            try:
                user_json = user['node']
                user_doc = {
                    'user_id': user_json['id'],
                    'user_name': user_json['username'],
                    'user_full_name': self.normalizer.clean_sentence(user_json['full_name']),
                    'user_is_private': user_json['is_private'],
                    'user_is_verified': user_json['is_verified'],
                    'user_profile_pic': user_json['profile_pic_url'],
                }
                usr_docs.append(user_doc)
            except Exception as err:
                self.logger.error("Extract User Fields Exception")
                self.logger.error("Casuse: %s" % err)
        self.SolrCommit(usr_docs, self.__SolrUsersInfoIndex)
        return usr_docs

    def getUsersAllFollow(self, type, users_id, next_page):
        # documents = []
        field = 'edge_follow'
        follow_url = self.QUERY_Following
        if type is 'follower':
            field = 'edge_followed_by'
            follow_url = self.QUERY_Follower
        for user_id in users_id:
            try:
                user_data = self.getEntityJSON(url=follow_url, url_var=self.QUERY_Following_VARS, entity=user_id, end_cursor='')
                start_time = time.time()
                if user_data is None:
                    self.logger.info('User with [{}] user ID is not exist in Instagram.'.format(user_id))
                    continue
                user_json = user_data['data']['user'][field]
                docs = self.ExtractFollowFields(user_json['edges'])
                # documents.append(docs)
                doc_counter = len(docs)
                counter = 1
                self.logger.info('User Size: {}'.format(counter))
                if next_page and len(docs) > 0:
                    has_next_page = user_json['page_info']['has_next_page']
                    now = time.time()
                    while has_next_page:
                        if (counter > 195):
                            sleep_delay = 905 - (now - start_time)
                            self.logger.warning('Requests counter reached to max. System sleep for {} seconds.'.format(sleep_delay))
                            time.sleep(sleep_delay)
                            start_time = time.time()
                            counter = 0
                        try:
                            end_cursor = user_json['page_info']['end_cursor']
                            self.Client.rhx_gis = ''
                            node = self.getEntityJSON(url=follow_url, url_var=self.QUERY_Following_VARS, entity=user_id, end_cursor=end_cursor)
                            user_json = node['data']['user'][field]
                            docs = self.ExtractFollowFields(user_json['edges'])
                            # documents.append(docs)
                            has_next_page = user_json['page_info']['has_next_page']
                            now = time.time()
                            doc_counter += len(docs)
                            if (now - start_time) < 901:
                                counter += 1
                            else:
                                start_time = time.time()
                                counter = 0
                            self.logger.info('Requests Count {0}\tUser Size: {1}'.format(counter, doc_counter))
                        except Exception as err:
                            counter += 1
                            self.logger.error("get Next Posts Exception with end_cursor= " + end_cursor)
                            self.logger.error("Casuse: %s" % err)
            except Exception as err:
                self.logger.error("get Last Posts Exception")
                self.logger.error("Casuse: %s" % err)
        # return docs


    def getEntityJSON(self, **kwargs):
        try:
            params = kwargs['url_var'].format(kwargs['entity'], kwargs['end_cursor'])
            self.Client.update_ig_gis_header(params)
            resp = self.Client.get_json(kwargs['url'].format(params))
            return json.loads(resp)
        except Exception as err:
            self.logger.error("get Json Exception")
            self.logger.error("Casuse: %s" % err)




if __name__ == '__main__':
    solr_index = 'http://localhost:8983/solr/insta_general'  # insta_arab
    user_index = 'http://localhost:8983/solr/insta_users'
    # logging.basicConfig(handlers=[logging.FileHandler('UserLogs.log', 'w', 'utf-8')],format='[%(asctime)s] %(levelname)s %(name)s %(message)s',  level=logging.INFO)#,
    logging.basicConfig(stream=sys.stdout, format='[%(asctime)s] %(filename)s %(lineno)d %(levelname)s %(name)s %(message)s', level=logging.INFO)
    crawler = InstagramCrawler(logging, SolrPostsIndex=solr_index, SolrUsersInfoIndex=user_index, MaxPage=1)
    crawler.Client.session.cookies.set('sessionid', '50109634649:GacJUX6LlgYVxZ:20')


    crawler.getHashtagsAllPosts(['ایران'], False, True)