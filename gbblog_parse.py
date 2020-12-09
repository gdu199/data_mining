import bs4
import requests
from urllib.parse import urljoin
from database import DataBase
import json

#некоторые комментарии не удалось превратить в json. Нужна помощь

class ServerError(Exception):
    def __init__ (self, txt):
        self.txt = txt


class GbBlogParse:

    def __init__(self, start_url: str, db: DataBase):
        self.start_url = start_url
        self.page_done = set()
        self.db = db


    def __get(self, *args, **kwargs) -> bs4.BeautifulSoup:
        while True:
            try:
                response = requests.get(*args, **kwargs)
                if response.status_code != 200:
                    raise ServerError("Ошибка обработки запроса!")
                self.page_done.add(kwargs.get('url'))
                return bs4.BeautifulSoup(response.text, 'lxml')
            except ServerError as err:
                time.sleep(0.5)


    def run(self, url=None):
        if not url:
            url = self.start_url

        if url not in self.page_done and len(self.page_done) < 5:
            soup = self.__get(url = url)
            posts, pagination = self.parse(soup)

            for post_url in posts:
                page_data = self.page_parse(self.__get(url = post_url), post_url)
                self.save(page_data)
            for p_url in pagination:
                self.run(p_url)

    def parse(self, soup):
        ul_pag = soup.find('ul', attrs={'class': 'gb__pagination'})
        paginations = set(
            urljoin(self.start_url, url.get('href')) for url in ul_pag.find_all('a') if url.attrs.get('href'))
        posts = set(
            urljoin(self.start_url, url.get('href')) for url in soup.find_all('a', attrs={'class': 'post-item__title'}))
        return posts, paginations

    def page_parse(self, soup, url) -> dict:
        # контент есть тут
        # tmp = soup.find('script', attrs={'type': 'application/ld+json'}).string

        data = {
            'post_data': {
                'url': url,
                'title': soup.find('h1').text,
                'image': soup.find('div', attrs={'class': 'blogpost-content'}).find('img').get('src') if soup.find(
                    'div', attrs={'class': 'blogpost-content'}).find('img') else None,
                'date': soup.find('div', attrs={'class': 'blogpost-date-views'}).find('time').get('datetime'),
            },
            'writer': {'name': soup.find('div', attrs={'itemprop': 'author'}).text,
                       'url': urljoin(self.start_url,
                                      soup.find('div', attrs={'itemprop': 'author'}).parent.get('href'))},

            'tags': [],

            'comments': []
        }
        for tag in soup.find_all('a', attrs={'class': "small"}):
            tag_data = {
                'url': urljoin(self.start_url, tag.get('href')),
                'name': tag.text
            }
            data['tags'].append(tag_data)

        #тут получаем данные комментариев со страницы
        commentable_id = soup.find('comments', attrs={'commentable-type': 'Post'}).get('commentable-id')
        comment_url = urljoin(self.start_url, '/api/v2/comments')
        params = {
            'commentable_type': 'Post',
            'commentable_id': commentable_id,
        }
        response: requests.Response = self.__get(url = comment_url, params=params)
        comments_soup = bs4.BeautifulSoup(response.text, 'lxml')
        comments_for_json = comments_soup.find('p').text
        try:
            comment_json = json.loads(comments_for_json)
            self.get_comments(comment_json, data, None)
        except:
            pass
            # наверное надо экранировать какие то символы. Не осилил
            # print(f'Фэил {commentable_id}')
            # print(comments_for_json)

        return data


    def get_comments(self, comment_json, data, parent_id):
        for el in comment_json:
            print(el.get('comment').get('body'))
            comment_data = {
                'id': el.get('comment').get('id'),
                'parent_id': parent_id,
                'comment_text': el.get('comment').get('body'),
                'Author': {'name': el.get('comment').get('user').get('full_name'),
                           'url': el.get('comment').get('user').get('url')},
            }
            data['comments'].append(comment_data)
            self.get_comments(el.get('comment').get('children'), data, el.get('comment').get('id'))

    def save(self, page_data: dict):
        self.db.create_post(page_data)


if __name__ == '__main__':
    db = DataBase('sqlite:///gb_blog.db')
    parser = GbBlogParse('https://geekbrains.ru/posts', db)

    parser.run()