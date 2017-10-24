import requests

class Bitly(object):
    EP = 'https://api-ssl.bitly.com'
    def __init__(self, KEY):
        self.key = KEY

    def shorten(self, url, format='json'):
        URL = '{}/v3/shorten'.format(self.EP)

        params = {
            'access_token': self.key,
            'longUrl': url,
            'format': format,
        }
        tmp = requests.get(URL, params=params)

        try:
            return tmp.json()['data']['url']
        except TypeError:
            return tmp.json()['status_txt']

