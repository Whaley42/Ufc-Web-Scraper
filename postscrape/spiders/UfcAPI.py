import requests
import json
import sys


url = 'https://www.ufc.com/views/ajax?_wrapper_format=drupal_ajax'

class UfcAPI:
    # def __init__(self, args, path, dom_id) -> None:
        
    #     self.headers = {
    #         'authority': 'www.ufc.com',
    #         'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
    #         'x-requested-with': 'XMLHttpRequest'
    #     }

    #     self.data = {
    #         'view_name': 'athlete_results',
    #         'view_display_id': 'entity_view_1', 
    #         'view_args': args,
    #         'view_path': path,
    #         'view_dom_id': dom_id,
    #         'pager_element': 0,
    #         'page': 0,
    #         '_drupal_ajax': 1,
    #     }

    def __init__(self, args, path, dom_id) -> None:
        self.view_args = args
        self.view_path = path
        self.view_dom_id = dom_id
        self.headers = {
            'authority': 'www.ufc.com',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        }

    # def set_args(self, args_in, path_in, dom_id_in):
    #     UfcAPI.data['view_args'] = args_in
    #     UfcAPI.data['view_path'] = path_in
    #     UfcAPI.data['view_dom_id'] = dom_id_in

    def get_response(self,page):
        # print(f"Current page: {page}")
        data = {
            'view_name': 'athlete_results',
            'view_display_id': 'entity_view_1', 
            'view_args': self.view_args,
            'view_path': self.view_path,
            'view_dom_id': self.view_dom_id,
            'pager_element': 0,
            'page': 0,
            '_drupal_ajax': 1,
        }
        data['page'] = page
        response = requests.post(url=url, data=data,headers=self.headers)
        ajax_dict = json.loads(response.text)
        ajax_dict = ajax_dict[len(ajax_dict)-1]
        ajax_data = ajax_dict['data']
        # print("--Data--")
        # print(ajax_data)
        
        
        return ajax_data


    def get_responsev2(self):
        response = requests.post(url=url, data=self.data,headers=self.headers)
        ajax_dict = json.loads(response.text)
        ajax_dict = ajax_dict[len(ajax_dict)-1]
        ajax_data = ajax_dict['data']
        
        
        return ajax_data


    def next_page(self):
        self.data['page'] += 1













    # print(response)
    # # text = data.encode('utf-8').decode('ascii', 'ignore')
    # json_data = json.loads(response.text)
    # print(json.dumps(json_data, indent=4))
    # my_str = "Stöcker"
    # # print(u"{0}".format(text))
    # def print_utf(text):
    #     print(text.encode(sys.stdout.encoding, UnicodeDecodeError='replace'))