import scrapy
from .. items import PostscrapeItem
from . UfcDataCleaner import UfcDataCleaner as clean
from . UfcPipeline import UfcPipeline as pipeline
from . UfcAPI import UfcAPI as api
import json
import concurrent.futures
from itertools import repeat
import time


class UfcSpider(scrapy.Spider):
    name = 'ufc'
    start_urls = ['https://www.ufc.com/athletes/all?gender=All&search=&page=0']
    
    def __init__(self, name=None, **kwargs):
        self.items = PostscrapeItem()
        self.items['Fights'] = 0
        self.amount_fights = 0
        self.nums = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9' ]
        self.cleaner = clean()
        self.pipeline = pipeline()
        self.base_url = "https://www.ufc.com/athletes/all?gender=All&search=&page="
        self.next_page_num = 0
        self.next_page = self.base_url + str(self.next_page_num)
        self.current_page = 0
        self.api = ''
     

    
    

    def parse(self, response):
      
        athletes = response.css('.c-listing-athlete-flipcard__back')
        
        for athlete in athletes:
                
            link =  athlete.css('a::attr(href)').get()
            
            yield response.follow(url=link, callback=self.parse_athlete)
        
        
        
        self.next_page_num += 1
        
        
        if len(athletes) != 0:
            self.current_page += 1
            self.next_page = self.base_url + str(self.next_page_num)
            yield response.follow(url=self.next_page, callback=self.parse)
        

    def parse_athlete(self, response):
        self.get_basic_info(response)
        self.get_accuracy_stats(response)
        self.get_base_stats(response)
        self.get_misc_stats(response)
        self.get_target_stats(response)
        self.get_bio(response)
        self.parse_fights(response)
        clean_dict = self.cleaner.clean_data(athlete_info=dict(self.items))
        self.items.clear()
        self.items['Fights'] = 0
        #print(clean_dict)
        self.pipeline.send_to_csv(clean_dict)
        
    def parse_fights(self, response):
        script_text = response.xpath('/html/head/script[@data-drupal-selector]//text()').extract_first()
        script = json.loads(script_text)
        pretty = json.dumps(script, indent=4)
        threads = 12
        


        try:
            dom_key = list(script['views']['ajaxViews'].keys())[0]
            ajax = script['views']['ajaxViews'][dom_key]
            view_args = ajax['view_args']
            view_path = ajax['view_path']
            view_dom_id = ajax['view_dom_id']
            # ufc_api = api()
            ufc_api = api(view_args, view_path, view_dom_id)
            # fight_results = ufc_api.get_responsev2()
            # while fight_results != '':
            #     self.items['Fights'] += fight_results.count('c-card-event--athlete-results__headline')
            #     ufc_api.next_page()
            #     fight_results = ufc_api.get_responsev2()
            
            # print(f"----Name----: {self.items['first_name']}")
            # print(f"Total fights: {self.items['Fights']} ")
                

            
         
            # print(f"----Name----: {self.items['first_name']}")           
            # ufc_api.set_args(view_args, view_path, view_dom_id)
            test_lst = [0,1,2,3,4,5,6,7,8,9,10,11,11]
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                for result in executor.map(ufc_api.get_response, test_lst):
                    # self.items['Fights'] += result.count('c-card-event--athlete-results__headline')
                    amount_fights = result.count('c-card-event--athlete-results__headline')
                    if amount_fights == 0:
                        break
                    self.items['Fights'] += amount_fights

                    
                    # # print(result)
                    
            
            # fight_data = ufc_api.get_response()
        except Exception as e:
            print(e)
        # print(f"{self.items['Fights']}")
        self.items['Fights'] = str(self.items['Fights'])
        



    def get_basic_info(self, response):
        name = str(response.css('.hero-profile__name::text').extract_first())
        division = response.css('.hero-profile__division-title::text').extract_first()
       
        if " " in name:
            names = name.split(" ")
            self.items['first_name'] = names[0]
            self.items['last_name'] = names[1]
        else:
            self.items['first_name'] = name
            self.items['last_name'] = ''
        if division == None:
            self.items['Division'] = 'Unknown'
        else:
            self.items['Division'] = division
        
    


    def format_list(self, lst):
        new_lst = []
        for i in range(0, len(lst), 2):
            key = lst[i + 1]
            value = lst[i]
            new_lst.append({key:value})
        return new_lst

    
    def get_accuracy_stats(self, response):
        accuracy = response.css('.c-overlap__stats-text::text , .c-overlap__stats-value::text').extract()
        accuracy_dict = self.clean_accuracy_list(accuracy)
        self.add_items(accuracy_dict)
        
        

    def add_items(self, dict):
        for label, value in dict.items():
            new_label = label.strip()
            new_label = new_label.replace(" ", "_")
            new_label = new_label.replace(".", "")
            new_label = new_label.replace("/","_")
            self.items[new_label] = value
    
    def get_misc_stats(self, response):
        misc_values = response.css('.c-stat-3bar__value::text').extract()
        misc_labels = response.css('.c-stat-3bar__label::text').extract()

        misc_dict = self.lists_to_dict(misc_labels, misc_values)
        self.add_items(misc_dict)
    
    def get_target_stats(self, response):
        sig_strike_head = response.css('text#e-stat-body_x5F__x5F_head_value::text').extract_first()
        sig_strike_body = response.css('text#e-stat-body_x5F__x5F_body_value::text').extract_first()
        sig_strike_leg = response.css('text#e-stat-body_x5F__x5F_leg_value::text').extract_first()
        if sig_strike_head != None:
            self.items['Sig_Str_Head'] = sig_strike_head
        if sig_strike_body != None:
            self.items['Sig_Str_Body'] = sig_strike_body
        if sig_strike_leg != None:
            self.items['Sig_Str_Leg'] = sig_strike_leg



    def combine_lists(self, list1, list2):
        new_list = []
        for v1, v2 in enumerate(zip(list1, list2)):
            new_list.append(v2)
            new_list.append(v1)
            


    def get_base_stats(self,response):
        name = str(response.css('.hero-profile__name::text').extract_first())
        comparison_values = response.css('.c-stat-compare__number::text').extract()
        comparison_labels = response.css('.c-stat-compare__label::text').extract()
        comparison_list = response.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "c-stat-compare__group", " " ))]//text()').extract()
        cleaned_comp_list = []
        for data in comparison_list:
            clean_data = data.strip()
            if clean_data != '' and clean_data != 'Per Min' and clean_data != 'Per 15 Min' and clean_data != '%':
                cleaned_comp_list.append(clean_data)
        
        
        formatted_comp_list = []
        for idx,data in enumerate(cleaned_comp_list):
            if idx == 0 and not any(substring in data for substring in self.nums):
                formatted_comp_list.append('N/A')
                formatted_comp_list.append(data)
            elif not any(substring in data for substring in self.nums) and not any(substring in cleaned_comp_list[idx-1] for substring in self.nums):
                formatted_comp_list.append('N/A')
                formatted_comp_list.append(data)
            else:
                formatted_comp_list.append(data)
            

        for idx in range(0, len(formatted_comp_list) - 1, 2):

            formatted_comp_list[idx], formatted_comp_list[idx+1] = formatted_comp_list[idx+1], formatted_comp_list[idx]
        
        comp_dict = self.list_to_dict(formatted_comp_list)
        self.add_items(comp_dict)
        

    
    def clean_accuracy_list(self, accuracy_list):
        #If the next value is not a number, and the current is not a number, add a 0
        new_list = []
        for i, info in enumerate(accuracy_list):
            curr_value = info
            next_value = "false"  
            try:
                curr_value = int(info)
            except:
                pass
            try:
                next_value = int(accuracy_list[i+1])
            except IndexError:
                new_list.append(info)
                break
            except:
                pass
        
            if (not isinstance(next_value, int)) and (not isinstance(curr_value, int)):
                new_list.append(info)
                new_list.append('0')
            else:
                new_list.append(info)
            

        
       
        return self.list_to_dict(new_list)
    

    def list_to_dict(self, data):
        value = iter(data)
        res_dct = dict(zip(value, value))
        return res_dct




    def get_bio(self, response):
        bio_values = response.css('.c-bio__text::text').extract()
        bio_labels = response.css('.c-bio__label::text').extract()
        age = response.css('.field--name-age::text').extract_first()

        bio_values = self.clean_list(bio_values)
        if age:
            age_idx = bio_labels.index("Age")
            bio_values.insert(age_idx, age)
        
        bio_values = self.clean_list(bio_values)
       
        bio_dict = self.lists_to_dict(bio_labels, bio_values)
       
        
        self.add_items(bio_dict)
        
        


    

    def clean_list(self, values):
        res = []
        for val in values:
            val = val.strip()
            val = val.replace('\n', '')
            if val != '':
                res.append(val)
        return res
    
    def lists_to_dict(self, labels, values):
        res = {}
        for label, val in zip(labels, values):
            res[label] = val
        return res
    
    

            
    

        
        







        
