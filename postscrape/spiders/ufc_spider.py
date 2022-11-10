import scrapy
from .. items import PostscrapeItem
from . UfcDataCleaner import UfcDataCleaner as clean
from . UfcPipeline import UfcPipeline as pipeline
import csv

class UfcSpider(scrapy.Spider):
    name = 'ufc'
    start_urls = ['https://www.ufc.com/athletes/all?gender=All&search=&page=0']
    
    def __init__(self, name=None, **kwargs):
        self.items = PostscrapeItem()
        self.amount_fights = 0
        self.amount_called = 0
        self.nums = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9' ]
        self.cleaner = clean()
        self.pipeline = pipeline()
        
        
    

    def parse(self, response):
        
        athletes = response.css('.c-listing-athlete-flipcard__back').extract()

        # print(len(athletes))
        for athlete in response.css('.c-listing-athlete-flipcard__back'):
            
            link =  athlete.css('a::attr(href)').get()
            
            yield response.follow(url=link, callback=self.parse_athlete)
            
            # fights = response.css('#athlete-record .datetime::text').extract()
            # self.amount_fights += len(fights)
            # fight_page = response.css('li.pager__item a::attr(href)').get()
            # if fight_page is not None:
            #     yield response.follow(fight_page, callback= self.parse)
            
            # self.response = response
            # self.get_stats_records(response)
            # self.get_accuracy_stats(response)
            # self.get_base_stats(response)
            # self.get_misc_stats(response)
            # self.get_target_stats(response)
            # self.get_bio(response)
            
            

            # self.items["Fights"] = self.amount_fights
            # self.items["amount_called"] = self.amount_called
            
        # yield self.items

    def parse_athlete(self, response):
        self.get_basic_info(response)
        self.get_accuracy_stats(response)
        self.get_base_stats(response)
        self.get_misc_stats(response)
        self.get_target_stats(response)
        self.get_bio(response)
        # print(self.items)
        clean_dict = self.cleaner.clean_data(athlete_info=dict(self.items))
        self.items.clear()
        self.pipeline.send_to_csv(clean_dict)
        


    def get_basic_info(self, response):
        name = str(response.css('.hero-profile__name::text').extract_first())
        division = response.css('.hero-profile__division-title::text').extract_first()
       
        names = name.split(" ")
        self.items['first_name'] = names[0]
        self.items['last_name'] = names[1]
        self.items['Division'] = division
    
        



    def format_list(self, lst):
        new_lst = []
        for i in range(0, len(lst), 2):
            key = lst[i + 1]
            value = lst[i]
            new_lst.append({key:value})
        return new_lst
    
    # def get_stats_records(self, response):
    #     stats_records = response.css('.athlete-stats__text::text').extract()
    #     stats_records_len = len(stats_records)
    #     if stats_records_len > 0:
    #         stats_formatted = self.format_list(stats_records)
    #         self.items['stat_1'] = stats_formatted[0]
    #         self.items['stat_2'] = stats_formatted[1]
    #         self.items['stat_3'] = stats_formatted[2]
    #     else:
    #         self.items['stat_1'] = "N/A"
    #         self.items['stat_2'] = "N/A"
    #         self.items['stat_3'] = "N/A"
    
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

        self.items['Sig_Str_Head'] = sig_strike_head
        self.items['Sig_Str_Body'] = sig_strike_body
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
        

        # print(name)
        # print(formatted_comp_list)
        # print(len(comparison_labels))
        # print(len(comparison_values))
        # if len(comparison_labels) == len(comparison_values):
        #     comparison_dict = self.lists_to_dict(comparison_labels, comparison_values)
        #     self.add_items(comparison_dict)
    
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
    
    

            
    

        
        







        
