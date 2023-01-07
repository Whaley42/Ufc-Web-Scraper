import scrapy
import atexit
from .UfcDataCleaner import UfcDataCleaner as clean
from .UfcPipeline import UfcPipeline as pipeline
from .Helper import Helper
import time


class UfcSpider(scrapy.Spider):
    name = 'ufc'
    start_urls = ['https://www.ufc.com/athletes/all?gender=All&search=&page=0']


    def exit_handler(self):

        end_time = time.time()
        print(f"Final time: {end_time - self.time_start}")
        self.pipeline.send_to_csv(self.fighters) 
         
    
    def __init__(self, name=None, **kwargs):
        
        self.time_start = time.time()
        self.cleaner = clean()
        self.pipeline = pipeline()
        self.helper = Helper()
        self.base_url = "https://www.ufc.com/athletes/all?gender=All&search=&page="
        self.next_page_num = 0
        self.next_page = self.base_url + str(self.next_page_num)
        self.current_page = 0
       
        self.fighters = {}
        atexit.register(self.exit_handler)
        
        
    def parse(self, response):
        """Parses through all fighter links on the current page.
           Continutes to next page until there are no more fighters

        Args:
            response: response to the web page.
        """
        
        athletes = response.css('.c-listing-athlete-flipcard__back')
        
        for athlete in athletes:
            base_url = "https://www.ufc.com"
            link =  athlete.css('a::attr(href)').get()
            athlete_link = base_url + link + "?page=0"
             
            yield scrapy.Request(url=athlete_link, callback=self.parse_athlete, priority=1)
                
        self.next_page_num += 1
        
        if len(athletes) != 0:
            self.current_page += 1
            self.next_page = self.base_url + str(self.next_page_num)
            yield response.follow(url=self.next_page, callback=self.parse)
      

    def parse_athlete(self, response):
        """Creates a key for each fighter that will be stored in python dictionary. 
            Goes through the fighter's pages to calculate total ufc fights and wins.
            Finally, scrapes figher information.

        Args:
            response: response to the current page.
        """
        
        url_list = response.request.url.split("/")
        part_id = url_list[len(url_list) -1]
        unique_key = part_id.split("?")[0]
        
        if not unique_key in self.fighters:
            self.fighters[unique_key] = self.helper.reset_data()
            self.fighters[unique_key]["Page"] = 0
            self.fighters[unique_key]["Weight"] = ''
        
            names = str(response.css('.hero-profile__name::text').extract_first())
            
            if " " in names:
                name = names.split(" ")
                if name[1].isalpha():
                    self.fighters[unique_key]["fight_name"] = name[1]
                else:
                    self.fighters[unique_key]["fight_name"] = name[0]
                    name = name[0]
            else:
                self.fighters[unique_key]["fight_name"] = names
                
        
        curr_url = response.request.url
        button = response.css('.button').extract()
        fights, wins = self.helper.fight_stats(response, self.fighters[unique_key]["fight_name"])
        self.fighters[unique_key]["Fights"] += fights
        self.fighters[unique_key]["Wins"] += wins
        
        if button:
            base_url = curr_url.split("=")[0]
            self.fighters[unique_key]["Page"] += 1
            next_url = base_url + "=" + str(self.fighters[unique_key]["Page"])
            yield scrapy.Request(url=next_url, callback=self.parse_athlete)
        else:
    
            self.get_basic_info(response, unique_key)
            self.get_accuracy_stats(response, unique_key)
            self.get_base_stats(response, unique_key)         
            self.get_area_stats(response, unique_key)
            self.get_target_stats(response, unique_key)
            self.get_bio(response, unique_key)
            
            self.fighters[unique_key]= self.cleaner.clean_data(self.fighters[unique_key])
            
            
    def get_basic_info(self, response, id):
        """Add name and division to the fighter dictionary.

        Args:
            response: Response to the current page.
            id string: Unique id for the fighter
        """
        name = str(response.css('.hero-profile__name::text').extract_first())
        division = response.css('.hero-profile__division-title::text').extract_first()
       
        if " " in name:
            names = name.split(" ")
            self.fighters[id]['first_name'] = names[0]
            self.fighters[id]['last_name'] = names[1]
        else:
            self.fighters[id]['first_name'] = name
            
        if division != None:
            division = division.split(" Division")[0]
            self.fighters[id]['Division'] = division
        
        
    def get_accuracy_stats(self, response, id):
        """Add Sig. Strike and Takedown accuracy stats to the fighter dictionary.

        Args:
            response: Response to the current page.
            id string: Unique id for the fighter.
        """
        accuracy_list = response.css(".c-overlap__stats-value::text , .c-overlap__stats-text::text").extract()
        percent_list = response.css("text.e-chart-circle__percent::text").extract()
        percent = -1
            
        if len(accuracy_list) != 8:
           accuracy_list = self.helper.fix_accuracy_lists(accuracy_list)
           
        accuracy_dict = self.helper.list_to_dict(accuracy_list)
        
        if len(percent_list) == 2:
            percent = percent_list[1]
            percent = percent.replace("%", "")
            takedowns_percent = int(percent) / 100
        else:
            accuracy_dict["Takedowns Landed"] = 0
            accuracy_dict["Takedowns Attempted"] = 0
        
        if accuracy_dict["Takedowns Landed"] == "" and percent != -1:
            accuracy_dict["Takedowns Landed"] = round(takedowns_percent * int(accuracy_dict["Takedowns Attempted"]))
 
        self.add_items(accuracy_dict, id)
        
    
    def add_items(self, dict, id):
        """Function for adding stats to the fighter dictionary. 

        Args:
            dict dictionary: Dictionary of stats to be added.
            id string: Unique id for the fighter.
        """
        for label, value in dict.items():
            new_label = label.strip()
            new_label = new_label.replace(" ", "_")
            new_label = new_label.replace(".", "")
            new_label = new_label.replace("/","_")
            self.fighters[id][new_label] = value
            
    
    def get_area_stats(self, response, id):
        """Add Strikes by area to the fighter dictionary.

        Args:
            response: Response to the current page.
            id string: Unique id for the fighter.
        """
        area_values = response.css('.c-stat-3bar__value::text').extract()
        area_labels = response.css('.c-stat-3bar__label::text').extract()

        area_dict = self.helper.lists_to_dict(area_labels, area_values)
        self.add_items(area_dict, id)


    def get_target_stats(self, response, id):
        """Add strikes by target to the fighter dictionary.

        Args:
            response: Response to the current page.
            id string:Unique id for the fighter.
        """
        sig_strike_head = response.css('text#e-stat-body_x5F__x5F_head_value::text').extract_first()
        sig_strike_body = response.css('text#e-stat-body_x5F__x5F_body_value::text').extract_first()
        sig_strike_leg = response.css('text#e-stat-body_x5F__x5F_leg_value::text').extract_first()
        if sig_strike_head != None:
            self.fighters[id]['Sig_Str_Head'] = sig_strike_head
        if sig_strike_body != None:
            self.fighters[id]['Sig_Str_Body'] = sig_strike_body
        if sig_strike_leg != None:
            self.fighters[id]['Sig_Str_Leg'] = sig_strike_leg


    def get_base_stats(self,response, id):
        """Add the main fighter stats to the fighter dictionary. 

        Args:
            response: Response to the current page.
            id string: Unique id for the fighter.
        """
        comparison_list = response.css(".c-stat-compare__label::text , .c-stat-compare__number::text").extract()    

        for i, data in enumerate(comparison_list):
            clean_data = data.strip()
            comparison_list[i] = clean_data
       
        if len(comparison_list) == 15:
            comparison_list = self.helper.fix_base_lists(comparison_list)
                           
        if len(comparison_list) == 16:
            cleaned = self.helper.swap(comparison_list)
            comp_dict = self.helper.list_to_dict(cleaned)
            self.add_items(comp_dict, id)
            
        
        

    def get_bio(self, response, id):
        """Add the personal information about the fighter to the fighter dictionary.

        Args:
            response: Response to the current page.
            id string: Unique id for the fighter.
        """
        bio_values = response.css('.c-bio__text::text').extract()
        bio_labels = response.css('.c-bio__label::text').extract()
        age = response.css('.field--name-age::text').extract_first() 
    
        bio_values = self.helper.clean_list(bio_values)
        if age:
            age_idx = bio_labels.index("Age")
            bio_values.insert(age_idx, age)
        
        bio_values = self.helper.clean_list(bio_values)
        bio_dict = self.helper.lists_to_dict(bio_labels, bio_values)  
        
        self.add_items(bio_dict, id)

    

          
    






    

            
    

        
        







        
