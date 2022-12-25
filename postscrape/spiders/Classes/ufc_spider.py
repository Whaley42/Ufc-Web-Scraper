import scrapy
import atexit
from .UfcDataCleaner import UfcDataCleaner as clean
from .UfcPipeline import UfcPipeline as pipeline
from .UfcConnector import Connector as conn
from .Helper import Helper
import concurrent.futures
import logging
from pyspark.sql import SparkSession
import time



#TASKS:
#COMMENT AND CLEAN UP CODE/VARIABLE NAMES


class UfcSpider(scrapy.Spider):
    name = 'ufc'
    start_urls = ['https://www.ufc.com/athletes/all?gender=All&search=&page=0']

    def exit_handler(self):
        self.pipeline.send_to_csv(self.df_fact, self.df_bio)  
    
    def __init__(self, name=None, **kwargs):
        
        self.time = 0
        self.kill = False
        self.cleaner = clean()
        self.pipeline = pipeline()
        self.helper = Helper()
        self.data = self.helper.reset_data()
        self.base_url = "https://www.ufc.com/athletes/all?gender=All&search=&page="
        self.next_page_num = 0
        self.next_page = self.base_url + str(self.next_page_num)
        self.current_page = 0
        self.api = ''

        logging.getLogger("py4j").setLevel(logging.INFO)
        self.fact_heading = ["Bio ID", "Sig. Strikes Landed", "Sig. Strikes Attempted", "Sig. Strikes Landed Per Min", "Sig. Strikes Absorbed Per Min"
                            ,"Sig. Strike Defense", "Knockdown Average", "Sig. Strikes Standing", "Sig. Strikes Clinch", "Sig. Strikes Ground", "Sig. Strikes Head"
                            ,"Sig. Strikes Body", "Sig. Strikes Leg", "Takedowns Landed", "Takedowns Attempted", "Takedown Average", "Takedown Defense", "Submission Average"
                            ,"KO/TKO","DEC","SUB","Reach","Leg Reach", "Average Fight Time","Age", "Height","Number of Fights"]
        
        self.bio_heading = ["Bio ID", "First Name", "Last Name", "Division", "Status", "Hometown", "Fighting Style", "Trains At", "Octagon Debut"]

        schema_fact, schema_bio = self.helper.create_schemas()
        self.spark = SparkSession.builder.appName('ufc').getOrCreate()
        
        self.df_fact = self.spark.createDataFrame([],schema=schema_fact)
        self.df_bio = self.spark.createDataFrame([], schema=schema_bio)

        self.count = 0
        atexit.register(self.exit_handler)
        
        

    def parse(self, response):
        
        athletes = response.css('.c-listing-athlete-flipcard__back')
        
        for athlete in athletes:
                
            link =  athlete.css('a::attr(href)').get()
            
            yield response.follow(url=link, callback=self.parse_athlete)
        
        
        # self.next_page_num += 1
        
        # if len(athletes) != 0:
        #     self.current_page += 1
        #     self.next_page = self.base_url + str(self.next_page_num)
        #     yield response.follow(url=self.next_page, callback=self.parse)

        

    def parse_athlete(self, response):
        self.get_basic_info(response)
        validation1 = self.get_accuracy_stats(response)
        validation2 = self.get_base_stats(response)
        
        if validation1 and validation2:    
            self.get_misc_stats(response)
            self.get_target_stats(response)
            self.get_bio(response)
            startTime = time.time()
            self.parse_fights(response)
            endTime = time.time()
            self.time += (endTime -startTime)
            print(f"Total time: {self.time}")
            
            bio_data, fact_data = self.helper.seperate_tables(self.data)
            clean_fact = self.cleaner.clean_data(fact_data)
            clean_bio = self.cleaner.clean_data(bio_data)

            new_row_fact = self.spark.createDataFrame([clean_fact], self.fact_heading)
            new_row_bio = self.spark.createDataFrame([clean_bio], self.bio_heading)

            self.df_fact = self.df_fact.union(new_row_fact)
            self.df_bio = self.df_bio.union(new_row_bio)
            self.helper.increment_id()
            # self.df_fact.show(vertical=True)
            # self.df_bio.show(vertical=True)
        
        self.data = self.helper.reset_data()
        # self.df_fact.show(vertical=True)
        # self.df_bio.show(vertical=True)
        # self.pipeline.send_to_csv(clean_dict)
        
    def parse_fights(self, response):
        button = response.css('.button').extract()
        url = response.request.url
        self.count +=1

        name = ""
        if not self.data["last_name"]:
            name = self.data["first_name"]
        else:
            name = self.data["last_name"]


        total_fights = 0
        total_wins = 0
        if len(button) == 0:
            # print(f"Spider Response {response.text}")
            total_fights, total_wins = self.helper.fight_stats(response, name)
        else:
            connection = conn(url, name)
            test_lst = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19]      

            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                for result in executor.map(connection.get_response, test_lst):
                    if self.kill:
                        break
                    amount_fights, amount_wins = result
                    total_fights += amount_fights
                    total_wins += amount_wins
                    if amount_fights == 0:
                        self.kill = True
            
            self.kill = False
        self.data["Fights"] = total_fights
        self.data["Wins"] = total_wins
        
        
    def get_basic_info(self, response):
        name = str(response.css('.hero-profile__name::text').extract_first())
        division = response.css('.hero-profile__division-title::text').extract_first()
       
        if " " in name:
            names = name.split(" ")
            self.data['first_name'] = names[0]
            self.data['last_name'] = names[1]
        else:
            self.data['first_name'] = name
        if division != None:
            self.data['Division'] = division
        
    
    def get_accuracy_stats(self, response):
        accuracy_list = response.css(".c-overlap__stats-value::text , .c-overlap__stats-text::text").extract()
        percent_list = response.css("text.e-chart-circle__percent::text").extract()
        if len(percent_list) != 2:
            return False
        

        percent = percent_list[1]
        percent = percent.replace("%", "")
        takedowns_percent = int(percent) / 100
        if len(accuracy_list) != 8:
           accuracy_list = self.helper.fix_accuracy_lists(accuracy_list)
           

        accuracy_dict = self.helper.list_to_dict(accuracy_list)
        

        if accuracy_dict["Takedowns Landed"] == "":
            accuracy_dict["Takedowns Landed"] = round(takedowns_percent * int(accuracy_dict["Takedowns Attempted"]))
            

        
        self.add_items(accuracy_dict, "accuracy stats")
        return True
        
    
    def add_items(self, dict, func):
        for label, value in dict.items():
            new_label = label.strip()
            new_label = new_label.replace(" ", "_")
            new_label = new_label.replace(".", "")
            new_label = new_label.replace("/","_")
            self.data[new_label] = value

    
    def get_misc_stats(self, response):
        misc_values = response.css('.c-stat-3bar__value::text').extract()
        misc_labels = response.css('.c-stat-3bar__label::text').extract()

        misc_dict = self.helper.lists_to_dict(misc_labels, misc_values)
        self.add_items(misc_dict, "misc stats")


    def get_target_stats(self, response):
        sig_strike_head = response.css('text#e-stat-body_x5F__x5F_head_value::text').extract_first()
        sig_strike_body = response.css('text#e-stat-body_x5F__x5F_body_value::text').extract_first()
        sig_strike_leg = response.css('text#e-stat-body_x5F__x5F_leg_value::text').extract_first()
        if sig_strike_head != None:
            self.data['Sig_Str_Head'] = sig_strike_head
        if sig_strike_body != None:
            self.data['Sig_Str_Body'] = sig_strike_body
        if sig_strike_leg != None:
            self.data['Sig_Str_Leg'] = sig_strike_leg


    def get_base_stats(self,response):      
        comparison_list = response.css(".c-stat-compare__label::text , .c-stat-compare__number::text").extract()
        cleaned = []

        for data in comparison_list:
            clean_data = data.strip()
            cleaned.append(clean_data)
       

        if len(cleaned) == 15:
            cleaned = self.helper.fix_base_lists(cleaned)

                           
        if len(cleaned) == 16:
            cleaned = self.helper.swap(cleaned)
            comp_dict = self.helper.list_to_dict(cleaned)
            self.add_items(comp_dict, "base stats")
            return True
        else:
            return False
        

    def get_bio(self, response):
        bio_values = response.css('.c-bio__text::text').extract()
        bio_labels = response.css('.c-bio__label::text').extract()
        age = response.css('.field--name-age::text').extract_first()

        bio_values = self.helper.clean_list(bio_values)
        if age:
            age_idx = bio_labels.index("Age")
            bio_values.insert(age_idx, age)
        
        bio_values = self.helper.clean_list(bio_values)
       
        bio_dict = self.helper.lists_to_dict(bio_labels, bio_values)
       
        
        self.add_items(bio_dict, "bio")

          
    






    

            
    

        
        







        
