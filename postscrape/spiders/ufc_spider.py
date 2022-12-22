import scrapy

from .UfcDataCleaner import UfcDataCleaner as clean
from .UfcPipeline import UfcPipeline as pipeline
from .UfcAPI import UfcAPI as api
import json
import concurrent.futures
import logging
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql import SparkSession
import itertools
import time


#TASKS:
#VERIFY THE DATA
#SEE IF POSSIBLE TO MAKE REQUESTS MORE EFFICIENT 
#ADD FILTER TO FILTER OUT BAD DATA
#SEND TO CSV FILE AT THE END OF RUN TIME
#COMMENT AND CLEAN UP CODE/VARIABLE NAMES


class UfcSpider(scrapy.Spider):
    name = 'ufc'
    start_urls = ['https://www.ufc.com/athletes/all?gender=All&search=&page=0']
    
    def __init__(self, name=None, **kwargs):
        
        self.time = 0
        self.kill = False
        self.current_id = 0
        self.data = self.reset_data()
        self.nums = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9' ]
        self.cleaner = clean()
        self.pipeline = pipeline()
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

        schema_fact, schema_bio = self.create_schemas()
        self.spark = SparkSession.builder.appName('ufc').getOrCreate()
        
        self.df_fact = self.spark.createDataFrame([],schema=schema_fact)
        self.df_bio = self.spark.createDataFrame([], schema=schema_bio)

        self.count = 0
        
        
     

    
    

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
            
            bio_data, fact_data = self.seperate_tables(self.data)
            clean_fact = self.cleaner.clean_data(fact_data)
            clean_bio = self.cleaner.clean_data(bio_data)

            

            new_row_fact = self.spark.createDataFrame([clean_fact], self.fact_heading)
            new_row_bio = self.spark.createDataFrame([clean_bio], self.bio_heading)

            self.df_fact = self.df_fact.union(new_row_fact)
            self.df_bio = self.df_bio.union(new_row_bio)
            self.current_id += 1
            # self.df_fact.show(vertical=True)
            # self.df_bio.show(vertical=True)
        

        self.data = self.reset_data()

        # self.df_fact.show(vertical=True)
        # self.df_bio.show(vertical=True)
        # self.pipeline.send_to_csv(clean_dict)
        
    def parse_fights(self, response):
        # script_text = response.xpath('/html/head/script[@data-drupal-selector]//text()').extract_first()
        button = response.css('.button').extract()
        url = response.request.url
        # print(f"Current url: {url}")
        # script = json.loads(script_text)
        # pretty = json.dumps(script, indent=4)
        # threads = 12
        self.count +=1
        
        
        if len(button) == 0:
            fights = response.css(".c-card-event--athlete-results__headline").extract()
            self.data["Fights"] = len(fights)
        else:
            try:
                # dom_key = list(script['views']['ajaxViews'].keys())[0]
                # ajax = script['views']['ajaxViews'][dom_key]
                # view_args = ajax['view_args']
                # view_path = ajax['view_path']
                # view_dom_id = ajax['view_dom_id']
            
                # ufc_api = api(view_args, view_path, view_dom_id)
                ufc_api = api(url)


                
                
                test_lst = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19]
            
                total_fights = 0
                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                    for result in executor.map(ufc_api.get_responsev3, test_lst):
                        if self.kill:
                            
                            break
                        amount_fights = result
                        total_fights += amount_fights
                        if amount_fights == 0:
                            self.kill = True
                            

                        
                    
                        

                        
                        # # print(result)
                
                self.kill = False
                self.data["Fights"] = total_fights
                    
                
                
                
            except Exception as e:
                print(e)
        
        
        



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
        
        
    


    def format_list(self, lst):
        new_lst = []
        for i in range(0, len(lst), 2):
            key = lst[i + 1]
            value = lst[i]
            new_lst.append({key:value})
        return new_lst

    
    def get_accuracy_stats(self, response):
        accuracy_list = response.css(".c-overlap__stats-value::text , .c-overlap__stats-text::text").extract()
        percent_list = response.css("text.e-chart-circle__percent::text").extract()
        if len(percent_list) != 2:
            return False
        

        percent = percent_list[1]
        percent = percent.replace("%", "")
        takedowns_percent = int(percent) / 100
        if len(accuracy_list) != 8:
           accuracy_list = self.fix_response_lists(accuracy_list)
           

        accuracy_dict = self.list_to_dict(accuracy_list)
        

        if accuracy_dict["Takedowns Landed"] == "":
            accuracy_dict["Takedowns Landed"] = round(takedowns_percent * int(accuracy_dict["Takedowns Attempted"]))
            

        
        self.add_items(accuracy_dict, "accuracy stats")
        return True
        
    def fix_response_lists(self, lst):
        updated_list = []
        final = ""
        for curr, next in self.pairwise(lst):

            final = next
            temp_next = next.replace(" ", "")
            temp_curr = curr.replace(" ", "")
            next_alpha = temp_next.isalpha()
            curr_alpha = temp_curr.isalpha()

            if next_alpha and curr_alpha:
                updated_list.append(curr)
                updated_list.append("")
            else:
                updated_list.append(curr)
                
        updated_list.append(final)
        if(final.isalpha()):
            updated_list.append("0")

        return updated_list

    def pairwise(self, iterable):
        a,b = itertools.tee(iterable)
        next(b,None)
        return zip(a,b)
    
    def add_items(self, dict, func):
        # print(f"From Function: {func}")
        # print(f"Current Dictionary: {dict}")
        for label, value in dict.items():
            new_label = label.strip()
            new_label = new_label.replace(" ", "_")
            new_label = new_label.replace(".", "")
            new_label = new_label.replace("/","_")
            # print(f"New Label being added: {new_label}")
            self.data[new_label] = value

    
    def get_misc_stats(self, response):
        misc_values = response.css('.c-stat-3bar__value::text').extract()
        misc_labels = response.css('.c-stat-3bar__label::text').extract()

        misc_dict = self.lists_to_dict(misc_labels, misc_values)
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



    def combine_lists(self, list1, list2):
        new_list = []
        for v1, v2 in enumerate(zip(list1, list2)):
            new_list.append(v2)
            new_list.append(v1)
            


    def get_base_stats(self,response):
       
        # comparison_list = response.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "c-stat-compare__group", " " ))]//text()').extract()
        comparison_list = response.css(".c-stat-compare__label::text , .c-stat-compare__number::text").extract()
        
        

        cleaned = []
        for data in comparison_list:
            clean_data = data.strip()
            cleaned.append(clean_data)
            # if clean_data != '' and clean_data != 'Per Min' and clean_data != 'Per 15 Min' and clean_data != '%':
            #     cleaned_comp_list.append(clean_data)
        
       

        if len(cleaned) == 15:
            cleaned = self.fix_base_lists(cleaned)

                           
        if len(cleaned) == 16:
            cleaned = self.swap(cleaned)
            comp_dict = self.list_to_dict(cleaned)
            self.add_items(comp_dict, "base stats")
            return True
        else:
            return False
        
       
        
    def fix_base_lists(self, cleaned):
        updated_list = []
        final = ""
        for i in range(0, len(cleaned) - 1):
            final = cleaned[i+1]
            temp_curr = cleaned[i].replace(" ", "")
            temp_curr = temp_curr.replace(".", "")
            temp_next = cleaned[i+1].replace(" ", "")
            temp_next = temp_next.replace(".", "")
           

            if temp_curr.isalpha() and temp_next.isalpha():
                if temp_next == "TakedownDefense":
                    updated_list.append(cleaned[i])
                    updated_list.append("")
                else:
                    updated_list.append(cleaned[i])
            else:
                updated_list.append(cleaned[i])
         
        updated_list.append(final)

        return updated_list

    def swap(self, lst):
        for i in range(0, len(lst) - 1, 2):
            lst[i], lst[i + 1] = lst[i + 1], lst[i]
        return lst

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
       
        
        self.add_items(bio_dict, "bio")
        
        


    

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

    def create_schemas(self):

        

        schema_fact = StructType([ \
        StructField("Bio ID",IntegerType(),True), \
        StructField("Sig. Strikes Landed",StringType(),True), \
        StructField("Sig. Strikes Attempted",StringType(),True), \
        StructField("Sig. Strikes Landed Per Min", StringType(), True), \
        StructField("Sig. Strikes Absorbed Per Min", StringType(), True), \
        StructField("Sig. Strike Defense", StringType(), True), \
        StructField("Knockdown Average",StringType(),True), \
        StructField("Sig. Strikes Standing",StringType(),True), \
        StructField("Sig. Strikes Clinch",StringType(),True), \
        StructField("Sig. Strikes Ground", StringType(), True), \
        StructField("Sig. Strikes Head", StringType(), True), \
        StructField("Sig. Strikes Body", StringType(), True), \
        StructField("Sig. Strikes Leg",StringType(),True), \
        StructField("Takedowns Landed",StringType(),True), \
        StructField("Takedowns Attempted", StringType(), True), \
        StructField("Takedown Average", StringType(), True), \
        StructField("Takedown Defense", StringType(), True), \
        StructField("Submission Average", StringType(), True), \
        StructField("KO/TKO",StringType(),True), \
        StructField("DEC",StringType(),True), \
        StructField("SUB", StringType(), True), \
        StructField("Reach", StringType(), True), \
        StructField("Leg Reach", StringType(), True), \
        StructField("Age", StringType(), True), \
        StructField("Height", StringType(), True), \
        StructField("Average Fight Time", StringType(), True), \
        StructField("Number of Fights", IntegerType(), True) \
            ])     

        schema_bio = StructType([ \
        StructField("Bio ID",IntegerType(),True), \
        StructField("First Name",StringType(),True), \
        StructField("Last Name",StringType(),True), \
        StructField("Division", StringType(), True), \
        StructField("Status", StringType(), True), \
        StructField("Hometown", StringType(), True), \
        StructField("Fighting Style",StringType(),True), \
        StructField("Trains At",StringType(),True), \
        StructField("Octagon Debut",StringType(),True) \
            ])

        return schema_fact, schema_bio
    
    def reset_data(self):
    
        data = {
            "Bio_ID": self.current_id,
            "first_name":"",
            "last_name":"",
            "Division":"",
            "Status":"",
            "Place_of_Birth":"",
            "Fighting_style":"",
            "Trains_at":"",
            "Octagon_Debut":"",

            "Sig_Strikes_Landed": "",
            "Sig_Strikes_Attempted":"",
            "Sig_Str_Landed": "",
            "Sig_Str_Absorbed": "",
            "Sig_Str_Defense": "",
            "Knockdown_Avg": "",
            "Standing":"",
            "Clinch":"",
            "Ground":"",
            "Sig_Str_Head":"",
            "Sig_Str_Body":"",
            "Sig_Str_Leg":"",
            "Takedowns_Landed":"",
            "Takedowns_Attempted":"",
            "Takedown_avg":"",
            "Takedown_Defense":"",
            "Submission_avg":"",
            "KO_TKO":"",
            "DEC":"",
            "SUB":"",
            "Reach":"",
            "Leg_reach":"",
            "Age":"",
            "Height":"",
            "Average_fight_time":"",
            "Fights":0
        }
        

       


        return data

    def seperate_tables(self, data):
        bio_data = dict(itertools.islice(data.items(), 0, 9))
        length = len(data.items())
        fact_data = dict(itertools.islice(data.items(), 9, length))
        fact_data["Bio_ID"] = bio_data["Bio_ID"]
        
        return bio_data, fact_data





    

            
    

        
        







        
