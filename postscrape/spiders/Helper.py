import itertools
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from bs4 import BeautifulSoup


class Helper:
    
    def __init__(self) -> None:
        self.current_id = 0

    def fix_accuracy_lists(self, lst):
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

    def pairwise(self, iterable):
        a,b = itertools.tee(iterable)
        next(b,None)
        return zip(a,b)

    def swap(self, lst):
        for i in range(0, len(lst) - 1, 2):
            lst[i], lst[i + 1] = lst[i + 1], lst[i]
        return lst

    def list_to_dict(self, data):

        value = iter(data)
        res_dct = dict(zip(value, value))
        
        return res_dct

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

    def seperate_tables(self, data):
        bio_data = dict(itertools.islice(data.items(), 0, 9))
        length = len(data.items())
        fact_data = dict(itertools.islice(data.items(), 9, length))
        fact_data["Bio_ID"] = bio_data["Bio_ID"]
        
        return bio_data, fact_data

    def fight_stats(self, response, name):
        soup = BeautifulSoup(response.text, "html.parser")
        total_fights = soup.find_all(class_='c-card-event--athlete-results__results')
        
        wins_blue = soup.select('.c-card-event--athlete-results__headline , .c-card-event--athlete-results__blue-image .win')
        wins_red = soup.select('.c-card-event--athlete-results__headline , .c-card-event--athlete-results__red-image .win')
        
        total_wins = 0
        if len(wins_blue) > 1:
            # print(f"Length of list: {len(wins_blue)}" )
            for i in range(1, len(wins_blue)):
                # print(f"Current index: {i}")
                # print(f"Length of blue list: {len(wins_blue)} Current index: {i}")
                curr = wins_blue[i].get_text().strip()
                
                prev = wins_blue[i-1].get_text().strip()
                
                if curr != "Win":

                    right_name = curr.split(" ")[2]
                    
                
                    if prev == "Win" and right_name == name:
                        total_wins += 1
        
        if len(wins_red) > 1:
            for i in range(1, len(wins_red)):
                # print(f"Length of red list {len(wins_red)} Current index: {i}")
                
                curr = wins_red[i].get_text().strip()
                prev = wins_red[i-1].get_text().strip()

                if curr != "Win":
                    
                    left_name = curr.split(" ")[0]
                
                
                    if prev == "Win" and left_name == name:
                        total_wins += 1
        return len(total_fights), total_wins

    def increment_id(self):
        self.current_id += 1

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
        StructField("Number of Fights", IntegerType(), True), \
        StructField("Wins", IntegerType(), True) \
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
            "Fights":0,
            "Wins":-1
        }
        

        


        return data