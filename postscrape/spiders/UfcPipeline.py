import csv
from collections import OrderedDict

class UfcPipeline:
    
    def __init__(self) -> None:
        # self.heading = ["First Name", "Last Name", "Division", "Age", "Status", "Height", "Weight", "Octagon Debut", "Reach", "Leg Reach", "Fighting Style"
        #                 ,"Trains At", "Hometown", "Sig. Strikes Landed", "Sig. Strikes Attempted", "Sig. Strikes Landed Per Min", "Sig. Strikes Absorbed Per Min"
        #                 ,"Sig. Strike Defense", "Knockdown Average Per Fight", "Sig. Strikes Standing", "Sig. Strikes Clinch", "Sig. Strikes Ground"
        #                 ,"Sig. Strikes Head", "Sig. Strikes Body", "Sig. Strikes Leg", "Takedowns Landed", "Takedowns Attempted", "Takedown Average Per Fight", "Takedown Defense"
        #                 ,"KO/TKO", "DEC", "SUB", "Submission Average Per 15", "Average Fight Time"]
        self.fact_table_heading = ["Bio ID", "Sig. Strikes Landed", "Sig. Strikes Attempted", "Sig. Strikes Landed Per Min", "Sig. Strikes Absorbed Per Min"
                            ,"Sig. Strike Defense", "Knockdown Average", "Sig. Strikes Standing", "Sig. Strikes Clinch", "Sig. Strikes Ground", "Sig. Strikes Head"
                            ,"Sig. Strikes Body", "Sig. Strikes Leg", "Takedowns Landed", "Takedowns Attempted", "Takedown Average", "Takedown Defense", "Submission Average"
                            ,"KO/TKO","DEC","SUB","Reach","Leg Reach", "Average Fight Time","Age", "Height","Number of Fights"]
        self.bio_table_heading = ["Bio ID", "First Name", "Last Name", "Division", "Status", "Hometown", "Fighting Style", "Trains At", "Octagon Debut"]
    
        with open("Ufc-Fact-Table.csv", "w", newline='', encoding='utf-8-sig') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(self.fact_table_heading)
        with open("Ufc-Bio-Table.csv", "w", newline='', encoding='utf-8-sig') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(self.bio_table_heading)

        fact_table = open("Ufc-Fact-Table.csv", "a", newline='', encoding='utf-8-sig')
        bio_table = open("Ufc-Bio-Table.csv", "a", newline='', encoding='utf-8-sig')
        self.fact_writer = csv.writer(fact_table)
        self.bio_writer = csv.writer(bio_table)
        self.surrogate_key = 1

    
    def send_to_csv(self, data):
        send_data = True
        if data['Division'] == 'Unknown':
            send_data = False
        elif data['Sig_Strikes_Attempted'] == 'N/A':
            send_data = False
        elif data['Height'] == 0 or data['Weight'] == 0:
            send_data = False

        if send_data:
            # fact_data = [data['first_name'], data['last_name'], data['Division'], data['Age'], data['Status'], data['Height'], data['Weight'], data['Octagon_Debut']
            #                 ,data['Reach'], data['Leg_reach'],data['Fighting_style'], data['Trains_at'], data['Hometown'], data['Sig_Strikes_Landed'], data['Sig_Strikes_Attempted']
            #                 ,data['Sig_Str_Landed'], data['Sig_Str_Absorbed'], data['Sig_Str_Defense'], data['Knockdown_Avg'], data['Standing'], data['Clinch'], data['Ground']
            #                 ,data['Sig_Str_Head'], data['Sig_Str_Body'], data['Sig_Str_Leg'], data['Takedowns_Landed'], data['Takedowns_Attempted'], data['Takedown_avg'], data['Takedown_Defense']
            #                 ,data['KO_TKO'], data['DEC'], data['SUB'], data['Submission_avg'], data['Average_fight_time']]
            
            fact_data = [self.surrogate_key, data['Sig_Strikes_Landed'], data['Sig_Strikes_Attempted'], data['Sig_Str_Landed'], data['Sig_Str_Absorbed'], data['Sig_Str_Defense'], data['Knockdown_Avg']
                        ,data['Standing'], data['Clinch'], data['Ground'], data['Sig_Str_Head'], data['Sig_Str_Body'], data['Sig_Str_Leg'], data['Takedowns_Landed'], data['Takedowns_Attempted']
                        ,data['Takedown_avg'], data['Takedown_Defense'],data['Submission_avg'], data['KO_TKO'], data['DEC'], data['SUB'], data['Reach'], data['Leg_reach'], data['Average_fight_time']
                        ,data['Age'] ,data['Height'] ,data['Fights']]
            bio_data = [self.surrogate_key, data['first_name'], data['last_name'], data['Division'], data['Status'], data['Hometown'], data['Fighting_style'], data['Trains_at'],data['Octagon_Debut']]
            try:
                self.surrogate_key +=1
                self.fact_writer.writerow(fact_data)
                self.bio_writer.writerow(bio_data)
            except:
                print("FAILED TO WRITE TO FILE")

            
    