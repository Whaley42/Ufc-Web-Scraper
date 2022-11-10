import csv
from collections import OrderedDict

class UfcPipeline:
    
    def __init__(self) -> None:
        self.heading = ["First Name", "Last Name", "Division", "Age", "Status", "Height", "Weight", "Octagon Debut", "Reach", "Leg Reach", "Fighting Style"
                        ,"Trains At", "Hometown", "Sig. Strikes Landed", "Sig. Strikes Attempted", "Sig. Strikes Landed Per Min", "Sig. Strikes Absorbed Per Min"
                        ,"Sig. Strike Defense", "Knockdown Average Per Fight", "Sig. Strikes Standing", "Sig. Strikes Clinch", "Sig. Strikes Ground"
                        ,"Sig. Strikes Head", "Sig. Strikes Body", "Sig. Strikes Leg", "Takedowns Landed", "Takedowns Attempted", "Takedown Average Per Fight", "Takedown Defense"
                        ,"KO/TKO", "DEC", "SUB", "Submission Average Per 15", "Average Fight Time"]
        print(len(self.heading))
        self.heading_sorted = sorted(self.heading)
        with open("test3.csv", "w", newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(self.heading)

        csv_file = open("test3.csv", "a", newline='')
        self.writer = csv.writer(csv_file)

    
    def send_to_csv(self, data):
        ordered_data = [data['first_name'], data['last_name'], data['Division'], data['Age'], data['Status'], data['Height'], data['Weight'], data['Octagon_Debut']
                        ,data['Reach'], data['Leg_reach'],data['Fighting_style'], data['Trains_at'], data['Hometown'], data['Sig_Strikes_Landed'], data['Sig_Strikes_Attempted']
                        ,data['Sig_Str_Landed'], data['Sig_Str_Absorbed'], data['Sig_Str_Defense'], data['Knockdown_Avg'], data['Standing'], data['Clinch'], data['Ground']
                        ,data['Sig_Str_Head'], data['Sig_Str_Body'], data['Sig_Str_Leg'], data['Takedowns_Landed'], data['Takedowns_Attempted'], data['Takedown_avg'], data['Takedown_Defense']
                        ,data['KO_TKO'], data['DEC'], data['SUB'], data['Submission_avg'], data['Average_fight_time']]
        print("---")
        print(len(ordered_data))
        
        print("---")
        self.writer.writerow(ordered_data)

            
    