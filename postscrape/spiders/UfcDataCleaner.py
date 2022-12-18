

class UfcDataCleaner:

    def __init__(self) -> None:
        self.attributes = ['Sig_Strikes_Landed', 'Sig_Strikes_Attempted', 'Sig_Str_Landed', 'Sig_Str_Absorbed','Sig_Str_Defense','Knockdown_Avg'
                            ,'Standing', 'Clinch', 'Ground', 'Sig_Str_Head', 'Sig_Str_Body', 'Sig_Str_Leg', 'Takedowns_Landed', 'Takedowns_Attempted'
                            ,'Takedown_avg', 'Takedown_Defense', 'Submission_avg','KO_TKO', 'DEC', 'SUB', 'Average_fight_time', 'Status'
                            ,'Hometown', 'Fighting_style', 'Trains_at', 'Age', 'Height', 'Weight', 'Octagon_Debut', 'Reach', 'Leg_reach'
                            ,'Division','first_name', 'last_name']
    def clean_data(self, athlete_info):
        
        for dataK, dataV in athlete_info.items():
            

            data = dataV.strip()
           
            if dataK == 'Average_fight_time':
                (m, s) = dataV.split(':')
                result = int(m) * 60 + int(s)
                data = str(result / 60)
            if '%' in data:
                data_list = data.split(" ")
                data = data_list[0]
            
            athlete_info[dataK] = data

        for attribute in self.attributes:
            if not attribute in athlete_info:
                athlete_info[attribute] = 'N/A'

        return athlete_info
        
