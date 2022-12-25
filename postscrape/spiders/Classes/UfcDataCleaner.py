

class UfcDataCleaner:

    def __init__(self) -> None:
        pass
        # self.attributes = ['Sig_Strikes_Landed', 'Sig_Strikes_Attempted', 'Sig_Str_Landed', 'Sig_Str_Absorbed','Sig_Str_Defense','Knockdown_Avg'
        #                     ,'Standing', 'Clinch', 'Ground', 'Sig_Str_Head', 'Sig_Str_Body', 'Sig_Str_Leg', 'Takedowns_Landed', 'Takedowns_Attempted'
        #                     ,'Takedown_avg', 'Takedown_Defense', 'Submission_avg','KO_TKO', 'DEC', 'SUB', 'Average_fight_time', 'Status'
        #                     ,'Hometown', 'Fighting_style', 'Trains_at', 'Age', 'Height', 'Weight', 'Octagon_Debut', 'Reach', 'Leg_reach'
        #                     ,'Division','first_name', 'last_name']
    def clean_data(self, athlete_info):
        # print(f"This is the data: {athlete_info}")

        if 'Weight' in athlete_info:
            del athlete_info['Weight']

        for dataK, dataV in athlete_info.items():

            data = dataV
            if isinstance(dataV, str):
                data = data.strip()
                if '%' in data:
                    data_list = data.split(" ")
                    data = data_list[0]
           
            if dataK == 'Average_fight_time':
                (m, s) = dataV.split(':')
                result = int(m) * 60 + int(s)
                data = str(result / 60)
            
        

            
            athlete_info[dataK] = data


        # for attribute in self.attributes:
        #     if not attribute in athlete_info:
        #         athlete_info[attribute] = 'N/A'
        athlete_info = self.data_to_tuple(athlete_info)
        
        return athlete_info

    def data_to_tuple(self, athlete_info):
        
        return_values = []
        for data in athlete_info.values():
            return_values.append(data)

        if not isinstance(return_values[0], int):
            return_values.insert(0, return_values.pop())
        
        
        return tuple(return_values)

        
   