

class UfcDataCleaner:


    def clean_data(self, athlete_info):

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

        athlete_info = self.data_to_tuple(athlete_info)
        
        return athlete_info

    def data_to_tuple(self, athlete_info):
        
        return_values = []
        for data in athlete_info.values():
            return_values.append(data)

        if not isinstance(return_values[0], int):
            return_values.insert(0, return_values.pop())
        
        
        return tuple(return_values)

        
   