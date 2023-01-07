import datetime

class UfcDataCleaner:


    def clean_data(self, fighter_data):
        """Cleans the data so it can be properly stored and sent to AWS in a csv file.

        Args:
            fighter_data dictionary: Dictionary to be cleaned.

        Returns:
            dictionary: The cleaned dictionary.
        """

        del fighter_data['Weight']
        del fighter_data['Page']
        del fighter_data['fight_name']

        if "Octagon_Debut" in fighter_data:
            split = fighter_data["Octagon_Debut"].split(" ")
        
        if ";" in fighter_data["Trains_at"]:
            fighter_data["Trains_at"] = fighter_data["Trains_at"].replace(";","&")

        if fighter_data["Average_fight_time"]:
            (m, s) = fighter_data["Average_fight_time"].split(':')
            result = int(m) * 60 + int(s)
            data = str(round(result / 60, 2))
            fighter_data["Average_fight_time"] = data
            
        month = self.month_to_num(split[0])
        day = split[1].replace(",","")
        year = split[2]
        
        fighter_data["Octagon_Debut"] = datetime.date(int(year), int(month), int(day))

        for dataK, dataV in fighter_data.items():

            data = dataV
            if isinstance(dataV, str):
                data = data.strip()
                if '%' in data:
                    data_list = data.split(" ")
                    data = data_list[0] 
            
            fighter_data[dataK] = data
      
        return fighter_data
    

    def month_to_num(self, month):
        """Returns a number value based on the month.

        Args:
            month string: The month.

        Returns:
            int: Matching number for the month.
        """
        return {
            'Jan.': 1,
            'Feb.': 2,
            'Mar.': 3,
            'Apr.': 4,
            'May.': 5,
            'Jun.': 6,
            'Jul.': 7,
            'Aug.': 8,
            'Sep.': 9, 
            'Oct.': 10,
            'Nov.': 11,
            'Dec.': 12
    }[month]
        
   