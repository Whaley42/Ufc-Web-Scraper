import csv

class UfcPipeline:
    
    def send_to_csv(self, fighter_data):
        """Creates a csv of all the fighter data

        Args:
            fighter_data dictionary: Dictionary of the fighter data.
        """
        data_file = open("data/raw_data.csv", "w")

        data_writer = csv.writer(data_file)

        count = 0
        for data in fighter_data.keys(): 
            temp_data = fighter_data[data]
            if count == 0:
                header = temp_data.keys()
                data_writer.writerow(header)
                count += 1
            data_writer.writerow(temp_data.values())

            



            
    