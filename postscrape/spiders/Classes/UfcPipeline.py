from pyspark.sql.functions import *
from IPython.display import display
class UfcPipeline:
    

    
    def send_to_csv(self, fact_df, bio_df):
        fact_df, bio_df = self.filter(fact_df, bio_df)

        # fact_df.show(vertical=True)
        # bio_df.show(vertical=True)
        display(fact_df.toPandas())
        display(bio_df.toPandas())
        fact_df.toPandas().to_csv("fact.csv", index=False)
        bio_df.toPandas().to_csv("bio.csv", index=False)

        
    

    def filter(self, fact_df, bio_df):
        new_fact = fact_df.filter(fact_df.Number_of_Fights > 0)
        new_fact = new_fact.alias('fact_df')
        bio_df = bio_df.alias('bio_df')
        bio_df = new_fact.join(bio_df, new_fact.ID == bio_df.ID)\
                .select(\
                        new_fact.ID, \
                        bio_df.First_Name, \
                        bio_df.Last_Name, \
                        bio_df.Division, \
                        bio_df.Status, \
                        bio_df.Hometown, \
                        bio_df.Fighting_Style, \
                        bio_df.Trains_At, \
                        bio_df.Octagon_Debut)
                    
                    
        return new_fact, bio_df
           

            
    