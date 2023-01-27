import pandas as pd

def transform_data():
    
    df = pd.read_csv(f"data/raw_data.csv")

    filtered_df = df[df["Fights"] > 0]
    filtered_df = filtered_df[filtered_df["Sig_Strikes_Attempted"] > 0]
    
    filtered_df.to_csv(f"data/data.csv",index=False)
        
    