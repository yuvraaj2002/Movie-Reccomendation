import pandas as pd
import sys
import os 
import requests
from src.logger import logging
from src.exception import CustomException
from sklearn.model_selection import train_test_split
from dataclasses import dataclass


@dataclass
class DataIngestionConfig:
    raw_storage_path = os.path.join('arfitacts','raw_data.csv')
    train_data_path = os.path.join('arfitacts','train_data.csv')
    test_data_path = os.path.join('arfitacts','test_data.csv')


class DataIngestion:

    def __init__(self):
        self.ingestion_config = DataIngestionConfig()  # Assign ingestion_config to the class attribute

    def initialize_data_ingestion(self):
        logging.info("Entered into data ingestion phase")

        try:
            headers = {
                "accept": "application/json",
                "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJiNjljMDBlNTJiNDBhYjU4NGFiYjdlMzA2OWJjN2U4NSIsInN1YiI6IjY0OTI4ZDllNGJhNTIyMDBlMjUyOTdmNyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.mbIkhif3M_XMvqdgkdh-fiyh36NjMafgoZffAc_Msp8"
            }

            df = pd.DataFrame()
            for i in range(1,429):
                base_url = "https://api.themoviedb.org/3/movie/top_rated?language=en-US&page="
                url = base_url + str(i)
                response = requests.get(url, headers=headers)
                temp_df = pd.DataFrame(response.json()['results'])[['id','title','overview','release_date','popularity','vote_average','vote_count']]
                df = pd.concat([temp_df, df], ignore_index=True, axis=0)

            logging.info("Created a dataframe by fetching data from API")

            # Creating a directory named artifacts
            os.makedirs(os.path.dirname(self.ingestion_config.raw_storage_path), exist_ok=True)
            logging.info("artifacts directory created")

            # Saving the dataframe as a CSV file
            df.to_csv(self.ingestion_config.raw_storage_path, index=False, header=True)  # Use the storage_path from DataIngestionConfig
            logging.info("Saved the raw data in raw_Data.csv file")


            # Let's now do train test split
            logging.info("Train test split initiated")
            # Split the dataframe into train and test sets
            train_set, test_set = train_test_split(df, test_size=0.2, random_state=42)

            # Saving the train and test data in csv files
            train_set.to_csv(self.ingestion_config.train_data_path, index=False, header=True)
            test_set.to_csv(self.ingestion_config.test_data_path,index=False, header=True)
            logging.info("Data ingestion process completed")

            # Return the paths to the train and test data files
            return (
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path
            )
        
        except Exception as e:
            raise CustomException(e, sys)


if __name__ == "__main__":
    obj = DataIngestion()
    # Return -> train, test data path
    train_data, test_data = obj.initialize_data_ingestion()


    # data_transformation = DataTransformation()
    # train_arr, test_arr, _ = data_transformation.initiate_data_transformation(
    #     train_data, test_data)

    # modeltrainer = ModelTrainer()
    # print(modeltrainer.initiate_model_trainer(train_arr, test_arr))