from pyspark.sql import DataFrame
from abc import ABC, abstractmethod

class BaseAnalysis(ABC):
    
    @abstractmethod
    def get_results(self):
        pass
    
    def save_results(self, file_path):
        data: DataFrame = self.data
        data.write.mode('overwrite').csv(file_path, header=True, mode='overwrite')
