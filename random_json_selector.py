import json
import random
from abc import ABC
import numpy as np

class RandomJSONSelector(ABC):

    @staticmethod
    def get_normalized_rnd_integer(n):
        """
            Returns a random integer based on a normal distribution.
            The integer is clipped to the range of [0, n - 1].
            Args:
                n (int): The upper bound of the range.
            Returns:
                int: A random integer in the range of [0, n - 1].
        """
        mean = n / 2
        std_dev = n / 4
        dim_size = 1
        random_integer = np.clip(np.round(np.random.normal(mean, std_dev, dim_size)).astype(int), 0, n - 1)
        return random_integer[0]
    
    @staticmethod
    def load_list(filename):
        """
            Loads the products from the json file.
            It now shuffles the list before returning it.
            Args:
                filename (str): The name of the json file.
            Returns:
                list: A list of json objects as dict.
        """
        with open(filename, "r") as file:
            json_list = json.load(file)
        
        random.shuffle(json_list)
        return json_list