import pandas as pd
import numpy as np
from utils.recomend_extended import *

# Load data
path = "https://drive.google.com/uc?id=1HDPOyxM6cs1SDx4boqKGrRVQam1VEPfy"

# Get recommendations for a user
# random sampling = 0.8 or 80% of user_id <= 1,000 (dataset n=5,976,479 samples)
# User from random
frac = 0.8       # sampling data
test_size = 0.2         # for split train-test
n_neighbors = 50        # for k-nn
n_recommendations= 10   # number recommend items
run_recommendation_analysis(path, frac=frac, test_size=test_size, n_neighbors=n_neighbors,
                            n_recommendations=n_recommendations)
