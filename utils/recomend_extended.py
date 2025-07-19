import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.neighbors import NearestNeighbors
from sklearn.model_selection import train_test_split
from typing import Tuple, List, Dict, Set
from multiprocessing import Pool
from functools import partial
import os
import numba


class RecommenderSystem:
    def __init__(self, n_neighbors: int = 10):
        """
        Initialize the recommender system with multiple approaches

        Parameters:
        -----------
        n_neighbors : int
            Number of neighbors for KNN-based approaches
        """
        self.n_neighbors = n_neighbors
        self.user_item_matrix = None
        self.sparse_matrix = None
        self.item_similarity = None
        self.user_similarity = None
        self.user_knn = None
        self.item_knn = None

    def fit(self, ratings_df: pd.DataFrame, user_col: str = 'user_id',
            item_col: str = 'book_id', rating_col: str = 'rating'):
        """
        Fit the recommender system with rating data
        """
        # Create user-item matrix
        self.user_item_matrix = ratings_df.pivot_table(
            index=user_col,
            columns=item_col,
            values=rating_col
        ).fillna(0)

        # Create sparse matrix
        self.sparse_matrix = csr_matrix(self.user_item_matrix.values)

        # Compute similarity matrices
        self.user_similarity = cosine_similarity(self.sparse_matrix)
        self.item_similarity = cosine_similarity(self.sparse_matrix.T)

        # Fit KNN models
        self.user_knn = NearestNeighbors(n_neighbors=self.n_neighbors, metric='cosine')
        self.item_knn = NearestNeighbors(n_neighbors=self.n_neighbors, metric='cosine')

        self.user_knn.fit(self.sparse_matrix)
        self.item_knn.fit(self.sparse_matrix.T)

        return self

    def recommend_by_user_cf(self, user_id: int, n_recommendations: int = 10,
                             min_similarity: float = 0.0) -> pd.Series:
        """
        Generate recommendations using user-based collaborative filtering
        """

        if user_id not in self.user_item_matrix.index:
            raise KeyError(f"User {user_id} not found in the dataset")

        # Get user's ratings and similarities
        user_idx = self.user_item_matrix.index.get_loc(user_id)
        user_similarities = self.user_similarity[user_idx]

        # Filter similar users
        similar_users = user_similarities > min_similarity
        if not any(similar_users):
            return pd.Series()

        # Calculate weighted ratings
        weighted_ratings = np.zeros(self.user_item_matrix.shape[1])
        similarity_sums = np.zeros(self.user_item_matrix.shape[1])

        for other_idx in range(len(self.user_item_matrix)):
            if other_idx != user_idx and similar_users[other_idx]: # need to be T AND T
                similarity = user_similarities[other_idx]
                ratings = self.user_item_matrix.iloc[other_idx].values

                weighted_ratings += similarity * ratings
                similarity_sums += np.abs(similarity) * (ratings != 0)

        # Avoid division by zero
        similarity_sums[similarity_sums == 0] = 1e-8
        recommendations = weighted_ratings / similarity_sums

        # Convert to series
        recommendations = pd.Series(
            recommendations,
            index=self.user_item_matrix.columns
        )

        # Filter out already rated items
        user_ratings = self.user_item_matrix.loc[user_id]
        recommendations[user_ratings > 0] = -1

        return recommendations.nlargest(n_recommendations)

    def recommend_by_item_cf(self, user_id: int, n_recommendations: int = 10,
                             min_similarity: float = 0.0) -> pd.Series:
        """
        Generate recommendations using item-based collaborative filtering
        """
        if user_id not in self.user_item_matrix.index:
            raise KeyError(f"User {user_id} not found in the dataset")

        # Get user's ratings
        user_ratings = self.user_item_matrix.loc[user_id]
        rated_items = user_ratings[user_ratings > 0] # if rating already

        if len(rated_items) == 0:
            return pd.Series()

        # Calculate weighted ratings
        weighted_sum = np.zeros(len(self.user_item_matrix.columns))
        similarity_sum = np.zeros(len(self.user_item_matrix.columns))

        for item_id, rating in rated_items.items():
            item_idx = self.user_item_matrix.columns.get_loc(item_id)
            similarities = self.item_similarity[item_idx]

            # Apply similarity threshold
            similarities[similarities < min_similarity] = 0

            weighted_sum += similarities * rating
            similarity_sum += np.abs(similarities)

        # Avoid division by zero
        similarity_sum[similarity_sum == 0] = 1e-8
        recommendations = weighted_sum / similarity_sum

        # Convert to series
        recommendations = pd.Series(
            recommendations,
            index=self.user_item_matrix.columns
        )


        # Filter out already rated items
        recommendations[user_ratings > 0] = -1

        return recommendations.nlargest(n_recommendations)

    def recommend_by_user_knn(self, user_id: int, n_recommendations: int = 10) -> pd.Series:
        """
        Generate recommendations using KNN-based user similarity
        """
        if user_id not in self.user_item_matrix.index:
            raise KeyError(f"User {user_id} not found in the dataset")

        user_idx = self.user_item_matrix.index.get_loc(user_id)
        user_vector = self.sparse_matrix[user_idx:user_idx + 1] # cause 2D

        # Find nearest neighbors
        distances, indices = self.user_knn.kneighbors(user_vector)

        # Calculate weighted ratings
        weights = 1 / (distances.flatten() + 1e-8)
        weighted_sum = np.zeros(self.user_item_matrix.shape[1])
        weight_sum = np.zeros(self.user_item_matrix.shape[1])

        for idx, weight in zip(indices.flatten(), weights):
            weighted_sum += weight * self.user_item_matrix.iloc[idx].values
            weight_sum += weight  # like similarity

        # Avoid division by zero
        weight_sum[weight_sum == 0] = 1e-8
        recommendations = weighted_sum / weight_sum

        # Convert to series
        recommendations = pd.Series(
            recommendations,
            index=self.user_item_matrix.columns
        )

        # Filter out already rated items
        user_ratings = self.user_item_matrix.loc[user_id]
        recommendations[user_ratings > 0] = -1

        return recommendations.nlargest(n_recommendations)

    def recommend_by_item_knn(self, user_id: int, n_recommendations: int = 10) -> pd.Series:
        """
        Generate recommendations using KNN-based item similarity
        """
        if user_id not in self.user_item_matrix.index:
            raise KeyError(f"User {user_id} not found in the dataset")

        # Get user's ratings
        user_ratings = self.user_item_matrix.loc[user_id]
        rated_items = user_ratings[user_ratings > 0]

        if len(rated_items) == 0:
            return pd.Series()

        # Calculate recommendations for each rated item
        recommendations = np.zeros(len(self.user_item_matrix.columns))
        weights = np.zeros(len(self.user_item_matrix.columns))

        for item_id, rating in rated_items.items():
            item_idx = self.user_item_matrix.columns.get_loc(item_id)
            item_vector = self.sparse_matrix.T[item_idx:item_idx + 1]

            # Find nearest neighbors
            distances, indices = self.item_knn.kneighbors(item_vector)
            neighbor_weights = 1 / (distances.flatten() + 1e-8)

            for idx, weight in zip(indices.flatten(), neighbor_weights):
                recommendations[idx] += weight * rating
                weights[idx] += weight

        # Avoid division by zero
        weights[weights == 0] = 1e-8
        recommendations = recommendations / weights

        # Convert to series
        recommendations = pd.Series(
            recommendations,
            index=self.user_item_matrix.columns
        )

        # Filter out already rated items
        recommendations[user_ratings > 0] = -1

        return recommendations.nlargest(n_recommendations)

    import os
    import numpy as np
    import pandas as pd
    from multiprocessing import Pool
    from typing import List, Dict, Set

    def process_user(recommender_instance, user_id: int, test_data: pd.DataFrame, k_values: List[int]) -> Dict:
        """
        Process a single user for recommendation evaluation

        Parameters:
        - recommender_instance: The recommender system instance
        - user_id: User to evaluate
        - test_data: DataFrame with test interactions
        - k_values: List of k values for precision calculation

        Returns:
        - Dictionary of precision metrics for the user
        """
        # Store precision results for this user
        user_precisions = {}

        # Check if user exists in user item matrix
        if user_id not in recommender_instance.user_item_matrix.index:
            return user_precisions

        # Get actual items from test set for this user
        actual_items = set(test_data[test_data['user_id'] == user_id]['book_id'])

        # Skip if no actual items
        if len(actual_items) == 0:
            return user_precisions

        # Compute precisions for different k values and recommendation methods
        for k in k_values:
            try:
                # Generate recommendations for different methods
                rec_methods = {
                    'cf_user': set(recommender_instance.recommend_by_user_cf(user_id, k).index),
                    'cf_item': set(recommender_instance.recommend_by_item_cf(user_id, k).index),
                    'knn_user': set(recommender_instance.recommend_by_user_knn(user_id, k).index),
                    'knn_item': set(recommender_instance.recommend_by_item_knn(user_id, k).index)
                }

                # Calculate precision for each method
                for method_name, recommendations in rec_methods.items():
                    precision_key = f'precision@{k}_{method_name}'
                    user_precisions[precision_key] = len(recommendations & actual_items) / k

            except Exception as e:
                # Log or handle exceptions
                print(f"Error processing user {user_id}: {e}")
                continue

        return user_precisions

    def evaluate(self, test_data: pd.DataFrame, k_values: List[int] = [5, 10]) -> Dict:

        """
        Evaluate the recommender system using various metrics
        """
        # Add more comprehensive error checking
        if test_data.empty:
            raise ValueError("Test data is empty")

        if not all(col in test_data.columns for col in ['user_id', 'book_id']):
            raise ValueError("Test data missing required columns")

        metrics = {}

        for k in k_values:
            precision_cf_user = []
            precision_cf_item = []
            precision_knn_user = []
            precision_knn_item = []

            for user_id in test_data['user_id'].unique():
                if user_id in self.user_item_matrix.index:
                    # Get actual items from test set
                    actual_items = set(test_data[test_data['user_id'] == user_id]['book_id'])

                    if len(actual_items) > 0:
                        # Get recommendations from each method
                        try:
                            rec_cf_user = set(self.recommend_by_user_cf(user_id, k).index)
                            rec_cf_item = set(self.recommend_by_item_cf(user_id, k).index)
                            rec_knn_user = set(self.recommend_by_user_knn(user_id, k).index)
                            rec_knn_item = set(self.recommend_by_item_knn(user_id, k).index)

                            # Calculate precision for each method
                            precision_cf_user.append(len(rec_cf_user & actual_items) / k)  # Set Intersect
                            precision_cf_item.append(len(rec_cf_item & actual_items) / k)
                            precision_knn_user.append(len(rec_knn_user & actual_items) / k)
                            precision_knn_item.append(len(rec_knn_item & actual_items) / k)
                        except:
                            continue

            # Store metrics
            metrics[f'precision@{k}_cf_user'] = np.mean(precision_cf_user)
            metrics[f'precision@{k}_cf_item'] = np.mean(precision_cf_item)
            metrics[f'precision@{k}_knn_user'] = np.mean(precision_knn_user)
            metrics[f'precision@{k}_knn_item'] = np.mean(precision_knn_item)

        return metrics

# usage
def run_recommendation_analysis(ratings_path: str, frac=0.005, test_size=0.2, n_neighbors=10,
                                n_recommendations=10):
    """
    Run complete recommendation analysis
    """
    # Load and prepare data
    df = pd.read_csv(ratings_path)
    df = df[df['user_id'] <= 1000]
    df_sample = df.sample(frac=frac, random_state=42).reset_index(drop=True)
    train_data, test_data = train_test_split(df_sample, test_size=test_size, random_state=42)

    print(f"Original data shape: {df.shape}")
    print(f"Columns: {df.columns.values}")
    print(f"Sampling data shape: {df_sample.shape}")

    # Initialize and fit recommender
    recommender = RecommenderSystem(n_neighbors=n_neighbors)
    recommender.fit(train_data)

    # Get a random user for demonstration
    random_user = np.random.choice(recommender.user_item_matrix.index)
    # random_user = 7961
    print(f"\nGenerating recommendations for user_id: {random_user}")

    # Generate recommendations using all methods
    rec_cf_user = recommender.recommend_by_user_cf(random_user, n_recommendations)
    rec_cf_item = recommender.recommend_by_item_cf(random_user, n_recommendations)
    rec_knn_user = recommender.recommend_by_user_knn(random_user, n_recommendations)
    rec_knn_item = recommender.recommend_by_item_knn(random_user, n_recommendations)

    print("\nUser-based CF recommendations:", rec_cf_user.index.tolist())
    print("Item-based CF recommendations:", rec_cf_item.index.tolist())
    print("User KNN recommendations:", rec_knn_user.index.tolist())
    print("Item KNN recommendations:", rec_knn_item.index.tolist())

    # Evaluate all methods
    metrics = recommender.evaluate(test_data, k_values=[5, 10, 15])
    print("\nEvaluation Metrics:")
    for metric, value in metrics.items():
        print(f"{metric}: {value:.4f}")

    return recommender, metrics


# Usage example
if __name__ == "__main__":
    path = "https://drive.google.com/uc?id=1HDPOyxM6cs1SDx4boqKGrRVQam1VEPfy"
    run_recommendation_analysis(path)