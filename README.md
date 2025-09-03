IMDB Query Execution Time Prediction

This project is all about predicting how long a SQL query will take to run, without actually executing it. Using the IMDB dataset as a foundation, I built a complete machine learning pipeline that cleans messy raw data, engineers meaningful features, and trains multiple models to forecast execution time with high accuracy.

Working with the IMDB dataset wasn’t easy — the raw files were massive (tens of millions of rows), filled with encoding errors, oversized columns, and malformed rows. I created a custom preprocessing pipeline to clean the TSV files, handle missing values, split the data into smaller chunks to avoid memory issues, and log-transform the target variable to reduce skewness. I also used outlier detection methods (IQR and Z-score) to make the dataset more robust and reliable.

Once the data was clean, I experimented with several machine learning models, including Random Forest, Gradient Boosting, XGBoost, Linear Regression, and SVR. Each model was tuned with GridSearchCV to get the best hyperparameters. For evaluation, I didn’t just rely on standard metrics like MSE, RMSE, MAE, and R² — I also introduced tolerance-based accuracy (Acc@10%, 20%, 30%), which measures how often predictions fall within a reasonable margin of the actual runtime. This gave me a more practical view of how well the models perform in real-world scenarios.

In the end, XGBoost stood out as the most effective model, consistently delivering strong results across validation and test sets. The final model was saved with joblib so it can be reused without retraining.

You can explore the full workflow — from data cleaning to model evaluation — in the Google Colab notebook
.

This project demonstrates how combining careful preprocessing, thoughtful evaluation, and modern machine learning techniques can help predict query runtimes efficiently. It’s a step toward smarter database performance optimization without the overhead of actually running costly queries.
