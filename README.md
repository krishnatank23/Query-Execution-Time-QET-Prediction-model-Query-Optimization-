📊 IMDB Query Execution Time Prediction
🚀 Project Overview

This project predicts SQL query execution time without running the query, using the IMDB dataset. The aim is to provide smarter database performance optimization and query planning with the help of machine learning models.

🔧 Workflow
1. Data Preprocessing

Cleaned massive IMDB TSV files (tens of millions of rows).

Fixed encoding errors, oversized columns, and malformed rows.

Handled missing values and memory issues by chunking data.

Applied log transformation to target values for skewness reduction.

Removed outliers using IQR and Z-score methods.

2. Feature Engineering & Modeling

Built features that capture query structure and dataset statistics.

Models trained & tuned with GridSearchCV:

✅ Random Forest

✅ Gradient Boosting

✅ XGBoost

✅ Linear Regression

✅ Support Vector Regression (SVR)

3. Evaluation Strategy

Standard regression metrics: MSE, RMSE, MAE, R², Adjusted R².

Introduced tolerance-based accuracy (Acc@10%, 20%, 30%) to measure prediction reliability within error margins.

4. Results

XGBoost emerged as the best model, consistently delivering high accuracy across validation and test sets.
