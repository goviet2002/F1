import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.neighbors import KNeighborsRegressor
import xgboost as xgb

# Step 1: Create the dataset
data = {
    'Square Footage': [1500, 1800, 1200, 1600, 2000],
    'Bedrooms': [3, 4, np.nan, 3, 4],  # Missing value for House 3
    'Age of House': [10, 15, 5, np.nan, 20],  # Missing value for House 4
    'House Price ($)': [300000, 350000, 250000, 320000, 400000]
}

df = pd.DataFrame(data)

# Step 2: Separate features (X) and target (y)
X = df.drop(columns=['House Price ($)'])
y = df['House Price ($)']

# Step 3: Impute missing values using Random Forest for the 'Bedrooms' column
# First, we will split the data into the rows with missing values and the ones without.
X_train = X[X['Bedrooms'].notnull()]  # Rows where 'Bedrooms' is not missing
y_train = X_train['Bedrooms']         # 'Bedrooms' column (target for imputation)
X_train = X_train.drop(columns=['Bedrooms'])  # Features used to predict 'Bedrooms'

X_missing = X[X['Bedrooms'].isnull()]  # Rows where 'Bedrooms' is missing
X_missing = X_missing.drop(columns=['Bedrooms'])  # Features used for imputation

# Now, we will add the target variable (House Price) as an additional feature for the imputation
X_train['House Price ($)'] = y_train  # Adding 'House Price' to help predict 'Bedrooms'
X_missing['House Price ($)'] = y[X_missing.index]  # Adding 'House Price' for rows with missing 'Bedrooms'

# Now we can use RandomForest to impute the missing 'Bedrooms'
rf_imputer = RandomForestRegressor(n_estimators=100, random_state=42)
rf_imputer.fit(X_train, y_train)

# Predict the missing 'Bedrooms' values
predicted_bedrooms = rf_imputer.predict(X_missing)

# Fill in the missing 'Bedrooms' in the original data
X.loc[X['Bedrooms'].isnull(), 'Bedrooms'] = predicted_bedrooms

# Step 4: Now that the missing values are imputed, let's train the models for house price prediction
X_imputed = X
print(X_imputed)
# Model 1: Random Forest Regressor for House Price Prediction
rf = RandomForestRegressor(n_estimators=100, random_state=42)
rf.fit(X_imputed, y)

# Model 2: K-Nearest Neighbors Regressor for House Price Prediction
knn = KNeighborsRegressor(n_neighbors=2)
knn.fit(X_imputed, y)

# Model 3: XGBoost for House Price Prediction
dtrain = xgb.DMatrix(X_imputed, label=y)
params = {
    'objective': 'reg:squarederror',
    'eval_metric': 'rmse',
    'max_depth': 3,
    'eta': 0.1,
}
xgb_model = xgb.train(params, dtrain, num_boost_round=10)

# Step 5: Predict for House 3 (where 'Bedrooms' was initially missing)
house_3 = pd.DataFrame({
    'Square Footage': [1200],
    'Bedrooms': [3],  # Now filled
    'Age of House': [5],
    'House Price ($)': [250000]  # Include House Price as a feature
})

# Predict the house price for House 3 using the trained models
rf_prediction = rf.predict(house_3)
knn_prediction = knn.predict(house_3)
xgb_prediction = xgb_model.predict(xgb.DMatrix(house_3))

print(f"Random Forest Prediction for House 3 (Price): {rf_prediction}")
print(f"KNN Prediction for House 3 (Price): {knn_prediction}")
print(f"XGBoost Prediction for House 3 (Price): {xgb_prediction}")
