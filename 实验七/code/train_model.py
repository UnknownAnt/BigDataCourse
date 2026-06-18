import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_validate
import joblib
import numpy as np

# 1. Load Data
# Sample 50000 rows
df = pd.read_csv('user_behavior_100M.csv', names=['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp'], nrows=200000)

# Create label: 1 if behavior_type == 'buy', else 0
df['label'] = (df['behavior_type'] == 'buy').astype(int)

# Positive samples might be rare, let's balance or just use it if we have enough.
pos_samples = df[df['label'] == 1]
neg_samples = df[df['label'] == 0].sample(n=len(pos_samples)*5, random_state=42) # 1:5 ratio
train_df = pd.concat([pos_samples, neg_samples]).sample(frac=1, random_state=42).reset_index(drop=True)

# Limit to 50000 if larger
if len(train_df) > 50000:
    train_df = train_df.sample(n=50000, random_state=42)

print(f"Training samples: {len(train_df)}")
print(f"Positive samples: {train_df['label'].sum()} ({train_df['label'].sum()/len(train_df)*100:.2f}%)")

# Extract features
train_df['timestamp'] = pd.to_datetime(train_df['timestamp'], unit='s')
train_df['hour'] = train_df['timestamp'].dt.hour
train_df['dayofweek'] = train_df['timestamp'].dt.dayofweek

X = train_df[['category_id', 'hour', 'dayofweek']]
y = train_df['label']

# 2. Build Pipeline
numeric_features = ['hour', 'dayofweek']
numeric_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler())
])

categorical_features = ['category_id']
categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('onehot', OneHotEncoder(handle_unknown='ignore'))
])

preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features),
        ('cat', categorical_transformer, categorical_features)
    ])

clf = RandomForestClassifier(n_estimators=500, max_depth=15, random_state=42, n_jobs=-1)

pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                           ('classifier', clf)])

# 3. Cross-validation
print("Running 5-fold cross-validation...")
cv_results = cross_validate(pipeline, X, y, cv=5, scoring=['accuracy', 'roc_auc'])
print(f"Average Accuracy: {np.mean(cv_results['test_accuracy']):.4f}")
print(f"Average AUC: {np.mean(cv_results['test_roc_auc']):.4f}")

# 4. Train on full data and save
print("Training final model...")
pipeline.fit(X, y)
joblib.dump(pipeline, 'model.pkl')
import os
print(f"model.pkl size: {os.path.getsize('model.pkl') / 1024 / 1024:.2f} MB")

# 5. Validation
print("Validating saved model on 3 samples...")
loaded_model = joblib.load('model.pkl')
sample_X = X.head(3)
preds = loaded_model.predict(sample_X)
probs = loaded_model.predict_proba(sample_X)
for i in range(3):
    print(f"Sample {i}: Pred={preds[i]}, Prob={probs[i]}")
