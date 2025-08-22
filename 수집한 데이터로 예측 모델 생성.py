import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import rc
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor

# ==============================
# 0. 한글 폰트 설정
# ==============================
rc('font', family='Malgun Gothic')
plt.rcParams['axes.unicode_minus'] = False

# ==============================
# 1. 데이터 불러오기 & 결측치 처리
# ==============================
def preprocess_missing(df, drop_years=None, missing_values=[-99.9, -999]):
    if drop_years:
        df = df[~df['YEAR'].isin(drop_years)]
    df.replace(missing_values, np.nan, inplace=True)
    df.fillna(df.mean(), inplace=True)
    return df

merged_df = pd.read_csv("gamgyul_weather_merged.csv")
merged_df = preprocess_missing(merged_df, drop_years=[1995, 2012, 2021])

# ==============================
# 2. Feature / Target 정의
# ==============================
drop_cols = ['YEAR', '생산량(톤)', '면적(ha)', '조수입(백만원)', 
             '재배농가(호)', 'kg당가격(원)', '연평균지면온도(℃)']

X = merged_df.drop(columns=drop_cols)
y = np.log1p(merged_df['생산량(톤)'])

# ==============================
# 3. 상관계수 히트맵
# ==============================
def plot_corr_heatmap(df, figsize=(12,10), title="상관계수 히트맵"):
    plt.figure(figsize=figsize)
    sns.heatmap(df.corr(), cmap="coolwarm", center=0)
    plt.title(title)
    plt.show()

plot_corr_heatmap(X, title="특징 간 상관계수 히트맵")

# ==============================
# 4. 학습용/테스트용 데이터 분리
# ==============================
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# ==============================
# 5. 스케일링 (선형 모델용)
# ==============================
def scale_features(X_train, X_test):
    scaler = StandardScaler()
    return scaler.fit_transform(X_train), scaler.transform(X_test)

# ==============================
# 6. 모델 정의
# ==============================
models = {
    "LinearRegression": LinearRegression(),
    "Ridge": Ridge(alpha=1.0),
    "Lasso": Lasso(alpha=0.01),
    "ElasticNet": ElasticNet(alpha=0.01, l1_ratio=0.5),
    "RandomForest": RandomForestRegressor(n_estimators=500, max_depth=5, min_samples_split=10, min_samples_leaf=5, random_state=42, n_jobs=-1),
    "GradientBoosting": GradientBoostingRegressor(n_estimators=500, learning_rate=0.05, max_depth=3, random_state=42),
    "XGBoost": XGBRegressor(n_estimators=500, learning_rate=0.05, max_depth=3, random_state=42),
    "LightGBM": LGBMRegressor(n_estimators=500, learning_rate=0.05, max_depth=-1, random_state=42)
}

# ==============================
# 7. 학습 & 평가 함수
# ==============================
def train_and_evaluate(model, X_train, X_test, y_train, y_test, scale=False):
    if scale:
        X_train, X_test = scale_features(X_train, X_test)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    y_pred_exp, y_test_exp = np.expm1(y_pred), np.expm1(y_test)
    rmse = np.sqrt(mean_squared_error(y_test_exp, y_pred_exp))
    r2 = r2_score(y_test_exp, y_pred_exp)
    return rmse, r2, y_pred_exp

results = {}
for name, model in models.items():
    scale_flag = name in ["LinearRegression", "Ridge", "Lasso", "ElasticNet"]
    results[name] = train_and_evaluate(model, X_train, X_test, y_train, y_test, scale_flag)
    print(f"{name} - RMSE: {results[name][0]:.2f}, R²: {results[name][1]:.3f}")

# ==============================
# 8. 모델 성능 비교 시각화
# ==============================
def plot_model_comparison(results, metric="R2"):
    df = pd.DataFrame([(name, val[0], val[1]) for name, val in results.items()], columns=["Model", "RMSE", "R2"])
    plt.figure(figsize=(10,6))
    sns.barplot(data=df, x=metric, y="Model")
    plt.title(f"모델별 {metric} 비교")
    plt.show()

plot_model_comparison(results, metric="R2")
plot_model_comparison(results, metric="RMSE")

# ==============================
# 9. 실제값 vs 예측값 산점도
# ==============================
def plot_actual_vs_pred(results, y_test_exp, models_to_plot):
    fig, axes = plt.subplots(1, len(models_to_plot), figsize=(7*len(models_to_plot),6))
    if len(models_to_plot) == 1:
        axes = [axes]
    for ax, model_name in zip(axes, models_to_plot):
        y_pred_exp = results[model_name][2]
        ax.scatter(y_test_exp, y_pred_exp, alpha=0.7)
        ax.plot([y_test_exp.min(), y_test_exp.max()], [y_test_exp.min(), y_test_exp.max()], 'r--')
        ax.set_title(f"{model_name} 실제값 vs 예측값")
        ax.set_xlabel("실제 생산량")
        ax.set_ylabel("예측 생산량")
    plt.tight_layout()
    plt.show()

plot_actual_vs_pred(results, np.expm1(y_test), ["Ridge", "LinearRegression"])