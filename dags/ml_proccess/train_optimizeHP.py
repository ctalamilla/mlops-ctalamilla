


n_estimators = [100, 150]
max_features = np.arange(0,19)
max_depth = np.arange(5,15)


random_grid = {'n_estimators': n_estimators,
               'max_features': max_features,
               'max_depth': max_depth,
               }
print(random_grid)

rf = RandomForestRegressor()
rf_random = RandomizedSearchCV(estimator = rf, 
                            param_distributions = random_grid, 
                            n_iter = 10, 
                            cv = 10, 
                            verbose=2, 
                            random_state=42,
                            scoring    = 'neg_mean_absolute_error',
                            return_train_score = True,
                            n_jobs = -1)
rf_random.fit(X_train, y_train)
