import numpy as np
from sklearn import linear_model
from scipy.stats import norm

##Function Templates
def expfunc(xx, a, c):
    return a*np.exp(-c*xx)

##Regression Methods
def fit_linear(xx, yy, ridge=0, lasso=0, ci=99, graphdata=False):
    xx = xx.reshape(-1,1)
    if ridge == 0 & lasso == 0:
        model = linear_model.LinearRegression()
        model.fit(xx, yy)
        x = np.linspace(round(np.nanmin(xx)), round(np.nanmax(xx)), 500)
        x = x.reshape(-1,1)
        print type(x)
        y = model.predict(x)
        sd = np.sqrt(model.residues_ / (len(xx) - 2))
        ci = norm.ppf(0.99, scale=sd)
        yup = model.coef_*x + model.intercept_ + ci
        ylow = model.coef_*x + model.intercept_ - ci
        print "Resulting equation fit is y = e^(%s*x + %s)" %(model.coef_[0],model.intercept_)
    if graphdata==True:
        return model.coef_[0], model.intercept_, sd, model.residues_, ci, x, y, yup, ylow
    else:
        return model.coef_[0], model.intercept_, sd, model.residues_, ci