import analysis.regression as reg
import data_scraping.scrapingutils as su
import data_scraping.datautils as du
import numpy as np
import pylab as plt

#5e4 --> 1.5e8, 1.5e8 --> 1e9, 1e9 --> 1.1e10, 1.1e10 --> 4e10, 4e10 --> 1.2e11, 1.2e11 --> 3e11
#measure_filters = {"rank":[0, 500], "total_supply":[5e4,1.2e7], "price_usd":[0., 3e3]}
measure_filters = {"rank":[0, 300], "total_supply":[2e5,1.2e11], "price_usd":[0., 3e3]}
data = su.read_csv("https://docs.google.com/spreadsheet/ccc?key=1v_zFSIBsKvAepGQ8uIbIyb0Y4PgUdIqz3LccTa0-w0I&output=csv")
data = du.filter_df(data, m_filters=measure_filters, d_exclude=True)
coef, intercept, sd, residues, ci, x, y, yup, ylow = reg.fit_linear(np.log(data["total_supply"].values), np.log(data["price_usd"].values), graphdata=True)
data["stddevs"] = 0.0
data["nearest_high_price"] = 0.0

for symbol, row in data.iterrows():
    stddevs = (np.log(row[10]) - (np.log(row[12])*coef + intercept))/sd
    data.set_value(symbol, "stddevs", stddevs)

data = du.filter_df(data, m_filters={"price_usd":[0., 4e2]}, d_exclude=True)
#data = du.filter_df(data, m_filters={"price_usd":[0., 4e2], "total_supply":[2e5,1.5e9]}, d_exclude=True)
for symbol, row in data.iterrows():
    if row[12] > 1e9:
        weight = 0.33
    else:
        est_price = np.exp((np.log(row[12]) * coef + intercept))
        weight = 1.0 / (1.0 + np.exp(-10.0 * est_price))
    data.set_value(symbol, "nearest_high_price", data[(data["total_supply"] > (row[12] - weight*row[12])) & (data["total_supply"] < (row[12] + weight*row[12]))]['price_usd'].max())

x = reg.expfunc(x, 1, -1)
y = reg.expfunc(y, 1, -1)
plt.plot(data["total_supply"], data["price_usd"], 'ko', data["total_supply"], data["nearest_high_price"], 'rx', x, y)
plt.show()