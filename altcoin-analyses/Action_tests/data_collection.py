import data_scraping.gitservice.db_interface as dbi
import custom_utils.datautils as du
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import expon

def pre_process_stats(commits=False, committers=False, stars=False, forks=False, issues=False, pullrequests=False, format_="stats", timescale="month"):
    results = {}
    data_list = ['commits', 'committers', 'stars', 'forks', 'issues', 'pullrequests']
    data_list = du.select_elements_by_boolean(data_list, (commits, committers, stars, forks, issues, pullrequests))
    target_stats = ["6mo", "total_6mo", "av_6mo", "total_3mo", "av_3mo", "past_month"]
    reference_time_6mo = du.get_time(tz_aware=False, months=6).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    reference_time_3mo = du.get_time(tz_aware=False, months=3).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    reference_time_1mo = du.get_time(tz_aware=False, months=1).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    reference_time_current_mo = du.get_time(tz_aware=False).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    reference_dates = [(reference_time_6mo, 0)]
    for i in range(1, 7):
        print i
        reference_dates.append((du.get_time(input_time=reference_time_6mo, add=True, months=i, tz_aware=False).replace(day=1, hour=0, minute=0, second=0, microsecond=0), 0))

    reference_dates = np.asarray(reference_dates)
    stat_list = []
    for datapoint in data_list:
        stat_list += [datapoint + "_" + x for x in target_stats]
    a = dbi.get_time_series_data(timescale=timescale, commits=commits, committers=committers,stars=stars, forks=forks, issues=issues, pullrequests=pullrequests)
    if format_ == 'stats':
        results = {v[0]: {'org_stats': {statistic: 0 for statistic in stat_list}} for v in a['unique_repos']}
        for repo_entry in a['unique_repos']:
            results[repo_entry[0]][repo_entry[1]] = {stat: [] for stat in data_list}
        for stat in data_list:
            for entry in a[stat]:
                results[entry[2]][entry[3]][stat].append((entry[0], int(entry[1])))
                results[entry[2]]["org_stats"][stat + "_6mo"] = reference_dates[:]
                results[entry[2]]["org_stats"][stat + "_3mo"] = reference_dates[3:]
        for coin in results.keys():
            for repo in results[coin].keys():
                if repo == "org_stats":
                    continue
                for stat in data_list:
                    results[coin][repo][stat] = np.asarray(results[coin][repo][stat])
                    if len(results[coin][repo][stat]) > 0:
                        for entry in results[coin][repo][stat]:
                            for month in results[coin]["org_stats"][stat + "_6mo"]:
                                if entry[0] == month[0]:
                                    month[1] += entry[1]
                        results[coin][repo][stat + "_total_6mo"] = np.sum(results[coin][repo][stat][np.where(results[coin][repo][stat][:, 0] > reference_time_6mo)][:, 1])
                        results[coin][repo][stat + "_av_6mo"] = results[coin][repo][stat + "_total_6mo"] / 6
                        results[coin][repo][stat + "_total_3mo"] = np.sum(results[coin][repo][stat][np.where(results[coin][repo][stat][:, 0] > reference_time_3mo)][:, 1])
                        results[coin][repo][stat + "_av_3mo"] = results[coin][repo][stat + "_total_3mo"] / 3
                        results[coin][repo][stat + "_past_month"] = np.sum(results[coin][repo][stat][np.where(results[coin][repo][stat][:, 0] > reference_time_1mo) and np.where(results[coin][repo][stat][:, 0] < reference_time_current_mo)][:,1])
                        print(stat)
                        print(results[coin]['org_stats'][stat + "_total_6mo"])
                        results[coin]['org_stats'][stat + "_total_6mo"] += results[coin][repo][stat + "_total_6mo"]
                        results[coin]['org_stats'][stat + "_total_3mo"] += results[coin][repo][stat + "_total_3mo"]
                        results[coin]['org_stats'][stat + "_past_month"] += results[coin][repo][stat + "_past_month"]
                    else:
                        results[coin][repo][stat + "_total_6mo"] = 0
                        results[coin][repo][stat + "_total_3mo"] = 0
                        results[coin][repo][stat + "_av_6mo"] = 0
                        results[coin][repo][stat + "_av_3mo"] = 0
                        results[coin][repo][stat + "_past_month"] = 0
            for stat in data_list:
                results[coin]['org_stats'][stat + "_av_6mo"] = results[coin]['org_stats'][stat + "_total_6mo"]/6
                results[coin]['org_stats'][stat + "_av_3mo"] = results[coin]['org_stats'][stat + "_total_3mo"]/3
    return results

results = pre_process_stats(True, True, True)

##Exploratory Methods
def create_histogram(data, bins):
    plt.hist(data, bins=bins)
    plt.show()



stat_list = ['commits','committers', 'stars']
fig, ax = plt.subplots()
stat = 'commits'
for coin in results.keys():
    for repo in results[coin].keys():
            if repo != "org_stats" and len(results[coin][repo][stat]) > 0:
                print "loop_entered"
                ax.plot(results[coin][repo][stat][np.where(results[coin][repo][stat][:, 0] > du.get_time(months=6, tz_aware=False))][:, 1])

x = []
y = []
for coin in results.keys():
    x.append(results[coin]['org_stats']['commits_av_6mo'])

y = [n for n in x if n > 0]

shape, scale = expon.fit(y)
q = np.arange(0,1000,0.50)
qq = expon.pdf(q, loc=0, scale=scale)
expon.cdf(y, loc =0, scale =scale)
plt.plot(q, qq, "--r")
deltas = y/expon.std(scale=scale) - 1

plt.show()

plt.plot(y, deltas, 'bo')
plt.show()

commits, committers, stars, forks, issues, pullrequests = True, True, True, False, False, False
format_= "stats"
timescale="month"
a = dbi.get_time_series_data(timescale=timescale, commits=commits, committers=committers, stars=stars, forks=forks, issues=issues, pullrequests=pullrequests)

results = {}
data_list = ['commits', 'committers', 'stars', 'forks', 'issues', 'pullrequests']
data_list = du.select_elements_by_boolean(data_list, (commits, committers, stars, forks, issues, pullrequests))
target_stats = ["6mo", "total_6mo", "av_6mo", "total_3mo", "av_3mo", "past_month"]
reference_time_6mo = du.get_time(tz_aware=False, months=6).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
reference_time_3mo = du.get_time(tz_aware=False, months=3).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
reference_time_1mo = du.get_time(tz_aware=False, months=1).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
reference_time_current_mo = du.get_time(tz_aware=False).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
reference_dates = [(reference_time_6mo, 0)]
for i in range(1, 7):
    reference_dates.append((du.get_time(input_time=reference_time_6mo, add=True, months=i, tz_aware=False).replace(day=1, hour=0, minute=0, second=0, microsecond=0), 0))

reference_dates = np.asarray(reference_dates)
stat_list = []
for datapoint in data_list:
    stat_list += [datapoint + "_" + x for x in target_stats]

if format_ == 'stats':
    results = {v[0]: {'org_stats': {statistic: 0 for statistic in stat_list}} for v in a['unique_repos']}
    for repo_entry in a['unique_repos']:
        results[repo_entry[0]][repo_entry[1]] = {stat: [] for stat in data_list}
    for stat in data_list:
        for entry in a[stat]:
            results[entry[2]][entry[3]][stat].append((entry[0], int(entry[1])))
            results[entry[2]]["org_stats"][stat + "_6mo"] = reference_dates[:]
            results[entry[2]]["org_stats"][stat + "_3mo"] = reference_dates[3:]
    for coin in results.keys():
        for repo in results[coin].keys():
            if repo == "org_stats":
                continue
            for stat in data_list:
                results[coin][repo][stat] = np.asarray(results[coin][repo][stat])
                if len(results[coin][repo][stat]) > 0:
                    for entry in results[coin][repo][stat]:
                        for month in results[coin]["org_stats"][stat + "_6mo"]:
                            if entry[0] == month[0]:
                                month[1] += entry[1]
                    results[coin][repo][stat + "_total_6mo"] = np.sum(
                        results[coin][repo][stat][np.where(results[coin][repo][stat][:, 0] > reference_time_6mo)][:, 1])
                    results[coin][repo][stat + "_av_6mo"] = results[coin][repo][stat + "_total_6mo"] / 6
                    results[coin][repo][stat + "_total_3mo"] = np.sum(
                        results[coin][repo][stat][np.where(results[coin][repo][stat][:, 0] > reference_time_3mo)][:, 1])
                    results[coin][repo][stat + "_av_3mo"] = results[coin][repo][stat + "_total_3mo"] / 3
                    results[coin][repo][stat + "_past_month"] = np.sum(results[coin][repo][stat][np.where(
                        results[coin][repo][stat][:, 0] > reference_time_1mo) and np.where(
                        results[coin][repo][stat][:, 0] < reference_time_current_mo)][:, 1])
                    print(stat)
                    print(results[coin]['org_stats'][stat + "_total_6mo"])
                    results[coin]['org_stats'][stat + "_total_6mo"] += results[coin][repo][stat + "_total_6mo"]
                    results[coin]['org_stats'][stat + "_total_3mo"] += results[coin][repo][stat + "_total_3mo"]
                    results[coin]['org_stats'][stat + "_past_month"] += results[coin][repo][stat + "_past_month"]
                else:
                    results[coin][repo][stat + "_total_6mo"] = 0
                    results[coin][repo][stat + "_total_3mo"] = 0
                    results[coin][repo][stat + "_av_6mo"] = 0
                    results[coin][repo][stat + "_av_3mo"] = 0
                    results[coin][repo][stat + "_past_month"] = 0
        for stat in data_list:
            results[coin]['org_stats'][stat + "_av_6mo"] = results[coin]['org_stats'][stat + "_total_6mo"] / 6
            results[coin]['org_stats'][stat + "_av_3mo"] = results[coin]['org_stats'][stat + "_total_3mo"] / 3
